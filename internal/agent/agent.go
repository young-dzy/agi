// Package agent 实现 UnifiedAgent：整合全部 6 个阶段能力的核心调度器。
//
// 路由策略（按优先级）：
//  1. ReAct + Harness — 复合查询（含 2+ 子需求，需多步推理）
//  2. Tool Agent      — 单一工具触发（时间 / 天气 / 搜索）
//  3. RAG             — 知识库已加载且无工具触发
//  4. Chat            — 直接与 LLM 对话
//
// 记忆系统作为基础层注入所有模式（偏好 + 长期记忆 → System Prompt，STM → 对话历史）
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"agi-ai-assitant/config"
	"agi-ai-assitant/internal/graph"
	"agi-ai-assitant/internal/infra"
	"agi-ai-assitant/internal/llm"
	"agi-ai-assitant/internal/memory"
	"agi-ai-assitant/internal/rag"
	"agi-ai-assitant/internal/runtime"
	"agi-ai-assitant/internal/sandbox"
	"agi-ai-assitant/internal/tools"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// ─────────────────────────────── ReAct 数据结构 ──────────────────────────

// StepType 是 ReAct 循环中的步骤类型
type StepType string

const (
	StepThought     StepType = "Thought"
	StepAction      StepType = "Action"
	StepObservation StepType = "Observation"
	StepFinalAnswer StepType = "Final Answer"
)

// ReActStep 记录 ReAct 循环的单个步骤
type ReActStep struct {
	Type    StepType          `json:"type"`
	Content string            `json:"content"`
	Tool    string            `json:"tool,omitempty"`
	Params  map[string]string `json:"params,omitempty"`
}

// ─────────────────────────────── Harness 数据结构 ────────────────────────

// TaskStepStatus 是任务步骤的执行状态
type TaskStepStatus string

const (
	StepPending      TaskStepStatus = "pending"
	StepRunning      TaskStepStatus = "running"
	StepDone         TaskStepStatus = "done"
	StepFailed       TaskStepStatus = "failed"
	StepInterrupted  TaskStepStatus = "interrupted"
)

// TaskStep 是 Harness 中可重试的原子执行单元
type TaskStep struct {
	ID         int            `json:"id"`
	Name       string         `json:"name"`
	ToolName   string         `json:"tool_name"`
	Params     map[string]string `json:"params"`
	Status     TaskStepStatus `json:"status"`
	Result     string         `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
	RetryCount int            `json:"retry_count"`
}

// TaskState 描述一次任务的完整执行状态
type TaskState struct {
	TaskID        string     `json:"task_id"`
	Query         string     `json:"query"`
	Status        string     `json:"status"`        // "running" | "completed" | "interrupted"
	Phase         string     `json:"phase"`          // "planning" | "executing" | "generating" | "done" | "interrupted"
	Steps         []TaskStep `json:"steps"`
	CurrentStep   int        `json:"current_step"`
	InterruptedAt int        `json:"interrupted_at,omitempty"` // 在第几步被中断的（0-based）
	Result        string     `json:"result,omitempty"`
}

// Snapshot 是某一时刻的任务状态快照（用于故障恢复）
type Snapshot struct {
	State     TaskState `json:"state"`
	Timestamp string    `json:"timestamp"`
}

// ─────────────────────────────── 统一响应 ────────────────────────────────

// Response 是 UnifiedAgent.Process 的输出，携带本次请求的全部上下文
type Response struct {
	Query          string                 `json:"query"`
	Answer         string                 `json:"answer"`
	Mode           string                 `json:"mode"`           // chat / tool / rag / memory / react
	Steps          []ReActStep            `json:"steps,omitempty"`
	ToolCall       *tools.CallResult      `json:"tool_call,omitempty"`
	SearchResults  []rag.SearchResult     `json:"search_results,omitempty"`
	Task           *TaskState             `json:"task,omitempty"`
	ExtractedInfo  string                 `json:"extracted_info,omitempty"`
	ShortTermCount int                    `json:"short_term_count"`
	LongTermCount  int                    `json:"long_term_count"`
	Preferences    map[string]string      `json:"preferences"`
	Interrupted    bool                   `json:"interrupted,omitempty"`
}

// ─────────────────────────────── Unified Agent ───────────────────────────

// UnifiedAgent 整合全部能力，是系统的核心调度入口
type UnifiedAgent struct {
	cfg      *config.APIConfig
	llm      *llm.Client
	rag      *rag.Engine
	tools    map[string]tools.Tool
	stm      *memory.ShortTerm
	ltm      *memory.LongTerm      // 保留直接引用，供 handler 暴露
	graphMem *memory.GraphMemory   // 图增强记忆层（包装 ltm）
	pref     *memory.Preference
	sandbox  *sandbox.Sandbox
	kg       *graph.KGStore // 知识图谱（RAG + 记忆图共享）
	snapshots []Snapshot
	task     *TaskState
	inf      *infra.Infrastructure
	cancelFn context.CancelFunc // 当前任务的取消函数

	// Schema-driven Runtime Context Assembly
	assembler   *runtime.ContextAssembler
	taskMem     *runtime.TaskMemBuffer
	toolTracker *runtime.ToolStateTracker
}

// New 创建并初始化 UnifiedAgent
func New(cfg *config.APIConfig, inf *infra.Infrastructure) *UnifiedAgent {
	llmClient := llm.New(cfg)
	ragEngine := rag.NewEngine(cfg, inf)
	ltm := memory.NewLongTerm()
	a := &UnifiedAgent{
		cfg:   cfg,
		llm:   llmClient,
		rag:   ragEngine,
		tools: tools.DefaultTools(),
		stm:   memory.NewShortTerm(cfg.ShortTermMaxTurns),
		ltm:   ltm,
		// graphMem 在 initKnowledgeGraph 中创建（需要 kg 先就绪）
		pref: memory.NewPreference(),
		inf:  inf,
	}
	// 配置长期记忆合并
	a.ltm.SetConsolidationConfig(&memory.ConsolidationConfig{
		SimilarityThreshold: cfg.MemoryConsolidationSimilarity,
		DedupThreshold:      cfg.MemoryConsolidationDedup,
		TTLDays:             cfg.MemoryConsolidationTTLDays,
		DecayRate:           cfg.MemoryConsolidationDecayRate,
		MinImportance:       cfg.MemoryConsolidationMinImport,
		TriggerInterval:     cfg.MemoryConsolidationTrigger,
	})
	// 注入 RAG 的 LLM 合成回调（携带记忆上下文）
	a.rag.SetGenerateFn(func(systemPrompt, userMsg string) string {
		// RAG 模式下用 schema-driven 装配（assembler 在 New 末尾才构造，此回调
		// 在运行期才会被触发，因此 a.assembler 一定已就绪）
		memPrefix := a.buildContextPrefix(context.Background(), userMsg, "rag")
		fullSystem := systemPrompt
		if memPrefix != "" {
			fullSystem = memPrefix + "\n\n" + systemPrompt + "\n结合用户偏好和记忆，用用户熟悉的方式回答。"
		}
		return a.llm.Chat(fullSystem, []llm.Message{{Role: "user", Content: userMsg}})
	})
	// 注入 RAG 的 Embedding 回调
	a.rag.SetEmbedFn(func(text string) ([]float64, error) {
		return a.llm.Embed(text)
	})
	// 初始化 RAG 基础设施（Milvus collection + ES 索引）
	a.inf.InitRAGInfra(cfg.RAGMilvusDim)
	// 将 RAG 注册为可选工具（私人黑洞知识库检索）
	a.tools["rag_search"] = tools.Tool{
		Name:        "rag_search",
		Description: "从私人黑洞（个人知识库）中检索相关文档内容",
		Parameters: []tools.Param{
			{Name: "query", Type: "string", Description: "检索关键词或问题", Required: true},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			q, _ := params["query"].(string)
			if q == "" {
				q = "相关内容"
			}
			if !a.rag.Loaded {
				return "", fmt.Errorf("知识库为空，请先在「私人黑洞」上传文档")
			}
			answer, _ := a.rag.Query(q)
			return answer, nil
		},
	}
	// 用 LLM 知识 + 可选 Tavily API 替换默认的 mock search_web
	a.tools["search_web"] = tools.Tool{
		Name:        "search_web",
		Description: "搜索互联网获取最新信息",
		Parameters: []tools.Param{
			{Name: "query", Type: "string", Description: "搜索关键词", Required: true},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			q, _ := params["query"].(string)
			if q == "" {
				return "", fmt.Errorf("搜索关键词不能为空")
			}
			// 优先尝试 Tavily 真实搜索
			if a.cfg.SearchAPIKey != "" {
				if result, err := tavilySearch(q, a.cfg.SearchAPIKey, a.cfg.SearchAPIURL); err == nil {
					return result, nil
				}
			}
			// 降级：用 LLM 知识库回答
			return a.llm.Chat(
				"你是一个知识丰富的搜索引擎助手。请基于你的知识，对用户的搜索问题给出准确、详细的回答。直接给出答案，不要说「我不知道」或「我无法搜索」。",
				[]llm.Message{{Role: "user", Content: "搜索：" + q}},
			), nil
		},
	}
	// 从 PostgreSQL 恢复跨会话记忆
	a.restoreFromDB()
	// 从 PostgreSQL 恢复 RAG chunks
	a.restoreRAGFromDB()
	// 初始化知识图谱（Neo4j），注入到 RAG 引擎
	a.initKnowledgeGraph()
	// 初始化沙箱并注册 exec_command 工具
	a.initSandbox()

	// ── Schema-driven Runtime Context Assembly ──
	a.taskMem = runtime.NewTaskMemBuffer(20)
	a.toolTracker = runtime.NewToolStateTracker(10)

	reg := runtime.NewSourceRegistry()
	reg.Register(runtime.NewProfileSource(a.pref, a.ltm))
	reg.Register(runtime.NewPlannerSource(func() *runtime.PlannerSnapshot {
		t := a.task
		if t == nil {
			return nil
		}
		snap := &runtime.PlannerSnapshot{
			TaskID:        t.TaskID,
			Query:         t.Query,
			Status:        t.Status,
			Phase:         t.Phase,
			TotalSteps:    len(t.Steps),
			CurrentStep:   t.CurrentStep,
			InterruptedAt: t.InterruptedAt,
		}
		if t.CurrentStep+1 < len(t.Steps) {
			next := t.Steps[t.CurrentStep+1]
			snap.NextStepName = next.Name
			snap.NextStepTool = next.ToolName
		}
		return snap
	}))
	reg.Register(runtime.NewTaskMemSource(a.taskMem))
	reg.Register(runtime.NewToolStateSource(
		func() map[string]tools.Tool { return a.tools },
		a.toolTracker,
	))
	reg.Register(runtime.NewConstraintsSource(sandbox.PolicySnapshot()))
	// RecallSource 优先用图记忆；graphMem 在 initKnowledgeGraph 中就绪
	if a.graphMem != nil {
		reg.Register(runtime.NewRecallSource(a.graphMem))
	} else {
		reg.Register(runtime.NewRecallSource(a.ltm))
	}
	a.assembler = runtime.NewAssembler(runtime.DefaultSchemas(), reg)

	return a
}

// RegisterTool 动态注册一个工具（支持 MCP 工具热插入）
func (a *UnifiedAgent) RegisterTool(t tools.Tool) {
	a.tools[t.Name] = t
}

// RAG 暴露 RAG 引擎，供 HTTP handler 直接调用 Ingest
func (a *UnifiedAgent) RAG() *rag.Engine { return a.rag }

// Tools 暴露工具集，供 HTTP handler 列出工具信息
func (a *UnifiedAgent) Tools() map[string]tools.Tool { return a.tools }

// ShortTerm 暴露短期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) ShortTerm() *memory.ShortTerm { return a.stm }

// LongTerm 暴露长期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) LongTerm() *memory.LongTerm { return a.ltm }

// Preferences 暴露用户偏好，供 HTTP handler 查询
func (a *UnifiedAgent) Preferences() *memory.Preference { return a.pref }

// Snapshots 返回历史快照列表
func (a *UnifiedAgent) Snapshots() []Snapshot { return a.snapshots }

// ─────────────────────────────── 主处理流程 ──────────────────────────────

// ChatOptions 控制本次对话的路由行为
type ChatOptions struct {
	UseRAG        bool     // 是否使用 RAG 知识库
	SelectedTools []string // 用户明确选中的工具列表；nil = 自动路由，[] = 禁用工具
	Explicit      bool     // true 时以 SelectedTools/UseRAG 为准，false 时自动路由
}

// Process 是统一入口（自动路由，向后兼容）
func (a *UnifiedAgent) Process(query string) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancelFn = cancel
	defer cancel()
	return a.process(ctx, query, ChatOptions{Explicit: false})
}

// ProcessWithOptions 带显式选项的入口，供前端精确控制路由
func (a *UnifiedAgent) ProcessWithOptions(query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancelFn = cancel
	defer cancel()
	return a.process(ctx, query, opts)
}

// ProcessContext 带 context 的入口，支持 SSE 流式和取消
func (a *UnifiedAgent) ProcessContext(ctx context.Context, query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(ctx)
	a.cancelFn = cancel
	defer cancel()
	return a.process(ctx, query, opts)
}

// Cancel 取消当前正在执行的任务
func (a *UnifiedAgent) Cancel() {
	if a.cancelFn != nil {
		a.cancelFn()
	}
}

func (a *UnifiedAgent) process(ctx context.Context, query string, opts ChatOptions) *Response {
	resp := &Response{Query: query, Mode: "chat"}

	// 更新短期记忆
	a.stm.Add("user", query)

	// 持久化用户消息到 PG
	a.inf.SaveChatHistory("user", query)

	// 偏好提取：优先 LLM，降级规则
	go func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) > 0 {
			a.pref.SaveBatch(kvs)
			for k, v := range kvs {
				a.inf.SavePreference("default", k, v)
				content := fmt.Sprintf("用户%s: %s", k, v)
				emb, _ := a.llm.Embed(content)
				if added, _ := a.graphMem.Store(content, 0.8, emb); added {
					embJSON, _ := json.Marshal(emb)
					pgID := a.inf.SaveLongTermItem(content, 0.8, embJSON)
					a.graphMem.SyncLastItemPGID(pgID)
				}
			}
		}
	}()

	// 同步规则提取（用于立即展示 ExtractedInfo）
	if key, value, ok := a.pref.ExtractAndSave(query); ok {
		resp.ExtractedInfo = fmt.Sprintf("已记住：%s = %s", key, value)
	}

	// ── 路由决策（mode 在装配前确定，让 schema 选取正确）──
	var mode string
	var routeTools map[string]tools.Tool
	if opts.Explicit {
		switch {
		case len(opts.SelectedTools) > 0:
			routeTools = a.filterTools(opts.SelectedTools)
			if a.needReActFromTools(query, routeTools) {
				mode = "react"
			} else {
				mode = "tool"
			}
		case opts.UseRAG && a.rag.Loaded:
			mode = "rag"
		default:
			mode = "chat"
		}
	} else {
		switch {
		case a.needReAct(query):
			mode = "react"
			routeTools = a.tools
		case a.needTool(query):
			mode = "tool"
			routeTools = a.tools
		case a.needRAG(query):
			mode = "rag"
		default:
			mode = "chat"
		}
	}
	resp.Mode = mode

	// ── 装配 Schema-driven 上下文前缀 ──
	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(query)

	// 检查 context 是否已取消
	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		return resp
	}

	// ── 分发执行（mode 已确定）──
	switch mode {
	case "react":
		answer, steps, task := a.runReActWithTools(ctx, query, routeTools, memPrefix, histMsgs)
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runToolFromSet(ctx, query, routeTools, memPrefix, histMsgs)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		answer, results := a.rag.Query(query)
		resp.Answer, resp.SearchResults = answer, results
	default:
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.llm.ChatContext(ctx, systemPrompt, histMsgs)
	}

	// 检查是否被中断
	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.stm.Add("assistant", resp.Answer)
	a.inf.SaveChatHistory("assistant", resp.Answer)

	// 从 assistant 回答中提取可记忆信息
	go a.extractMemoryFromReply(resp.Answer)

	// 异步触发记忆合并（去重+合并+衰减+过期；有图层时使用图感知合并以保护高中心度节点）
	go func() {
		if a.ltm.NeedConsolidation() {
			var result memory.ConsolidationResult
			if a.graphMem != nil {
				result = a.graphMem.GraphAwareConsolidate()
			} else {
				result = a.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	}()

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.inf.PublishEvent("agent.chat", string(eventData))

	resp.ShortTermCount = len(a.stm.Messages)
	resp.LongTermCount = len(a.ltm.Items)
	resp.Preferences = a.pref.Data
	return resp
}

// buildContextPrefix 调用 Schema-driven ContextAssembler，返回当次推理的系统提示前缀
func (a *UnifiedAgent) buildContextPrefix(ctx context.Context, query string, mode string) string {
	if a.assembler == nil {
		return ""
	}
	emb, _ := a.llm.EmbedContext(ctx, query)
	taskID := ""
	if a.task != nil {
		taskID = a.task.TaskID
	}
	rc := a.assembler.Assemble(ctx, runtime.Query{
		Text:      query,
		Embedding: emb,
		TaskID:    taskID,
		Mode:      mode,
	})
	return rc.Render()
}

// buildSystemPrompt 构建带记忆前缀的 system prompt
func (a *UnifiedAgent) buildSystemPrompt(memPrefix, basePrompt string) string {
	if memPrefix == "" {
		return basePrompt
	}
	return memPrefix + "\n\n" + basePrompt
}

// buildHistoryMessages 将 STM 历史消息转为 LLM 消息列表（末尾附上当前 user query）
func (a *UnifiedAgent) buildHistoryMessages(query string) []llm.Message {
	var msgs []llm.Message
	// STM 最后一条是刚加入的 user query，跳过重复
	for _, m := range a.stm.Messages {
		if m.Role == "user" || m.Role == "assistant" {
			msgs = append(msgs, llm.Message{Role: m.Role, Content: m.Content})
		}
	}
	// 如果最后一条不是当前 query（初次调用时 STM 已包含），则附上
	if len(msgs) == 0 || msgs[len(msgs)-1].Content != query {
		msgs = append(msgs, llm.Message{Role: "user", Content: query})
	}
	return msgs
}

// filterTools 按名称列表过滤可用工具集
func (a *UnifiedAgent) filterTools(names []string) map[string]tools.Tool {
	result := make(map[string]tools.Tool)
	for _, name := range names {
		if t, ok := a.tools[name]; ok {
			result[name] = t
		}
	}
	return result
}

// needReActFromTools — 只要工具集非空就走 ReAct，保证每次工具调用都有完整推理轨迹
func (a *UnifiedAgent) needReActFromTools(query string, ts map[string]tools.Tool) bool {
	return len(ts) > 0
}

// ─────────────────────────────── 路由判断 ────────────────────────────────

func (a *UnifiedAgent) needTool(query string) bool {
	q := strings.ToLower(query)
	return strings.Contains(q, "几点") || strings.Contains(q, "时间") ||
		strings.Contains(q, "天气") || strings.Contains(q, "查") ||
		strings.Contains(q, "搜索") || strings.Contains(q, "是什么")
}

func (a *UnifiedAgent) needRAG(query string) bool {
	return a.rag.Loaded && !a.needTool(query) && !a.needReAct(query)
}

// needReAct 当 query 涉及 2+ 个子需求时触发多步推理
func (a *UnifiedAgent) needReAct(query string) bool {
	q := strings.ToLower(query)
	count := 0
	if strings.Contains(q, "时间") || strings.Contains(q, "几点") {
		count++
	}
	if strings.Contains(q, "天气") {
		count++
	}
	if strings.Contains(q, "总结") || strings.Contains(q, "汇总") {
		count++
	}
	if strings.Contains(q, "查") || strings.Contains(q, "搜索") {
		count++
	}
	return count >= 2
}

// tavilySearch 调用 Tavily Search API，返回格式化的搜索结果摘要
func tavilySearch(query, apiKey, apiURL string) (string, error) {
	if apiURL == "" {
		apiURL = "https://api.tavily.com/search"
	}
	body, _ := json.Marshal(map[string]interface{}{
		"api_key":      apiKey,
		"query":        query,
		"search_depth": "basic",
		"max_results":  5,
	})
	resp, err := http.Post(apiURL, "application/json", bytes.NewReader(body)) //nolint
	if err != nil {
		return "", fmt.Errorf("Tavily 请求失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("Tavily 返回错误状态: %d", resp.StatusCode)
	}
	var result struct {
		Answer  string `json:"answer"`
		Results []struct {
			Title   string `json:"title"`
			URL     string `json:"url"`
			Content string `json:"content"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("解析 Tavily 响应失败: %w", err)
	}
	// 优先返回 Tavily 合成的 answer
	if result.Answer != "" {
		var sb strings.Builder
		sb.WriteString(result.Answer)
		if len(result.Results) > 0 {
			sb.WriteString("\n\n**来源：**\n")
			for i, r := range result.Results {
				if i >= 3 {
					break
				}
				sb.WriteString(fmt.Sprintf("- [%s](%s)\n", r.Title, r.URL))
			}
		}
		return sb.String(), nil
	}
	// 无 answer 时拼接 top 结果摘要
	if len(result.Results) == 0 {
		return "", fmt.Errorf("Tavily 返回空结果")
	}
	var sb strings.Builder
	for i, r := range result.Results {
		if i >= 3 {
			break
		}
		sb.WriteString(fmt.Sprintf("**%s**\n%s\n%s\n\n", r.Title, r.Content, r.URL))
	}
	return strings.TrimSpace(sb.String()), nil
}

// ─────────────────────────────── Stage 3：Tool Agent ─────────────────────

func (a *UnifiedAgent) runToolFromSet(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string, histMsgs []llm.Message) (string, *tools.CallResult) {
	tc := tools.Decide(query, ts)
	if tc == nil {
		return "我无法处理这个请求。", nil
	}
	tool, ok := ts[tc.ToolName]
	if !ok {
		return fmt.Sprintf("工具 %s 不存在", tc.ToolName), tc
	}

	// 偏好感知参数自动填充
	a.fillParamsFromPreference(tc)

	result, err := tool.Execute(tc.Params)
	if err != nil {
		if ctx.Err() != nil {
			return "[已中断]", tc
		}
		if a.toolTracker != nil {
			a.toolTracker.Record(runtime.ToolCallTrace{
				ToolName: tc.ToolName, Success: false, Summary: err.Error(),
			})
		}
		return fmt.Sprintf("工具执行失败: %v", err), tc
	}
	tc.ToolResult = result
	if a.toolTracker != nil {
		a.toolTracker.Record(runtime.ToolCallTrace{
			ToolName: tc.ToolName, Success: true, Summary: result,
		})
	}

	// 用带记忆的 system prompt 生成自然语言回复
	systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个善于综合信息的AI助手。结合你掌握的用户信息，使回答更个性化。")
	userMsg := fmt.Sprintf("用户问：%s\n工具 %s 返回结果：%s\n请根据结果自然地回答用户。", query, tc.ToolName, result)
	answer := a.llm.ChatContext(ctx, systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
	return answer, tc
}

// ─────────────────────────────── Stage 4：ReAct ──────────────────────────

func (a *UnifiedAgent) runReActWithTools(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string, histMsgs []llm.Message) (string, []ReActStep, *TaskState) {
	var reactSteps []ReActStep
	var observations []string

	// ── Step 1: Planner LLM 决定调哪些工具及参数 ──────────────────────────
	planItems := a.llmPlanSteps(ctx, query, ts, memPrefix)

	// 若 Planner 决定不需要任何工具，直接走 LLM 对话
	if len(planItems) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		answer := a.llm.ChatContext(ctx, systemPrompt, histMsgs)
		reactSteps = append(reactSteps, ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"})
		reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
		if ctx.Err() != nil {
			return "[已中断] 规划完成但生成被中断", reactSteps, nil
		}
		return answer, reactSteps, nil
	}

	// 将 planItems 转换为 TaskStep 列表
	var taskSteps []TaskStep
	for i, pi := range planItems {
		taskSteps = append(taskSteps, TaskStep{
			ID: i + 1, Name: pi.Reason, ToolName: pi.Tool,
			Params: pi.Params, Status: StepPending,
		})
	}

	a.task = &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing", Steps: taskSteps,
	}
	a.snapshots = nil
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot()

	// ── Step 2: 按 Planner 计划逐步执行工具 ───────────────────────────────
	for i := range a.task.Steps {
		// 每步开始前检查 context 是否已取消
		if ctx.Err() != nil {
			a.task.Phase = "interrupted"
			a.task.Status = "interrupted"
			a.task.InterruptedAt = i
			// 将当前步骤标记为中断
			a.task.Steps[i].Status = StepInterrupted
			// 生成中断摘要
			interruptMsg := a.buildInterruptMessage(a.task)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg})
			a.saveSnapshot()
			return "[已中断] " + interruptMsg, reactSteps, a.task
		}

		ts2 := &a.task.Steps[i]
		a.task.CurrentStep = i
		ts2.Status = StepRunning

		// Thought：展示 Planner 给出的调用理由
		reactSteps = append(reactSteps, ReActStep{
			Type:    StepThought,
			Content: ts2.Name, // Name 即 Planner 生成的 reason
		})
		reactSteps = append(reactSteps, ReActStep{
			Type:    StepAction,
			Content: fmt.Sprintf("调用 %s", ts2.ToolName),
			Tool:    ts2.ToolName,
			Params:  ts2.Params,
		})

		tool, ok := ts[ts2.ToolName]
		if !ok {
			ts2.Status = StepFailed
			ts2.Error = fmt.Sprintf("工具 %s 不在允许列表中", ts2.ToolName)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: ts2.Error})
			a.saveSnapshot()
			continue
		}
		if a.executeStepWithRetryTool(ctx, ts2, tool) {
			ts2.Status = StepDone
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: ts2.Result})
			observations = append(observations, fmt.Sprintf("[%s] %s", ts2.ToolName, ts2.Result))
			if a.taskMem != nil {
				a.taskMem.Push(runtime.StepObservation{
					StepID: ts2.ID, ToolName: ts2.ToolName,
					Result: ts2.Result, Success: true,
				})
			}
			if a.toolTracker != nil {
				a.toolTracker.Record(runtime.ToolCallTrace{
					ToolName: ts2.ToolName, Success: true, Summary: ts2.Result,
				})
			}
		} else {
			if ctx.Err() != nil {
				ts2.Status = StepInterrupted
				reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断]"})
			} else {
				ts2.Status = StepFailed
				reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", ts2.Error)})
				if a.taskMem != nil {
					a.taskMem.Push(runtime.StepObservation{
						StepID: ts2.ID, ToolName: ts2.ToolName,
						Error: ts2.Error, Success: false,
					})
				}
				if a.toolTracker != nil {
					a.toolTracker.Record(runtime.ToolCallTrace{
						ToolName: ts2.ToolName, Success: false, Summary: ts2.Error,
					})
				}
			}
		}
		a.saveSnapshot()
	}

	// ── Step 3: Generator LLM 综合所有观察结果生成最终答案 ────────────────
	if ctx.Err() != nil {
		a.task.Phase = "interrupted"
		a.task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessage(a.task)
		return "[已中断] " + interruptMsg, reactSteps, a.task
	}

	a.task.Phase = "generating"
	answer := a.llmGenerate(ctx, query, observations, memPrefix, histMsgs)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	a.task.Result = answer
	a.task.Status = "completed"
	a.task.Phase = "done"
	return answer, reactSteps, a.task
}

// buildInterruptMessage 根据已完成的步骤生成中断摘要
func (a *UnifiedAgent) buildInterruptMessage(task *TaskState) string {
	doneSteps := 0
	var doneDesc []string
	var pendingDesc []string
	for _, s := range task.Steps {
		switch s.Status {
		case StepDone:
			doneSteps++
			doneDesc = append(doneDesc, fmt.Sprintf("%d.%s→%s", s.ID, s.ToolName, truncateStr(s.Result, 30)))
		case StepPending, StepRunning, StepInterrupted:
			pendingDesc = append(pendingDesc, fmt.Sprintf("%d.%s", s.ID, s.ToolName))
		}
	}
	msg := fmt.Sprintf("已完成 %d/%d 步", doneSteps, len(task.Steps))
	if len(doneDesc) > 0 {
		msg += "：" + strings.Join(doneDesc, "；")
	}
	if len(pendingDesc) > 0 {
		msg += "；未执行：" + strings.Join(pendingDesc, "、")
	}
	return msg
}

func truncateStr(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}

// ─────────────────────────── Planner LLM ─────────────────────────────────

// planItem 是 Planner LLM 输出的单个工具调用计划
type planItem struct {
	Tool   string            `json:"tool"`
	Params map[string]string `json:"params"`
	Reason string            `json:"reason"`
}

// llmPlanSteps 调用 Planner LLM，从允许的工具集中智能选择需要调用的工具及参数。
// 若 LLM 不可用或解析失败，降级为关键词规则。
func (a *UnifiedAgent) llmPlanSteps(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string) []planItem {
	if !a.cfg.IsRealLLM() {
		return a.rulePlanItems(ctx, query, ts, memPrefix)
	}

	// 构造工具描述
	var toolLines []string
	for name, t := range ts {
		var pDescs []string
		for _, p := range t.Parameters {
			req := ""
			if p.Required {
				req = "（必填）"
			}
			pDescs = append(pDescs, fmt.Sprintf("%s(%s)%s", p.Name, p.Type, req))
		}
		params := strings.Join(pDescs, ", ")
		if params == "" {
			params = "无参数"
		}
		toolLines = append(toolLines, fmt.Sprintf("- %s: %s [参数: %s]", name, t.Description, params))
	}

	planPrompt := fmt.Sprintf(`你是一个任务规划器。根据用户问题，从可用工具中选出真正需要调用的工具（不要为了用工具而用工具，按需选择）。

用户问题：%s

可用工具：
%s

请以 JSON 数组格式输出执行计划，格式如下：
[{"tool":"工具名","params":{"参数名":"参数值"},"reason":"一句话说明为什么调用这个工具"}]

如果无需工具直接回答，输出 []。只输出 JSON，不要其他内容。`,
		query, strings.Join(toolLines, "\n"))

	plannerBase := "你是一个精准的任务规划器，只在必要时才调用工具，不做无意义的调用。"
	if memPrefix != "" {
		plannerBase = memPrefix + "\n\n" + plannerBase + "\n注意：用户偏好可能影响工具参数选择（如城市、时区等），请在参数中体现。"
	}
	raw := a.llm.ChatContext(ctx, plannerBase,
		[]llm.Message{{Role: "user", Content: planPrompt}})

	if ctx.Err() != nil {
		return a.rulePlanItems(ctx, query, ts, memPrefix)
	}

	// 清洗 LLM 输出（可能包含 markdown 代码块）
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var items []planItem
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		log.Printf("⚠️  Planner LLM 解析失败 (%v)，降级到规则规划。原始输出: %s", err, raw)
		return a.rulePlanItems(ctx, query, ts, memPrefix)
	}

	// 过滤：只保留工具集中实际存在的工具
	var valid []planItem
	for _, item := range items {
		if _, ok := ts[item.Tool]; ok {
			if item.Params == nil {
				item.Params = map[string]string{}
			}
			valid = append(valid, item)
		}
	}
	return valid
}

// rulePlanItems 关键词规则降级规划（无真实 LLM 时使用）
func (a *UnifiedAgent) rulePlanItems(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string) []planItem {
	q := strings.ToLower(query)
	var items []planItem

	if _, ok := ts["get_time"]; ok {
		if strings.Contains(q, "时间") || strings.Contains(q, "几点") || strings.Contains(q, "现在") {
			params := map[string]string{}
			if strings.Contains(q, "东京") {
				params["timezone"] = "Asia/Tokyo"
			}
			items = append(items, planItem{Tool: "get_time", Params: params, Reason: "查询当前时间"})
		}
	}
	if _, ok := ts["get_weather"]; ok {
		if strings.Contains(q, "天气") {
			city := "北京"
			for _, c := range []string{"东京", "北京", "上海", "广州", "深圳", "纽约", "伦敦"} {
				if strings.Contains(q, c) {
					city = c
					break
				}
			}
			items = append(items, planItem{Tool: "get_weather", Params: map[string]string{"city": city}, Reason: "查询" + city + "天气"})
		}
	}
	if _, ok := ts["search_web"]; ok {
		if strings.Contains(q, "搜索") || strings.Contains(q, "查询") || strings.Contains(q, "介绍") ||
			strings.Contains(q, "是什么") || strings.Contains(q, "怎么") || strings.Contains(q, "如何") {
			items = append(items, planItem{Tool: "search_web", Params: map[string]string{"query": query}, Reason: "搜索相关信息"})
		}
	}
	if _, ok := ts["rag_search"]; ok {
		items = append(items, planItem{Tool: "rag_search", Params: map[string]string{"query": query}, Reason: "检索个人知识库"})
	}
	// MCP / 自定义工具
	builtins := map[string]bool{"get_time": true, "get_weather": true, "search_web": true, "rag_search": true}
	for name, t := range ts {
		if builtins[name] {
			continue
		}
		params := a.extractParamsForTool(ctx, query, t)
		items = append(items, planItem{Tool: name, Params: params, Reason: "调用工具 " + name})
	}
	return items
}

// ─────────────────────────── Generator LLM ───────────────────────────────

// llmGenerate 调用 Generator LLM，将多个工具观察结果合成为自然语言最终答案
func (a *UnifiedAgent) llmGenerate(ctx context.Context, query string, observations []string, memPrefix string, histMsgs []llm.Message) string {
	if len(observations) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		return a.llm.ChatContext(ctx, systemPrompt, histMsgs)
	}
	if !a.cfg.IsRealLLM() {
		return "综合查询结果：" + strings.Join(observations, "；")
	}

	var obsBuilder strings.Builder
	for i, obs := range observations {
		obsBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, obs))
	}

	genPrompt := fmt.Sprintf(`请根据以下工具执行结果，综合回答用户的问题。回答要自然流畅、重点突出，不要机械罗列原始数据，也不要重复问题本身。

用户问题：%s

工具执行结果：
%s`, query, obsBuilder.String())

	generatorBase := "你是一个善于综合信息的AI助手，能将多个工具的执行结果整合成清晰自然的回答。"
	if memPrefix != "" {
		generatorBase = memPrefix + "\n\n" + generatorBase + "\n结合用户偏好，使回答更个性化。"
	}
	return a.llm.ChatContext(ctx, generatorBase,
		[]llm.Message{{Role: "user", Content: genPrompt}})
}

// extractParamsForTool 用 LLM 从 query 中提取工具所需参数；无法调用时用 query 填充首个必填参数
func (a *UnifiedAgent) extractParamsForTool(ctx context.Context, query string, t tools.Tool) map[string]string {
	result := make(map[string]string)
	if len(t.Parameters) == 0 {
		return result
	}
	if !a.cfg.IsRealLLM() {
		for _, p := range t.Parameters {
			if p.Required {
				result[p.Name] = query
				break
			}
		}
		return result
	}
	var lines []string
	for _, p := range t.Parameters {
		req := ""
		if p.Required {
			req = "（必填）"
		}
		lines = append(lines, fmt.Sprintf("- %s (%s)%s: %s", p.Name, p.Type, req, p.Description))
	}
	prompt := fmt.Sprintf(
		"从下面的用户消息中提取工具「%s」所需的参数，以JSON对象格式输出，只输出JSON，不加任何说明。\n\n参数说明：\n%s\n\n用户消息：%s",
		t.Name, strings.Join(lines, "\n"), query,
	)
	raw := a.llm.ChatContext(ctx, "", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		// LLM 输出无法解析时兜底：用 query 填充首个必填参数
		for _, p := range t.Parameters {
			if p.Required {
				result[p.Name] = query
				break
			}
		}
	}
	return result
}

// ─────────────────────────────── Stage 6：Harness ────────────────────────

// executeStepWithRetryTool 带重试的步骤执行，使用传入的具体工具实例
func (a *UnifiedAgent) executeStepWithRetryTool(ctx context.Context, step *TaskStep, tool tools.Tool) bool {
	params := make(map[string]interface{}, len(step.Params))
	for k, v := range step.Params {
		params[k] = v
	}
	for attempt := 0; attempt < a.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			step.Error = "被用户中断"
			return false
		}
		result, err := tool.Execute(params)
		if err == nil {
			step.Result = result
			return true
		}
		if ctx.Err() != nil {
			step.Error = "被用户中断"
			return false
		}
		step.RetryCount = attempt + 1
		step.Error = err.Error()
		time.Sleep(time.Duration(a.cfg.RetryDelayMs) * time.Millisecond)
	}
	return false
}

// executeStepWithRetry 带重试的步骤执行，失败时按配置延迟后重试
func (a *UnifiedAgent) executeStepWithRetry(step *TaskStep) bool {
	tool, ok := a.tools[step.ToolName]
	if !ok {
		return false
	}
	params := make(map[string]interface{}, len(step.Params))
	for k, v := range step.Params {
		params[k] = v
	}
	for attempt := 0; attempt < a.cfg.MaxRetries; attempt++ {
		result, err := tool.Execute(params)
		if err == nil {
			step.Result = result
			return true
		}
		step.RetryCount = attempt + 1
		step.Error = err.Error()
		time.Sleep(time.Duration(a.cfg.RetryDelayMs) * time.Millisecond)
	}
	return false
}

// saveSnapshot 对当前 TaskState 做深拷贝快照并持久化到 PG
func (a *UnifiedAgent) saveSnapshot() {
	var stateCopy TaskState
	data, _ := json.Marshal(a.task)
	json.Unmarshal(data, &stateCopy)
	snap := Snapshot{State: stateCopy, Timestamp: time.Now().Format("15:04:05")}
	a.snapshots = append(a.snapshots, snap)
	a.inf.SaveSnapshot(a.task.TaskID, data)
}

// ─────────────────────────────── Stage 5：Memory（基础层，注入所有模式）────────
//
// 旧的 buildMemorySystemPrefix / buildMemorySystemPrefixWithCtx 已删除，
// 由 buildContextPrefix → runtime.ContextAssembler 取代（Schema-driven 装配）。

// fillParamsFromPreference 用用户偏好自动补全工具调用参数中缺失的值
// 例如：偏好中有 "城市:北京"，则当工具参数含 city 但为空时自动填入
func (a *UnifiedAgent) fillParamsFromPreference(tc *tools.CallResult) {
	if tc == nil || len(a.pref.Data) == 0 {
		return
	}
	// 偏好 key → 工具参数名的映射
	prefToParam := map[string][]string{
		"城市":   {"city", "location", "location_name"},
		"时区":   {"timezone", "tz", "time_zone"},
		"姓名":   {"name", "username", "user_name"},
		"语言":   {"language", "lang"},
		"国家":   {"country", "nation"},
	}
	for prefKey, paramNames := range prefToParam {
		prefVal, ok := a.pref.Data[prefKey]
		if !ok || prefVal == "" {
			continue
		}
		for _, paramName := range paramNames {
			if v, exists := tc.Params[paramName]; !exists || v == nil || fmt.Sprint(v) == "" {
				tc.Params[paramName] = prefVal
			}
		}
	}
}

// extractMemoryFromReply 从 assistant 回复中提取值得记忆的信息并存入长期记忆。
// 写入前用规则层 + LLM 兜底对内容分类（category/tags/slot_hint），
// 使 Schema-driven 装配机制能按槽位过滤召回。
func (a *UnifiedAgent) extractMemoryFromReply(answer string) {
	if answer == "" || !a.cfg.IsRealLLM() {
		return
	}
	// 用 LLM 提取 k-v 事实
	prompt := `从下面这段AI回复中，提取值得长期记住的客观事实或用户偏好信息。
只提取明确的、非临时性的信息，忽略对话上下文和临时细节。
输出 JSON 对象（key为中文名称，value为具体值），如果没有值得记忆的信息则输出 {}。
只输出 JSON，不要有其他内容。

回复：` + answer
	raw := a.llm.Chat("", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	var kvs map[string]string
	if err := json.Unmarshal([]byte(raw), &kvs); err != nil || len(kvs) == 0 {
		return
	}
	for k, v := range kvs {
		if k == "" || v == "" {
			continue
		}
		a.pref.Save(k, v)
		a.inf.SavePreference("default", k, v)
		content := fmt.Sprintf("用户%s: %s", k, v)

		// ── 分类管线：规则优先，LLM 兜底 ──
		category, tags, slotHint := classifyMemoryContent(k, v)
		if category == "" {
			category, tags, slotHint = a.llmClassifyMemory(content)
		}

		emb, _ := a.llm.Embed(content)
		if a.graphMem != nil {
			if added, _ := a.graphMem.StoreClassified(content, 0.7, emb, category, tags, slotHint); added {
				embJSON, _ := json.Marshal(emb)
				pgID := a.inf.SaveLongTermItemClassified(content, 0.7, embJSON, category, tags, slotHint)
				a.graphMem.SyncLastItemPGID(pgID)
			}
		} else if a.ltm.StoreClassified(content, 0.7, emb, category, tags, slotHint) {
			embJSON, _ := json.Marshal(emb)
			pgID := a.inf.SaveLongTermItemClassified(content, 0.7, embJSON, category, tags, slotHint)
			a.ltm.SyncLastItemPGID(pgID)
		}
		log.Printf("🧠 从回复中提取记忆：%s = %s（类别=%s）", k, v, category)
	}
}

// classifyMemoryContent 用正则规则快速分类；返回空字符串表示规则未命中，由 LLM 兜底
func classifyMemoryContent(key, value string) (category string, tags []string, slotHint string) {
	combined := key + value
	switch {
	case containsAny(combined, "叫", "名字", "姓名", "是我", "我是"):
		return "identity", []string{"name"}, "profile"
	case containsAny(combined, "喜欢", "偏好", "习惯", "爱好", "讨厌", "不喜欢"):
		return "preference", []string{"preference"}, "profile"
	case containsAny(combined, "工具", "失败", "错误", "报错", "异常"):
		return "tool_failure", []string{"tool", "error"}, "tool_state"
	case containsAny(combined, "禁止", "不要", "不能", "必须", "强制"):
		return "policy", []string{"constraint"}, "constraints"
	default:
		return "", nil, ""
	}
}

// containsAny 检查 s 是否包含 subs 中任意子串
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// llmClassifyMemory 调用一次 LLM 对记忆内容做 JSON 分类，
// 返回 category / tags / slotHint；失败时回退到 "general"
func (a *UnifiedAgent) llmClassifyMemory(content string) (category string, tags []string, slotHint string) {
	if !a.cfg.IsRealLLM() {
		return "general", nil, ""
	}
	prompt := `请对以下记忆内容进行分类，只输出 JSON，格式如下：
{"category":"identity|preference|fact|episodic|tool_failure|policy|general","tags":["tag1"],"slot_hint":"profile|planner|task_memory|tool_state|constraints|recall_memory"}

记忆内容：` + content
	raw := a.llm.Chat("", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(raw), "```json"), "```"))
	var result struct {
		Category string   `json:"category"`
		Tags     []string `json:"tags"`
		SlotHint string   `json:"slot_hint"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil || result.Category == "" {
		return "general", nil, ""
	}
	return result.Category, result.Tags, result.SlotHint
}

// syncConsolidationToDB 将记忆合并结果同步到 PostgreSQL
func (a *UnifiedAgent) syncConsolidationToDB(result memory.ConsolidationResult) {
	if len(result.DeleteFromDB) > 0 {
		a.inf.DeleteLongTermItems(result.DeleteFromDB)
		log.Printf("🧹 记忆合并：删除 %d 条（去重=%d, 合并=%d, 过期=%d）",
			result.Deduped+result.Merged+result.Expired, result.Deduped, result.Merged, result.Expired)
	}
	for _, item := range result.UpdateInDB {
		embJSON, _ := json.Marshal(item.Embedding)
		a.inf.UpdateLongTermItem(item.ID, item.Content, item.Importance, embJSON)
		log.Printf("🔗 记忆合并：更新 id=%d", item.ID)
	}
}

// restoreFromDB 启动时从 PostgreSQL 恢复跨会话偏好、长期记忆和聊天记录
func (a *UnifiedAgent) restoreFromDB() {
	// 恢复偏好
	prefs := a.inf.LoadPreferences("default")
	a.pref.SaveBatch(prefs)

	// 恢复长期记忆
	rows := a.inf.LoadLongTermItems()
	for _, row := range rows {
		a.ltm.StoreItem(memory.Item{
			ID:           row.ID,
			Content:      row.Content,
			Importance:   row.Importance,
			Embedding:    row.Embedding,
			CreatedAt:    row.CreatedAt,
			LastAccessed: row.LastAccessed,
		})
	}

	// 恢复聊天记录到短期记忆（最近 N 条）
	chatLimit := a.cfg.ShortTermMaxTurns * 2 // 每轮 = user + assistant
	history := a.inf.LoadChatHistory(chatLimit)
	for _, h := range history {
		a.stm.Add(h.Role, h.Content)
	}

	if len(prefs) > 0 || len(rows) > 0 || len(history) > 0 {
		log.Printf("✅ 记忆恢复：%d 条偏好，%d 条长期记忆，%d 条聊天记录", len(prefs), len(rows), len(history))
	}
}

// restoreRAGFromDB 从 PostgreSQL 加载持久化的 RAG chunks 到 TF 兜底索引
func (a *UnifiedAgent) restoreRAGFromDB() {
	chunkRows, err := a.inf.LoadAllRAGChunks()
	if err != nil || len(chunkRows) == 0 {
		return
	}
	var chunks []rag.Chunk
	for i, row := range chunkRows {
		chunks = append(chunks, rag.Chunk{ID: i, Content: row.Content})
	}
	a.rag.RestoreChunks(chunks)
	log.Printf("✅ RAG chunks 恢复：%d 条", len(chunks))
}

// initKnowledgeGraph 初始化 Neo4j 知识图谱存储，并注入到 RAG 引擎 + GraphMemory
func (a *UnifiedAgent) initKnowledgeGraph() {
	kg := graph.NewKGStore(a.cfg, func(systemPrompt, userMsg string) string {
		return a.llm.Chat(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
	})
	a.kg = kg
	a.rag.SetKGStore(kg)

	// 构建图记忆层（包装现有 ltm）
	a.graphMem = memory.NewGraphMemory(a.ltm, kg, a.cfg.MemoryConsolidationSimilarity)
	a.graphMem.SyncPrevID() // 从 DB 恢复后对齐 prevID

	if kg.Available() {
		log.Printf("🕸️  知识图谱已就绪（Neo4j），RAG 升级为三路混合检索，记忆系统已接入图层")
	} else {
		log.Printf("ℹ️  Neo4j 不可用，RAG 保持双路检索，记忆系统退化为纯向量模式")
	}
}

// KG 暴露知识图谱实例，供 HTTP handler 或记忆模块使用
func (a *UnifiedAgent) KG() *graph.KGStore { return a.kg }

// initSandbox 初始化命令执行沙箱并注册 exec_command 工具
func (a *UnifiedAgent) initSandbox() {
	if !a.cfg.SandboxEnabled {
		log.Printf("ℹ️  沙箱未启用（config.sandbox.enabled=false），跳过 exec_command 工具")
		return
	}

	sbCfg := sandbox.SandboxConfig{
		Image:           a.cfg.SandboxImage,
		Timeout:         time.Duration(a.cfg.SandboxTimeoutMs) * time.Millisecond,
		MaxOutputBytes:  a.cfg.SandboxMaxOutput,
		MemoryLimitMB:   a.cfg.SandboxMemoryMB,
		CPUPercent:      a.cfg.SandboxCPUPercent,
		MaxPIDs:         a.cfg.SandboxMaxPIDs,
		NetworkDisabled: a.cfg.SandboxNetDisabled,
		ReadOnlyRootfs:  a.cfg.SandboxReadOnly,
	}
	secCfg := sandbox.SecurityConfig{
		MaxCommandLength: a.cfg.SecMaxCmdLength,
		AllowlistMode:    a.cfg.SecAllowlistMode,
		Allowlist:        a.cfg.SecAllowlist,
	}

	sb := sandbox.NewSandbox(a.cfg.SandboxBackend, sbCfg, secCfg)

	// 注入审计回调：将每条命令执行结果发送到 Kafka
	sb.SetAuditFn(func(r sandbox.ExecResult) {
		event, _ := json.Marshal(map[string]interface{}{
			"command":     r.Command,
			"level":       string(r.Validation.Level),
			"exit_code":   r.ExitCode,
			"duration_ms": r.Duration.Milliseconds(),
			"backend":     r.Backend,
			"killed":      r.Killed,
			"truncated":   r.Truncated,
			"reason":      r.Validation.Reason,
			"violations":  r.Validation.Violations,
		})
		a.inf.PublishEvent("sandbox.exec", string(event))
	})

	a.sandbox = sb
	a.tools["exec_command"] = tools.ExecCommandTool(sb)
	log.Printf("🛡️  沙箱已就绪，后端=%s，exec_command 工具已注册", sb.Backend())
}

// Sandbox 暴露沙箱实例，供 HTTP handler 或前端查询状态
func (a *UnifiedAgent) Sandbox() *sandbox.Sandbox { return a.sandbox }
