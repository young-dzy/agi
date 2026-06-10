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
	"agi-ai-assitant/config"
	"agi-ai-assitant/internal/graph"
	"agi-ai-assitant/internal/llm"
	"agi-ai-assitant/internal/memory"
	"agi-ai-assitant/internal/promptctx"
	"agi-ai-assitant/internal/rag"
	"agi-ai-assitant/internal/repo/chathistory"
	"agi-ai-assitant/internal/repo/eventbus"
	"agi-ai-assitant/internal/repo/longterm"
	"agi-ai-assitant/internal/repo/preference"
	"agi-ai-assitant/internal/repo/ragchunk"
	"agi-ai-assitant/internal/repo/snapshot"
	"agi-ai-assitant/internal/sandbox"
	"agi-ai-assitant/internal/tools"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
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
	StepPending     TaskStepStatus = "pending"
	StepRunning     TaskStepStatus = "running"
	StepDone        TaskStepStatus = "done"
	StepFailed      TaskStepStatus = "failed"
	StepInterrupted TaskStepStatus = "interrupted"
)

// TaskStep 是 Harness 中可重试的原子执行单元
type TaskStep struct {
	ID         int               `json:"id"`
	Name       string            `json:"name"`
	ToolName   string            `json:"tool_name"`
	Params     map[string]string `json:"params"`
	Status     TaskStepStatus    `json:"status"`
	Result     string            `json:"result,omitempty"`
	Error      string            `json:"error,omitempty"`
	RetryCount int               `json:"retry_count"`
}

// TaskState 描述一次任务的完整执行状态
type TaskState struct {
	TaskID        string     `json:"task_id"`
	Query         string     `json:"query"`
	Status        string     `json:"status"` // "running" | "completed" | "interrupted"
	Phase         string     `json:"phase"`  // "planning" | "executing" | "generating" | "done" | "interrupted"
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
	Query          string             `json:"query"`
	Answer         string             `json:"answer"`
	Mode           string             `json:"mode"` // chat / tool / rag / memory / react
	Steps          []ReActStep        `json:"steps,omitempty"`
	ToolCall       *tools.CallResult  `json:"tool_call,omitempty"`
	SearchResults  []rag.SearchResult `json:"search_results,omitempty"`
	Task           *TaskState         `json:"task,omitempty"`
	ExtractedInfo  string             `json:"extracted_info,omitempty"`
	ShortTermCount int                `json:"short_term_count"`
	LongTermCount  int                `json:"long_term_count"`
	Preferences    map[string]string  `json:"preferences"`
	Interrupted    bool               `json:"interrupted,omitempty"`
}

// ─────────────────────────────── SSE 流式事件 ────────────────────────────────

// StreamEvent 是 SSE 流式推送的事件，handler 逐条写入 EventStream
type StreamEvent struct {
	Type string      `json:"type"` // route / step / token / tool_call / rag_result / memory / done
	Data interface{} `json:"data"`
}

// NewStreamEvent 创建一个 SSE 事件
func NewStreamEvent(eventType string, data interface{}) StreamEvent {
	return StreamEvent{Type: eventType, Data: data}
}

// ─────────────────────────────── Unified Agent ───────────────────────────

// UnifiedAgent 整合全部能力，是系统的核心调度入口
type UnifiedAgent struct {
	cfg      *config.APIConfig
	llm      *llm.Client
	rag      *rag.Engine
	stm      *memory.ShortTerm
	ltm      *memory.LongTerm    // 保留直接引用，供 handler 暴露
	graphMem *memory.GraphMemory // 图增强记忆层（包装 ltm）
	pref     *memory.Preference
	sandbox  *sandbox.Sandbox
	kg       *graph.KGStore // 知识图谱（RAG + 记忆图共享）

	// 数据访问层（每个 domain 用各自的 repo 接口）
	chatRepo     chathistory.Repo
	prefRepo     preference.Repo
	snapRepo     snapshot.Repo
	ltmRepo      longterm.Repo
	ragChunkRepo ragchunk.Repo
	events       eventbus.Publisher

	// infraStatus 是 platform 层连接健康状态的快照（用于 status 端点）
	// key: "milvus" | "pg" | "elasticsearch" | "kafka" | "neo4j"
	// value: "connected" | "disconnected"
	infraStatus map[string]string

	// RAG 维度（启动期 ragchunk repo 初始化用）
	ragMilvusDim int

	// Schema-driven Runtime Context Assembly
	assembler   *promptctx.ContextAssembler
	taskMem     *promptctx.TaskMemBuffer
	toolTracker *promptctx.ToolStateTracker

	// 工具集：可被 RegisterTool（MCP 热插）并发写入，被 ReAct/Decide 并发读取。
	// Go map 并发读写会直接 panic，必须串行化。toolsMu 独立于 mu 以避免锁粒度过大。
	toolsMu sync.RWMutex
	tools   map[string]tools.Tool

	// per-request 共享状态：snapshots、当前任务、in-flight cancel funcs
	//
	// 并发：mu 串行化对 task/snapshots/cancelFns 的所有读写。
	// 旧实现把这三个字段当无锁全局变量，多请求并发时数据竞争 + Cancel()
	// 因 cancelFn 互相覆盖只能取消最近一次请求；这里改为 cancelFns map，
	// 每个 in-flight 请求一个 token，Cancel() 触发全部。
	mu           sync.Mutex
	task         *TaskState
	snapshots    []Snapshot
	cancelFns    map[int64]context.CancelFunc
	nextCancelID int64
}

// Deps 是 UnifiedAgent 的依赖注入容器，由 main.go 在启动期组装。
type Deps struct {
	ChatRepo     chathistory.Repo
	PrefRepo     preference.Repo
	SnapRepo     snapshot.Repo
	LTMRepo      longterm.Repo
	RAGChunkRepo ragchunk.Repo
	Events       eventbus.Publisher
	// InfraStatus 平台层连接健康快照
	InfraStatus map[string]string
}

// New 创建并初始化 UnifiedAgent
func New(cfg *config.APIConfig, deps Deps) *UnifiedAgent {
	llmClient := llm.New(cfg)
	ragEngine := rag.NewEngine(cfg, deps.RAGChunkRepo, deps.Events)
	ltm := memory.NewLongTerm()
	a := &UnifiedAgent{
		cfg:          cfg,
		llm:          llmClient,
		rag:          ragEngine,
		tools:        tools.DefaultTools(),
		stm:          memory.NewShortTerm(cfg.ShortTermMaxTurns),
		ltm:          ltm,
		pref:         memory.NewPreference(),
		chatRepo:     deps.ChatRepo,
		prefRepo:     deps.PrefRepo,
		snapRepo:     deps.SnapRepo,
		ltmRepo:      deps.LTMRepo,
		ragChunkRepo: deps.RAGChunkRepo,
		events:       deps.Events,
		infraStatus:  deps.InfraStatus,
		ragMilvusDim: cfg.RAGMilvusDim,
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
	// 启动期 IO 并发：以下 4 项互不依赖，串行总耗时是各自之和（PG 全量加载 + Milvus 建表
	// + ES 建索引 + Docker probe 1.5s + Neo4j 5s 验证），并行后压缩到最慢一项的耗时。
	//
	//   - InitRAGInfra      建 Milvus collection + ES 索引
	//   - restoreFromDB     从 PG 恢复偏好 / 长期记忆 / 聊天记录
	//   - restoreRAGFromDB  从 PG 恢复 RAG chunks
	//   - initSandbox       Docker daemon 探测 + exec_command 工具注册
	//
	// initKnowledgeGraph 依赖 restoreFromDB 完成后的 ltm.Items（SyncPrevID 读取最后一条），
	// 因此放在并发组之后单独执行。
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); a.ragChunkRepo.Init(cfg.RAGMilvusDim) }()
	go func() { defer wg.Done(); a.restoreFromDB() }()
	go func() { defer wg.Done(); a.restoreRAGFromDB() }()
	go func() { defer wg.Done(); a.initSandbox() }()
	// 将 RAG 注册为可选工具（私人黑洞知识库检索）。
	// 通过 RegisterTool 持锁写入，避免与并发的 initSandbox（也写 a.tools["exec_command"]）竞争。
	a.RegisterTool(tools.Tool{
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
	})
	// 用 LLM 知识 + 可选 Tavily API 替换默认的 mock search_web
	a.RegisterTool(tools.Tool{
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
				if result, err := tools.TavilySearch(q, a.cfg.SearchAPIKey, a.cfg.SearchAPIURL); err == nil {
					return result, nil
				}
			}
			// 降级：用 LLM 知识库回答
			return a.llm.Chat(
				"你是一个知识丰富的搜索引擎助手。请基于你的知识，对用户的搜索问题给出准确、详细的回答。直接给出答案，不要说「我不知道」或「我无法搜索」。",
				[]llm.Message{{Role: "user", Content: "搜索：" + q}},
			), nil
		},
	})
	// 等待第一阶段并发 init 完成（restoreFromDB / restoreRAGFromDB / InitRAGInfra / initSandbox）
	wg.Wait()
	// 第二阶段：知识图谱依赖 restoreFromDB 加载的 ltm 才能 SyncPrevID
	a.initKnowledgeGraph()

	// ── Schema-driven Runtime Context Assembly ──
	a.taskMem = promptctx.NewTaskMemBuffer(20)
	a.toolTracker = promptctx.NewToolStateTracker(10)

	reg := promptctx.NewSourceRegistry()
	reg.Register(promptctx.NewProfileSource(a.pref, a.ltm))
	reg.Register(promptctx.NewPlannerSource(func() *promptctx.PlannerSnapshot {
		t := a.currentTask() // 持锁读取，避免与 ReAct 循环并发写打架
		if t == nil {
			return nil
		}
		snap := &promptctx.PlannerSnapshot{
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
	reg.Register(promptctx.NewTaskMemSource(a.taskMem))
	reg.Register(promptctx.NewToolStateSource(
		// 持读锁拷贝供 ToolStateSource 装配 prompt：每次调用都拿一致的工具集快照
		a.toolsSnapshot,
		a.toolTracker,
	))
	reg.Register(promptctx.NewConstraintsSource(sandbox.PolicySnapshot()))
	// RecallSource 优先用图记忆；graphMem 在 initKnowledgeGraph 中就绪
	if a.graphMem != nil {
		reg.Register(promptctx.NewRecallSource(a.graphMem))
	} else {
		reg.Register(promptctx.NewRecallSource(a.ltm))
	}
	a.assembler = promptctx.NewAssembler(promptctx.DefaultSchemas(), reg)

	return a
}

// RegisterTool 动态注册一个工具（支持 MCP 工具热插入）
//
// 持 toolsMu.Lock 串行化对工具 map 的写入，避免与 ReAct/Decide 并发读冲突
// （Go map 并发读写会直接 panic，不只是脏读）。
func (a *UnifiedAgent) RegisterTool(t tools.Tool) {
	a.toolsMu.Lock()
	a.tools[t.Name] = t
	a.toolsMu.Unlock()
}

// RAG 暴露 RAG 引擎，供 HTTP handler 直接调用 Ingest
func (a *UnifiedAgent) RAG() *rag.Engine { return a.rag }

// Tools 暴露工具集（持锁拷贝），供 HTTP handler 列出工具信息。
// 调用方拿到的是快照，可无锁安全使用，且修改不影响 agent 内部 map。
func (a *UnifiedAgent) Tools() map[string]tools.Tool {
	return a.toolsSnapshot()
}

// toolsSnapshot 持锁返回工具 map 的浅拷贝（Tool 内部字段不可变，浅拷贝足够）
// 路由层（runReAct*/runTool*/Decide）调用一次后即可无锁使用，且能保证整次
// 调用看到一致的工具集（不被 in-flight RegisterTool 干扰）。
func (a *UnifiedAgent) toolsSnapshot() map[string]tools.Tool {
	a.toolsMu.RLock()
	defer a.toolsMu.RUnlock()
	cp := make(map[string]tools.Tool, len(a.tools))
	for k, v := range a.tools {
		cp[k] = v
	}
	return cp
}

// ShortTerm 暴露短期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) ShortTerm() *memory.ShortTerm { return a.stm }

// LongTerm 暴露长期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) LongTerm() *memory.LongTerm { return a.ltm }

// Preferences 暴露用户偏好，供 HTTP handler 查询
func (a *UnifiedAgent) Preferences() *memory.Preference { return a.pref }

// Snapshots 返回历史快照列表（持锁拷贝）
func (a *UnifiedAgent) Snapshots() []Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]Snapshot, len(a.snapshots))
	copy(cp, a.snapshots)
	return cp
}

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
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, ChatOptions{Explicit: false})
}

// ProcessWithOptions 带显式选项的入口，供前端精确控制路由
func (a *UnifiedAgent) ProcessWithOptions(query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, opts)
}

// ProcessContext 带 context 的入口，支持 SSE 流式和取消
func (a *UnifiedAgent) ProcessContext(ctx context.Context, query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, opts)
}

// ProcessStream 流式处理入口，在关键节点通过 onEvent 回调推送 SSE 事件。
// 返回完整的 Response（与 Process 一致），同时通过回调实时推送中间事件。
func (a *UnifiedAgent) ProcessStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.processStream(ctx, query, opts, onEvent)
}

// Cancel 取消所有当前正在执行的任务（每个 in-flight 请求都会收到取消信号）
func (a *UnifiedAgent) process(ctx context.Context, query string, opts ChatOptions) *Response {
	resp := &Response{Query: query, Mode: "chat"}

	// 更新短期记忆
	a.stm.Add("user", query)

	// 持久化用户消息到 PG
	a.chatRepo.Save("user", query)

	// 偏好提取：优先 LLM，降级规则
	a.goSafe("process.preference-extract", func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) > 0 {
			a.pref.SaveBatch(kvs)
			for k, v := range kvs {
				a.prefRepo.Save("default", k, v)
				content := fmt.Sprintf("用户%s: %s", k, v)
				emb, _ := a.llm.Embed(content)
				if added, _ := a.graphMem.Store(content, 0.8, emb); added {
					embJSON, _ := json.Marshal(emb)
					pgID := a.ltmRepo.Save(content, 0.8, embJSON)
					a.graphMem.SyncLastItemPGID(pgID)
				}
			}
		}
	})

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
			routeTools = a.toolsSnapshot()
		case a.needTool(query):
			mode = "tool"
			routeTools = a.toolsSnapshot()
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
	a.chatRepo.Save("assistant", resp.Answer)

	// 从 assistant 回答中提取可记忆信息
	a.goSafe("process.memory-extract", func() { a.extractMemoryFromReply(resp.Answer) })

	// 异步触发记忆合并（去重+合并+衰减+过期；有图层时使用图感知合并以保护高中心度节点）
	a.goSafe("process.consolidate", func() {
		if a.ltm.NeedConsolidation() {
			var result memory.ConsolidationResult
			if a.graphMem != nil {
				result = a.graphMem.GraphAwareConsolidate()
			} else {
				result = a.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.events.Publish("agent.chat", string(eventData))

	resp.ShortTermCount = a.stm.Count()
	resp.LongTermCount = a.ltm.Count()
	resp.Preferences = a.pref.Snapshot()
	return resp
}

// processStream 与 process 逻辑一致，但在关键节点通过 onEvent 推送 SSE 事件
func (a *UnifiedAgent) processStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	if onEvent == nil {
		return a.process(ctx, query, opts)
	}

	resp := &Response{Query: query, Mode: "chat"}

	a.stm.Add("user", query)
	a.chatRepo.Save("user", query)

	// 偏好提取（异步，与 process 一致）
	a.goSafe("processStream.preference-extract", func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) > 0 {
			a.pref.SaveBatch(kvs)
			for k, v := range kvs {
				a.prefRepo.Save("default", k, v)
				content := fmt.Sprintf("用户%s: %s", k, v)
				emb, _ := a.llm.Embed(content)
				if added, _ := a.graphMem.Store(content, 0.8, emb); added {
					embJSON, _ := json.Marshal(emb)
					pgID := a.ltmRepo.Save(content, 0.8, embJSON)
					a.graphMem.SyncLastItemPGID(pgID)
				}
			}
		}
	})

	// 同步规则提取
	if key, value, ok := a.pref.ExtractAndSave(query); ok {
		resp.ExtractedInfo = fmt.Sprintf("已记住：%s = %s", key, value)
		onEvent(NewStreamEvent("memory", map[string]string{"extracted_info": resp.ExtractedInfo}))
	}

	// ── 路由决策 ──
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
			routeTools = a.toolsSnapshot()
		case a.needTool(query):
			mode = "tool"
			routeTools = a.toolsSnapshot()
		case a.needRAG(query):
			mode = "rag"
		default:
			mode = "chat"
		}
	}
	resp.Mode = mode
	onEvent(NewStreamEvent("route", map[string]string{"mode": mode}))

	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(query)

	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		onEvent(NewStreamEvent("done", resp))
		return resp
	}

	// ── 分发执行（流式版） ──
	switch mode {
	case "react":
		answer, steps, task := a.runReActStream(ctx, query, routeTools, memPrefix, histMsgs, onEvent)
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runToolStream(ctx, query, routeTools, memPrefix, histMsgs, onEvent)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		answer, results := a.rag.Query(query)
		resp.Answer, resp.SearchResults = answer, results
		onEvent(NewStreamEvent("rag_result", map[string]interface{}{"search_results": results}))
		onEvent(NewStreamEvent("token", map[string]string{"content": answer}))
	default:
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
	}

	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.stm.Add("assistant", resp.Answer)
	a.chatRepo.Save("assistant", resp.Answer)

	a.goSafe("processStream.memory-extract", func() { a.extractMemoryFromReply(resp.Answer) })

	a.goSafe("processStream.consolidate", func() {
		if a.ltm.NeedConsolidation() {
			var result memory.ConsolidationResult
			if a.graphMem != nil {
				result = a.graphMem.GraphAwareConsolidate()
			} else {
				result = a.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.events.Publish("agent.chat", string(eventData))

	resp.ShortTermCount = a.stm.Count()
	resp.LongTermCount = a.ltm.Count()
	resp.Preferences = a.pref.Snapshot()
	onEvent(NewStreamEvent("done", resp))
	return resp
}

// buildContextPrefix 调用 Schema-driven ContextAssembler，返回当次推理的系统提示前缀
func (a *UnifiedAgent) buildContextPrefix(ctx context.Context, query string, mode string) string {
	if a.assembler == nil {
		return ""
	}
	emb, _ := a.llm.EmbedContext(ctx, query)
	taskID := ""
	if t := a.currentTask(); t != nil {
		taskID = t.TaskID
	}
	rc := a.assembler.Assemble(ctx, promptctx.Query{
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
	// 通过 Snapshot 拿到一致性副本，避免遍历期间 Add 并发改写底层切片
	for _, m := range a.stm.Snapshot() {
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

// filterTools 按名称列表过滤可用工具集（持读锁）
func (a *UnifiedAgent) filterTools(names []string) map[string]tools.Tool {
	a.toolsMu.RLock()
	defer a.toolsMu.RUnlock()
	result := make(map[string]tools.Tool, len(names))
	for _, name := range names {
		if t, ok := a.tools[name]; ok {
			result[name] = t
		}
	}
	return result
}

// needReActFromTools — 只要工具集非空就走 ReAct，保证每次工具调用都有完整推理轨迹
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
			a.toolTracker.Record(promptctx.ToolCallTrace{
				ToolName: tc.ToolName, Success: false, Summary: err.Error(),
			})
		}
		return fmt.Sprintf("工具执行失败: %v", err), tc
	}
	tc.ToolResult = result
	if a.toolTracker != nil {
		a.toolTracker.Record(promptctx.ToolCallTrace{
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

	// 把 task 持有为本地变量：ReAct 循环的所有读写都走本地，
	// 多请求并发时彼此互不干扰。setTask 把它发布到 agent 共享状态供
	// PlannerSource / Snapshots 等只读访问。
	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing", Steps: taskSteps,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	// ── Step 2: 按 Planner 计划逐步执行工具 ───────────────────────────────
	for i := range task.Steps {
		// 每步开始前检查 context 是否已取消
		if ctx.Err() != nil {
			task.Phase = "interrupted"
			task.Status = "interrupted"
			task.InterruptedAt = i
			// 将当前步骤标记为中断
			task.Steps[i].Status = StepInterrupted
			// 生成中断摘要
			interruptMsg := a.buildInterruptMessage(task)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg})
			a.saveSnapshot(task)
			return "[已中断] " + interruptMsg, reactSteps, task
		}

		ts2 := &task.Steps[i]
		task.CurrentStep = i
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
			a.saveSnapshot(task)
			continue
		}
		if a.executeStepWithRetryTool(ctx, ts2, tool) {
			ts2.Status = StepDone
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: ts2.Result})
			observations = append(observations, fmt.Sprintf("[%s] %s", ts2.ToolName, ts2.Result))
			if a.taskMem != nil {
				a.taskMem.Push(promptctx.StepObservation{
					StepID: ts2.ID, ToolName: ts2.ToolName,
					Result: ts2.Result, Success: true,
				})
			}
			if a.toolTracker != nil {
				a.toolTracker.Record(promptctx.ToolCallTrace{
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
					a.taskMem.Push(promptctx.StepObservation{
						StepID: ts2.ID, ToolName: ts2.ToolName,
						Error: ts2.Error, Success: false,
					})
				}
				if a.toolTracker != nil {
					a.toolTracker.Record(promptctx.ToolCallTrace{
						ToolName: ts2.ToolName, Success: false, Summary: ts2.Error,
					})
				}
			}
		}
		a.saveSnapshot(task)
	}

	// ── Step 3: Generator LLM 综合所有观察结果生成最终答案 ────────────────
	if ctx.Err() != nil {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessage(task)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	task.Phase = "generating"
	answer := a.llmGenerate(ctx, query, observations, memPrefix, histMsgs)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

// runToolStream 流式版本的单工具调用：推送 tool_call 和 token 事件
func (a *UnifiedAgent) runToolStream(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) (string, *tools.CallResult) {
	tc := tools.Decide(query, ts)
	if tc == nil {
		return "我无法处理这个请求。", nil
	}
	tool, ok := ts[tc.ToolName]
	if !ok {
		return fmt.Sprintf("工具 %s 不存在", tc.ToolName), tc
	}

	a.fillParamsFromPreference(tc)

	result, err := tool.Execute(tc.Params)
	if err != nil {
		if ctx.Err() != nil {
			return "[已中断]", tc
		}
		if a.toolTracker != nil {
			a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: tc.ToolName, Success: false, Summary: err.Error()})
		}
		return fmt.Sprintf("工具执行失败: %v", err), tc
	}
	tc.ToolResult = result
	if a.toolTracker != nil {
		a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: tc.ToolName, Success: true, Summary: result})
	}

	onEvent(NewStreamEvent("tool_call", map[string]interface{}{
		"tool_name":   tc.ToolName,
		"params":      tc.Params,
		"tool_result": result,
	}))

	systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个善于综合信息的AI助手。结合你掌握的用户信息，使回答更个性化。")
	userMsg := fmt.Sprintf("用户问：%s\n工具 %s 返回结果：%s\n请根据结果自然地回答用户。", query, tc.ToolName, result)
	answer := a.llm.ChatStreamContext(ctx, systemPrompt, []llm.Message{{Role: "user", Content: userMsg}}, func(token string) {
		onEvent(NewStreamEvent("token", map[string]string{"content": token}))
	})
	return answer, tc
}

// runReActStream 流式版本的 ReAct 循环：逐步推送 step 和 token 事件
func (a *UnifiedAgent) runReActStream(ctx context.Context, query string, ts map[string]tools.Tool, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) (string, []ReActStep, *TaskState) {
	var reactSteps []ReActStep
	var observations []string

	planItems := a.llmPlanSteps(ctx, query, ts, memPrefix)

	if len(planItems) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		reactSteps = append(reactSteps, ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"})
		onEvent(NewStreamEvent("step", ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"}))
		answer := a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
		reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
		if ctx.Err() != nil {
			return "[已中断] 规划完成但生成被中断", reactSteps, nil
		}
		return answer, reactSteps, nil
	}

	var taskSteps []TaskStep
	for i, pi := range planItems {
		taskSteps = append(taskSteps, TaskStep{
			ID: i + 1, Name: pi.Reason, ToolName: pi.Tool,
			Params: pi.Params, Status: StepPending,
		})
	}

	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing", Steps: taskSteps,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	for i := range task.Steps {
		if ctx.Err() != nil {
			task.Phase = "interrupted"
			task.Status = "interrupted"
			task.InterruptedAt = i
			task.Steps[i].Status = StepInterrupted
			interruptMsg := a.buildInterruptMessage(task)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg})
			onEvent(NewStreamEvent("step", ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg}))
			a.saveSnapshot(task)
			return "[已中断] " + interruptMsg, reactSteps, task
		}

		ts2 := &task.Steps[i]
		task.CurrentStep = i
		ts2.Status = StepRunning

		thoughtStep := ReActStep{Type: StepThought, Content: ts2.Name}
		actionStep := ReActStep{Type: StepAction, Content: fmt.Sprintf("调用 %s", ts2.ToolName), Tool: ts2.ToolName, Params: ts2.Params}
		reactSteps = append(reactSteps, thoughtStep, actionStep)
		onEvent(NewStreamEvent("step", thoughtStep))
		onEvent(NewStreamEvent("step", actionStep))

		tool, ok := ts[ts2.ToolName]
		if !ok {
			ts2.Status = StepFailed
			ts2.Error = fmt.Sprintf("工具 %s 不在允许列表中", ts2.ToolName)
			obsStep := ReActStep{Type: StepObservation, Content: ts2.Error}
			reactSteps = append(reactSteps, obsStep)
			onEvent(NewStreamEvent("step", obsStep))
			a.saveSnapshot(task)
			continue
		}
		if a.executeStepWithRetryTool(ctx, ts2, tool) {
			ts2.Status = StepDone
			obsStep := ReActStep{Type: StepObservation, Content: ts2.Result}
			reactSteps = append(reactSteps, obsStep)
			onEvent(NewStreamEvent("step", obsStep))
			observations = append(observations, fmt.Sprintf("[%s] %s", ts2.ToolName, ts2.Result))
			if a.taskMem != nil {
				a.taskMem.Push(promptctx.StepObservation{StepID: ts2.ID, ToolName: ts2.ToolName, Result: ts2.Result, Success: true})
			}
			if a.toolTracker != nil {
				a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: ts2.ToolName, Success: true, Summary: ts2.Result})
			}
		} else {
			if ctx.Err() != nil {
				ts2.Status = StepInterrupted
				obsStep := ReActStep{Type: StepObservation, Content: "[已中断]"}
				reactSteps = append(reactSteps, obsStep)
				onEvent(NewStreamEvent("step", obsStep))
			} else {
				ts2.Status = StepFailed
				obsStep := ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", ts2.Error)}
				reactSteps = append(reactSteps, obsStep)
				onEvent(NewStreamEvent("step", obsStep))
				if a.taskMem != nil {
					a.taskMem.Push(promptctx.StepObservation{StepID: ts2.ID, ToolName: ts2.ToolName, Error: ts2.Error, Success: false})
				}
				if a.toolTracker != nil {
					a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: ts2.ToolName, Success: false, Summary: ts2.Error})
				}
			}
		}
		a.saveSnapshot(task)
	}

	if ctx.Err() != nil {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessage(task)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	task.Phase = "generating"
	answer := a.llmGenerateStream(ctx, query, observations, memPrefix, histMsgs, onEvent)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

// llmGenerateStream 流式版本的 Generator LLM，逐 token 回调
func (a *UnifiedAgent) llmGenerateStream(ctx context.Context, query string, observations []string, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) string {
	if len(observations) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		return a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
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
	return a.llm.ChatStreamContext(ctx, generatorBase,
		[]llm.Message{{Role: "user", Content: genPrompt}},
		func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
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

// saveSnapshot 对当前 TaskState 做深拷贝快照并持久化到 PG
//
// 接受显式 task 参数（不再读 a.task），以支持多请求并发：每个请求把
// 自己 ReAct 循环的 task 传进来，互不影响。a.snapshots 仍为全局历史，
// 通过 a.mu 串行化 append。
func (a *UnifiedAgent) saveSnapshot(task *TaskState) {
	if task == nil {
		return
	}
	var stateCopy TaskState
	data, _ := json.Marshal(task)
	if err := json.Unmarshal(data, &stateCopy); err != nil {
		// 不应该发生（自序列化），但避免吃掉错误
		log.Printf("⚠️  saveSnapshot 反序列化失败: %v", err)
		return
	}
	snap := Snapshot{State: stateCopy, Timestamp: time.Now().Format("15:04:05")}
	a.mu.Lock()
	a.snapshots = append(a.snapshots, snap)
	a.mu.Unlock()
	a.snapRepo.Save(task.TaskID, data)
}

// ─────────────────────────────── Stage 5：Memory（基础层，注入所有模式）────────
//
// 旧的 buildMemorySystemPrefix / buildMemorySystemPrefixWithCtx 已删除，
// 由 buildContextPrefix → promptctx.ContextAssembler 取代（Schema-driven 装配）。

// fillParamsFromPreference 用用户偏好自动补全工具调用参数中缺失的值
// 例如：偏好中有 "城市:北京"，则当工具参数含 city 但为空时自动填入
func (a *UnifiedAgent) fillParamsFromPreference(tc *tools.CallResult) {
	if tc == nil {
		return
	}
	prefs := a.pref.Snapshot() // 一次性快照，下方可无锁访问
	if len(prefs) == 0 {
		return
	}
	// 偏好 key → 工具参数名的映射
	prefToParam := map[string][]string{
		"城市": {"city", "location", "location_name"},
		"时区": {"timezone", "tz", "time_zone"},
		"姓名": {"name", "username", "user_name"},
		"语言": {"language", "lang"},
		"国家": {"country", "nation"},
	}
	for prefKey, paramNames := range prefToParam {
		prefVal, ok := prefs[prefKey]
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
