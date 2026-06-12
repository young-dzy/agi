// Package agent 实现 UnifiedAgent：整合全部 6 个阶段能力的核心调度器。
//
// 路由策略（按优先级）：
//  1. ReAct + Harness — 复合查询（含 2+ 子需求，需多步推理）
//  2. Tool Agent      — 单一工具触发（时间 / 天气 / 搜索）
//  3. RAG             — 知识库已加载且无工具触发
//  4. Chat            — 直接与 LLM 对话
//
// 记忆系统作为基础层注入所有模式（偏好 + 长期记忆 → System Prompt，STM → 对话历史）
package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/knowledge"
	graphmem "agi-assistant/internal/domain/memory/graph"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/memory/shortterm"
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	prefrepo "agi-assistant/internal/infrastructure/persistence/preference"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	toolimpl "agi-assistant/internal/infrastructure/tool"
	"context"
	"fmt"
	"sync"
)

// ─────────────────────────────── Unified Agent ───────────────────────────

// UnifiedAgent 整合全部能力，是系统的核心调度入口
type UnifiedAgent struct {
	cfg      *config.APIConfig
	llm      *llm.Client
	rag      *rag.Engine
	stm      *shortterm.ShortTerm
	ltm      *longterm.LongTerm    // 保留直接引用，供 handler 暴露
	graphMem *graphmem.GraphMemory // 图增强记忆层（包装 ltm）
	pref     *preference.Preference
	sandbox  *sandbox.Sandbox
	kg       *knowledge.KGStore // 知识图谱（RAG + 记忆图共享）

	// 数据访问层（每个 domain 用各自的 repo 接口）
	chatRepo     chathistory.Repo
	prefRepo     prefrepo.Repo
	snapRepo     snapshot.Repo
	ltmRepo      ltmrepo.Repo
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
	tools   map[string]tool.Tool

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
	PrefRepo     prefrepo.Repo
	SnapRepo     snapshot.Repo
	LTMRepo      ltmrepo.Repo
	RAGChunkRepo ragchunk.Repo
	Events       eventbus.Publisher
	// InfraStatus 平台层连接健康快照
	InfraStatus map[string]string
}

// New 创建并初始化 UnifiedAgent
func New(cfg *config.APIConfig, deps Deps) *UnifiedAgent {
	llmClient := llm.New(cfg)
	ragEngine := rag.NewEngine(cfg, deps.RAGChunkRepo, deps.Events)
	ltm := longterm.New()
	a := &UnifiedAgent{
		cfg:          cfg,
		llm:          llmClient,
		rag:          ragEngine,
		tools:        toolimpl.DefaultTools(),
		stm:          shortterm.New(cfg.ShortTermMaxTurns),
		ltm:          ltm,
		pref:         preference.New(),
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
	a.ltm.SetConsolidationConfig(&longterm.ConsolidationConfig{
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
	// 注入 Query Rewriter（history-aware + multi-query）
	// 用独立的 generateFn（不带记忆前缀）避免改写 prompt 被偏好污染
	if cfg.RAGRewriteEnabled && cfg.RAGRewriteNumQueries > 1 {
		rewriteLLM := func(systemPrompt, userMsg string) string {
			return a.llm.Chat(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
		}
		a.rag.SetRewriter(rag.NewLLMRewriter(rewriteLLM, cfg.RAGRewriteNumQueries))
	}
	// 注入 Reranker（LLM listwise 精排）
	if cfg.RAGRerankEnabled {
		rerankLLM := func(systemPrompt, userMsg string) string {
			return a.llm.Chat(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
		}
		a.rag.SetReranker(rag.NewLLMReranker(rerankLLM, cfg.RAGRerankPreviewLen))
	}
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
	a.RegisterTool(tool.Tool{
		Name:        "rag_search",
		Description: "从私人黑洞（个人知识库）中检索相关文档内容",
		Parameters: []tool.Param{
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
	a.RegisterTool(tool.Tool{
		Name:        "search_web",
		Description: "搜索互联网获取最新信息",
		Parameters: []tool.Param{
			{Name: "query", Type: "string", Description: "搜索关键词", Required: true},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			q, _ := params["query"].(string)
			if q == "" {
				return "", fmt.Errorf("搜索关键词不能为空")
			}
			// 优先尝试 Tavily 真实搜索
			if a.cfg.SearchAPIKey != "" {
				if result, err := toolimpl.TavilySearch(q, a.cfg.SearchAPIKey, a.cfg.SearchAPIURL); err == nil {
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
