// Final Stage — 全阶段整合 AI 助手
//
// 目录结构：
//
//	config/                配置（YAML 加载 + 默认值，分组子结构）
//	internal/
//	  promptctx/           Schema-driven prompt context 装配（原 runtime 包）
//	  llm/                 LLM 客户端（真实 API + Mock 降级）
//	  rag/                 RAG 引擎（文本切分 + 三路混合检索）
//	  tools/               工具定义与调用（time / weather / search / exec_command / MCP）
//	  memory/              三层记忆（短期 / 长期 / 用户偏好 / 图记忆）
//	  graph/                知识图谱业务（实体抽取 + 文档图）
//	  sandbox/             命令沙箱执行
//	  agent/               UnifiedAgent（ReAct + Harness + 智能路由）
//	  handler/             HTTP API 路由处理
//	  platform/            平台层连接封装（milvus / postgres / es / kafka / neo4j）
//	  repo/                数据访问仓储（chathistory / preference / snapshot / longterm / ragchunk / eventbus）
//	frontend/              单文件前端 HTML
package main

import (
	"agi-assistant/config"
	"agi-assistant/internal/application/chat"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/longterm"
	"agi-assistant/internal/infrastructure/persistence/preference"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"agi-assistant/internal/infrastructure/platform/es"
	"agi-assistant/internal/infrastructure/platform/kafka"
	"agi-assistant/internal/infrastructure/platform/milvus"
	"agi-assistant/internal/infrastructure/platform/postgres"
	"agi-assistant/internal/interfaces/http/handler"
	"fmt"
	"log"
	"net/http"
)

func main() {
	cfg := config.DefaultConfig()

	// ── 平台层连接（每路独立失败降级，不阻塞启动）──
	log.Println("🔧 正在连接基础设施...")
	milvusClient, milvusStatus := milvus.Connect(cfg.MilvusConfig)
	pgDB, pgStatus := postgres.Connect(cfg.PostgresConfig)
	if pgDB != nil {
		postgres.BootstrapSchema(pgDB)
	}
	esClient, esStatus := es.Connect(cfg.ESConfig)
	kafkaWriter, kafkaStatus := kafka.Connect(cfg.KafkaConfig)

	// ── 仓储层（接口实现）──
	deps := chat.Deps{
		ChatRepo:     chathistory.NewPGRepo(pgDB),
		PrefRepo:     preference.NewPGRepo(pgDB),
		SnapRepo:     snapshot.NewPGRepo(pgDB),
		LTMRepo:      longterm.NewPGRepo(pgDB),
		RAGChunkRepo: ragchunk.NewStore(pgDB, milvusClient, esClient),
		Events:       eventbus.NewKafkaPublisher(kafkaWriter, kafkaStatus == "connected"),
		InfraStatus: map[string]string{
			"milvus":        milvusStatus,
			"pg":            pgStatus,
			"elasticsearch": esStatus,
			"kafka":         kafkaStatus,
		},
	}

	// 关闭顺序：HTTP 服务在 main 退出时随进程结束；这里负责释放外部连接
	defer func() {
		if milvusClient != nil {
			milvusClient.Close()
		}
		if pgDB != nil {
			pgDB.Close()
		}
		if kafkaWriter != nil {
			kafkaWriter.Close()
		}
	}()

	// ── 初始化 UnifiedAgent ──
	a := chat.New(cfg, deps)

	// ── 注册 HTTP 路由 ──
	handler.New(a, cfg)

	// ── 挂载前端静态资源 ──
	http.Handle("/", http.FileServer(http.Dir("frontend")))

	printBanner(cfg, deps.InfraStatus)

	addr := ":" + cfg.ServerPort
	log.Fatal(http.ListenAndServe(addr, nil))
}

func printBanner(cfg *config.APIConfig, status map[string]string) {
	addr := ":" + cfg.ServerPort
	fmt.Println("========================================")
	fmt.Println("Final Stage · AGI 智能助手启动成功")
	fmt.Println("========================================")

	fmt.Printf("[INFO] Service       http://localhost%s\n", addr)
	fmt.Printf("[INFO] 通用模型           %s\n", cfg.LLMModel)
	fmt.Printf("[INFO] Embedding     %s\n", cfg.EmbeddingModel)

	fmt.Println("----------------------------------------")

	fmt.Printf("[INFO] Milvus        %s\n", status["milvus"])
	fmt.Printf("[INFO] PostgreSQL    %s:%d (%s)\n", cfg.PGHost, cfg.PGPort, status["pg"])
	fmt.Printf("[INFO] ElasticSearch %s\n", status["elasticsearch"])
	fmt.Printf("[INFO] Kafka         %s\n", status["kafka"])

	fmt.Println("----------------------------------------")
	fmt.Println("[READY] 道阻且长，行则将至。")
	fmt.Println("========================================")
}
