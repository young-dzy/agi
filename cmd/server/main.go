// Final Stage — 全阶段整合 AI 助手
//
// 目录结构：
//
//	config/                配置（YAML 加载 + 默认值，分组子结构）
//	internal/
//	  promptctx/           Schema-driven prompt context 装配
//	  llm/                 LLM 客户端（真实 API + Mock 降级）
//	  rag/                 RAG 引擎（文本切分 + 三路混合检索）
//	  tools/               工具定义与调用（time / weather / search / exec_command / MCP）
//	  memory/              三层记忆（短期 / 长期 / 用户偏好 / 图记忆）
//	  graph/                知识图谱业务（实体抽取 + 文档图）
//	  sandbox/             命令沙箱执行
//	  agent/               UnifiedAgent（ReAct + Harness + 智能路由）
//	  handler/             HTTP API（chi.Router + 中间件）
//	  middleware/          requestID / panicRecover / accessLog / cors
//	  platform/            平台层连接封装（milvus / postgres / es / kafka / neo4j）
//	  repo/                数据访问仓储
//	frontend/              单文件前端 HTML
//
// 启动 → 退出全流程：
//
//  1. 加载配置 + 连接基础设施（每路独立失败降级）
//  2. 构建 UnifiedAgent + HTTP Server（chi 路由 + 中间件 + 三大 timeout）
//  3. 监听 SIGINT / SIGTERM
//  4. 收到信号后 srv.Shutdown(30s) 等 in-flight 请求完成
//  5. 关闭外部连接（顺序：HTTP → DB → 队列）
package main

import (
	"agi-assistant/config"
	authapp "agi-assistant/internal/application/auth"
	"agi-assistant/internal/application/chat"
	authdomain "agi-assistant/internal/domain/auth"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/documentrepo"
	"agi-assistant/internal/infrastructure/persistence/longterm"
	"agi-assistant/internal/infrastructure/persistence/preference"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"agi-assistant/internal/infrastructure/persistence/userrepo"
	"agi-assistant/internal/infrastructure/platform/es"
	"agi-assistant/internal/infrastructure/platform/kafka"
	"agi-assistant/internal/infrastructure/platform/milvus"
	"agi-assistant/internal/infrastructure/platform/postgres"
	"agi-assistant/internal/interfaces/http/handler"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

// HTTP server 各档超时——所有 timeout 都是"不响应即断"，单位都给宽一点：
//   - ReadHeaderTimeout：客户端发送 header 的总时间。挡 Slowloris 攻击。
//   - ReadTimeout：含 body，POST 大对象（文档上传）也要在这内完成读取。
//   - WriteTimeout：从 header 读完到 response 写完。SSE 路由可能持续数分钟，
//     给 5 分钟兜底；超长流式建议改用 WebSocket 或在 handler 里 SetWriteDeadline(zero)。
//   - IdleTimeout：keep-alive 空闲连接最长保持时间，到点回收 goroutine。
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpWriteTimeout      = 5 * time.Minute
	httpIdleTimeout       = 120 * time.Second
	shutdownTimeout       = 30 * time.Second
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
		DocumentRepo: documentrepo.NewStore(pgDB, ".data/documents"),
		Events:       eventbus.NewKafkaPublisher(kafkaWriter, kafkaStatus == "connected"),
		InfraStatus: map[string]string{
			"milvus":        milvusStatus,
			"pg":            pgStatus,
			"elasticsearch": esStatus,
			"kafka":         kafkaStatus,
		},
	}

	// ── 初始化 UnifiedAgent + Auth Service + HTTP Server ──
	a := chat.New(cfg, deps)

	// JWT 签发器：secret 必须 ≥32 字节，缺失/过短直接启动失败（防 misconfig 上线）。
	ttl := time.Duration(cfg.JWTTTLHours) * time.Hour
	issuer, err := authdomain.NewTokenIssuer(cfg.JWTSecret, ttl, cfg.JWTIssuer)
	if err != nil {
		log.Fatalf("❌ JWT 签发器构造失败: %v（请通过环境变量 JWT_SECRET 注入 ≥32 字节随机字符串）", err)
	}
	authSvc := authapp.NewService(userrepo.NewPGRepo(pgDB), issuer)

	srv := newHTTPServer(cfg, a, authSvc, issuer)

	// ── 监听信号 + 启动 server ──
	// signal.NotifyContext 会在收到 SIGINT/SIGTERM 时 cancel ctx，
	// 主流程检测到 ctx.Done() 后开始优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 服务器在独立 goroutine 跑，主 goroutine 等信号
	serverErr := make(chan error, 1)
	go func() {
		printBanner(cfg, deps.InfraStatus)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	// 等以下三种事件之一触发关停：
	//   1) 系统信号（ctx.Done）→ 正常关停
	//   2) ListenAndServe 异常返回（端口占用等）→ 直接退出
	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("❌ HTTP 服务启动失败: %v", err)
		}
	case <-ctx.Done():
		log.Println("📡 收到关停信号，开始 graceful shutdown...")
	}

	// ── Graceful Shutdown ──
	// 先关 HTTP server：拒绝新连接，等 in-flight 请求在 shutdownTimeout 内完成。
	// 再关基础设施 client：保证关闭时已无 handler 在用它们，避免半途断连产生坏数据。
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("⚠️  HTTP shutdown 失败 (可能有请求超时): %v", err)
	} else {
		log.Println("✅ HTTP server 已关闭")
	}

	// 关闭外部连接
	if milvusClient != nil {
		_ = milvusClient.Close()
	}
	if pgDB != nil {
		_ = pgDB.Close()
	}
	if kafkaWriter != nil {
		_ = kafkaWriter.Close()
	}
	log.Println("👋 所有外部连接已释放，进程退出")
}

// newHTTPServer 装配 chi.Router + 三大 timeout。
// 单独抽出便于测试（直接 srv.Handler.ServeHTTP）和未来加 TLS。
func newHTTPServer(cfg *config.APIConfig, a *chat.UnifiedAgent, authSvc *authapp.Service, issuer *authdomain.TokenIssuer) *http.Server {
	h := handler.New(a, authSvc, issuer, cfg)
	return &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           h.Handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
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
