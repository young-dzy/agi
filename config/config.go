package config

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// 子结构按职责分组。所有子结构以 embedded 方式放进 APIConfig，
// 让 cfg.LLMAPIUrl / cfg.PGHost 等老访问路径通过 Go 字段提升继续工作。

// ServerConfig 服务端口与运行参数
type ServerConfig struct {
	ServerPort string
}

// LLMConfig 聊天模型 API
type LLMConfig struct {
	LLMAPIUrl   string
	LLMAPIKey   string
	LLMModel    string
	Temperature float64
}

func (c LLMConfig) IsRealLLM() bool { return c.LLMAPIKey != "" }

// EmbeddingConfig 向量化模型 API
type EmbeddingConfig struct {
	EmbeddingAPIUrl string
	EmbeddingAPIKey string
	EmbeddingModel  string
}

func (c EmbeddingConfig) IsRealEmbedding() bool { return c.EmbeddingAPIKey != "" }

// MilvusConfig 向量数据库连接
type MilvusConfig struct {
	MilvusHost string
	MilvusPort int
}

func (c MilvusConfig) MilvusAddr() string {
	return fmt.Sprintf("%s:%d", c.MilvusHost, c.MilvusPort)
}

// PostgresConfig 关系型数据库连接
type PostgresConfig struct {
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string
}

func (c PostgresConfig) PGDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.PGUser, c.PGPassword, c.PGHost, c.PGPort, c.PGDatabase)
}

// ESConfig Elasticsearch 连接
type ESConfig struct {
	ESAddresses []string
	ESUsername  string
	ESPassword  string
}

// KafkaConfig 事件总线
type KafkaConfig struct {
	KafkaBrokers []string
	KafkaTopic   string
}

// Neo4jConfig 知识图谱后端
type Neo4jConfig struct {
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	KGMaxHops     int     // 图遍历最大跳数
	KGWeight      float64 // 图检索在 RRF 中的权重
	KGEnabled     bool    // 是否启用知识图谱
}

// StorageConfig 聚合所有外部存储/连接配置
type StorageConfig struct {
	MilvusConfig
	PostgresConfig
	ESConfig
	KafkaConfig
	Neo4jConfig
}

// RAGConfig 检索增强生成
type RAGConfig struct {
	ChunkSize          int
	ChunkOverlap       int
	TopK               int
	RRFConstantK       int
	SemanticWeight     float64
	EnableHybridSearch bool
	RAGMilvusDim       int

	// Query Rewrite（history-aware + multi-query）
	RAGRewriteEnabled    bool
	RAGRewriteNumQueries int // 含原查询在内的目标改写条数

	// Rerank（LLM listwise 精排）
	RAGRerankEnabled    bool
	RAGRerankPreviewLen int // 给 reranker 看的每条候选最大字符数
}

// MemoryConfig 三层记忆 + 长期记忆合并策略
type MemoryConfig struct {
	ShortTermMaxTurns             int
	LongTermTopK                  int
	MemoryConsolidationSimilarity float64
	MemoryConsolidationDedup      float64
	MemoryConsolidationTTLDays    int
	MemoryConsolidationDecayRate  float64
	MemoryConsolidationMinImport  float64
	MemoryConsolidationTrigger    int
}

// HarnessConfig 任务执行框架
type HarnessConfig struct {
	MaxRetries    int
	RetryDelayMs  int
	StepTimeoutMs int
	MaxIterations int
}

// SearchConfig 搜索 API（Tavily 等）
type SearchConfig struct {
	SearchAPIKey string
	SearchAPIURL string
}

// SandboxConfig 沙箱执行参数（注意：与 internal/sandbox.SandboxConfig 不是同一类型）
type SandboxConfig struct {
	SandboxEnabled     bool
	SandboxBackend     string // "docker" | "local" | "mock"
	SandboxImage       string
	SandboxTimeoutMs   int
	SandboxMaxOutput   int
	SandboxMemoryMB    int
	SandboxCPUPercent  int
	SandboxMaxPIDs     int
	SandboxNetDisabled bool
	SandboxReadOnly    bool
}

// SecurityConfig 命令安全校验
type SecurityConfig struct {
	SecMaxCmdLength  int
	SecAllowlistMode bool
	SecAllowlist     []string
}

// GraphRuntimeConfig 图运行时配置（并行调度 + 竞速）
type GraphRuntimeConfig struct {
	GraphMaxParallel   int
	GraphRaceTimeoutMs int
	GraphEnableRacing  bool
}

// APIConfig 整合所有阶段的 API + 基础设施配置。
//
// 所有子结构以 embedded 方式放进来，访问路径 cfg.LLMAPIUrl / cfg.PGHost
// 等通过 Go 字段提升仍然有效——重构以分层为目标，不改动调用方。
type APIConfig struct {
	ServerConfig
	LLMConfig
	EmbeddingConfig
	StorageConfig
	RAGConfig
	MemoryConfig
	HarnessConfig
	SearchConfig
	SandboxConfig
	SecurityConfig
	GraphRuntimeConfig
}

// yamlFile 对应 config/config.yaml 的结构
type yamlFile struct {
	LLM struct {
		APIUrl      string  `yaml:"api_url"`
		APIKey      string  `yaml:"api_key"`
		Model       string  `yaml:"model"`
		Temperature float64 `yaml:"temperature"`
	} `yaml:"llm"`
	Embedding struct {
		APIUrl string `yaml:"api_url"`
		APIKey string `yaml:"api_key"`
		Model  string `yaml:"model"`
	} `yaml:"embedding"`
	Milvus struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"milvus"`
	Postgres struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	} `yaml:"postgres"`
	Elasticsearch struct {
		Addresses []string `yaml:"addresses"`
		Username  string   `yaml:"username"`
		Password  string   `yaml:"password"`
	} `yaml:"elasticsearch"`
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Topic   string   `yaml:"topic"`
	} `yaml:"kafka"`
	RAG struct {
		ChunkSize          int     `yaml:"chunk_size"`
		ChunkOverlap       int     `yaml:"chunk_overlap"`
		TopK               int     `yaml:"top_k"`
		RRFConstantK       int     `yaml:"rrf_constant_k"`
		SemanticWeight     float64 `yaml:"semantic_weight"`
		EnableHybridSearch bool    `yaml:"enable_hybrid_search"`
		RAGMilvusDim       int     `yaml:"rag_milvus_dim"`
		Rewrite            struct {
			Enabled    bool `yaml:"enabled"`
			NumQueries int  `yaml:"num_queries"`
		} `yaml:"rewrite"`
		Rerank struct {
			Enabled    bool `yaml:"enabled"`
			PreviewLen int  `yaml:"preview_len"`
		} `yaml:"rerank"`
	} `yaml:"rag"`
	Memory struct {
		ShortTermMaxTurns int `yaml:"short_term_max_turns"`
		LongTermTopK      int `yaml:"long_term_top_k"`
		Consolidation     struct {
			SimilarityThreshold float64 `yaml:"similarity_threshold"`
			DedupThreshold      float64 `yaml:"dedup_threshold"`
			TTLDays             int     `yaml:"ttl_days"`
			DecayRate           float64 `yaml:"decay_rate"`
			MinImportance       float64 `yaml:"min_importance"`
			TriggerInterval     int     `yaml:"trigger_interval"`
		} `yaml:"consolidation"`
	} `yaml:"memory"`
	Harness struct {
		MaxRetries    int `yaml:"max_retries"`
		RetryDelayMs  int `yaml:"retry_delay_ms"`
		StepTimeoutMs int `yaml:"step_timeout_ms"`
		MaxIterations int `yaml:"max_iterations"`
	} `yaml:"harness"`
	Server struct {
		Port string `yaml:"port"`
	} `yaml:"server"`
	Search struct {
		APIKey string `yaml:"api_key"`
		APIURL string `yaml:"api_url"`
	} `yaml:"search"`
	Neo4j struct {
		URI      string  `yaml:"uri"`
		User     string  `yaml:"user"`
		Password string  `yaml:"password"`
		MaxHops  int     `yaml:"max_hops"`
		Weight   float64 `yaml:"weight"`
		Enabled  bool    `yaml:"enabled"`
	} `yaml:"neo4j"`
	Sandbox struct {
		Enabled         bool   `yaml:"enabled"`
		Backend         string `yaml:"backend"`
		Image           string `yaml:"image"`
		TimeoutMs       int    `yaml:"timeout_ms"`
		MaxOutputBytes  int    `yaml:"max_output_bytes"`
		MemoryLimitMB   int    `yaml:"memory_limit_mb"`
		CPUPercent      int    `yaml:"cpu_percent"`
		MaxPIDs         int    `yaml:"max_pids"`
		NetworkDisabled bool   `yaml:"network_disabled"`
		ReadOnlyRootfs  bool   `yaml:"readonly_rootfs"`
	} `yaml:"sandbox"`
	Security struct {
		MaxCommandLength int      `yaml:"max_command_length"`
		AllowlistMode    bool     `yaml:"allowlist_mode"`
		Allowlist        []string `yaml:"allowlist"`
	} `yaml:"security"`
	GraphRuntime struct {
		MaxParallel   int  `yaml:"max_parallel"`
		RaceTimeoutMs int  `yaml:"race_timeout_ms"`
		EnableRacing  bool `yaml:"enable_racing"`
	} `yaml:"graph_runtime"`
}

// DefaultConfig 从 config/config.yaml 加载配置
func DefaultConfig() *APIConfig {
	data, err := os.ReadFile("config/config.yaml")
	if err != nil {
		log.Fatalf("读取 config/config.yaml 失败: %v", err)
	}

	// 严格解析：未知字段直接报错，避免 yaml 键名拼错（如 api-key vs api_key）
	// 时被静默忽略，运行时才发现 LLM/Embedding 走了 mock 模式。
	var y yamlFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&y); err != nil {
		log.Fatalf("解析 config/config.yaml 失败: %v", err)
	}

	c := &APIConfig{
		ServerConfig: ServerConfig{
			ServerPort: y.Server.Port,
		},
		LLMConfig: LLMConfig{
			LLMAPIUrl:   y.LLM.APIUrl,
			LLMAPIKey:   y.LLM.APIKey,
			LLMModel:    y.LLM.Model,
			Temperature: y.LLM.Temperature,
		},
		EmbeddingConfig: EmbeddingConfig{
			EmbeddingAPIUrl: y.Embedding.APIUrl,
			EmbeddingAPIKey: y.Embedding.APIKey,
			EmbeddingModel:  y.Embedding.Model,
		},
		StorageConfig: StorageConfig{
			MilvusConfig: MilvusConfig{
				MilvusHost: y.Milvus.Host,
				MilvusPort: y.Milvus.Port,
			},
			PostgresConfig: PostgresConfig{
				PGHost:     y.Postgres.Host,
				PGPort:     y.Postgres.Port,
				PGUser:     y.Postgres.User,
				PGPassword: y.Postgres.Password,
				PGDatabase: y.Postgres.Database,
			},
			ESConfig: ESConfig{
				ESAddresses: y.Elasticsearch.Addresses,
				ESUsername:  y.Elasticsearch.Username,
				ESPassword:  y.Elasticsearch.Password,
			},
			KafkaConfig: KafkaConfig{
				KafkaBrokers: y.Kafka.Brokers,
				KafkaTopic:   y.Kafka.Topic,
			},
			Neo4jConfig: Neo4jConfig{
				Neo4jURI:      y.Neo4j.URI,
				Neo4jUser:     y.Neo4j.User,
				Neo4jPassword: y.Neo4j.Password,
				KGMaxHops:     y.Neo4j.MaxHops,
				KGWeight:      y.Neo4j.Weight,
				KGEnabled:     y.Neo4j.Enabled,
			},
		},
		RAGConfig: RAGConfig{
			ChunkSize:            y.RAG.ChunkSize,
			ChunkOverlap:         y.RAG.ChunkOverlap,
			TopK:                 y.RAG.TopK,
			RRFConstantK:         y.RAG.RRFConstantK,
			SemanticWeight:       y.RAG.SemanticWeight,
			EnableHybridSearch:   y.RAG.EnableHybridSearch,
			RAGMilvusDim:         y.RAG.RAGMilvusDim,
			RAGRewriteEnabled:    y.RAG.Rewrite.Enabled,
			RAGRewriteNumQueries: y.RAG.Rewrite.NumQueries,
			RAGRerankEnabled:     y.RAG.Rerank.Enabled,
			RAGRerankPreviewLen:  y.RAG.Rerank.PreviewLen,
		},
		MemoryConfig: MemoryConfig{
			ShortTermMaxTurns:             y.Memory.ShortTermMaxTurns,
			LongTermTopK:                  y.Memory.LongTermTopK,
			MemoryConsolidationSimilarity: y.Memory.Consolidation.SimilarityThreshold,
			MemoryConsolidationDedup:      y.Memory.Consolidation.DedupThreshold,
			MemoryConsolidationTTLDays:    y.Memory.Consolidation.TTLDays,
			MemoryConsolidationDecayRate:  y.Memory.Consolidation.DecayRate,
			MemoryConsolidationMinImport:  y.Memory.Consolidation.MinImportance,
			MemoryConsolidationTrigger:    y.Memory.Consolidation.TriggerInterval,
		},
		HarnessConfig: HarnessConfig{
			MaxRetries:    y.Harness.MaxRetries,
			RetryDelayMs:  y.Harness.RetryDelayMs,
			StepTimeoutMs: y.Harness.StepTimeoutMs,
			MaxIterations: y.Harness.MaxIterations,
		},
		SearchConfig: SearchConfig{
			SearchAPIKey: y.Search.APIKey,
			SearchAPIURL: y.Search.APIURL,
		},
		SandboxConfig: SandboxConfig{
			SandboxEnabled:     y.Sandbox.Enabled,
			SandboxBackend:     y.Sandbox.Backend,
			SandboxImage:       y.Sandbox.Image,
			SandboxTimeoutMs:   y.Sandbox.TimeoutMs,
			SandboxMaxOutput:   y.Sandbox.MaxOutputBytes,
			SandboxMemoryMB:    y.Sandbox.MemoryLimitMB,
			SandboxCPUPercent:  y.Sandbox.CPUPercent,
			SandboxMaxPIDs:     y.Sandbox.MaxPIDs,
			SandboxNetDisabled: y.Sandbox.NetworkDisabled,
			SandboxReadOnly:    y.Sandbox.ReadOnlyRootfs,
		},
		SecurityConfig: SecurityConfig{
			SecMaxCmdLength:  y.Security.MaxCommandLength,
			SecAllowlistMode: y.Security.AllowlistMode,
			SecAllowlist:     y.Security.Allowlist,
		},
		GraphRuntimeConfig: GraphRuntimeConfig{
			GraphMaxParallel:   y.GraphRuntime.MaxParallel,
			GraphRaceTimeoutMs: y.GraphRuntime.RaceTimeoutMs,
			GraphEnableRacing:  y.GraphRuntime.EnableRacing,
		},
	}

	applyDefaults(c)
	return c
}

// applyDefaults 为零值字段填充合理默认值
func applyDefaults(c *APIConfig) {
	// RAG 混合检索默认值
	if c.RRFConstantK <= 0 {
		c.RRFConstantK = 60
	}
	if c.SemanticWeight <= 0 {
		c.SemanticWeight = 0.7
	}
	if c.RAGMilvusDim <= 0 {
		c.RAGMilvusDim = 1024
	}

	// Query Rewrite / Rerank 默认值（默认开启，复用主 LLM 不增加依赖）
	if c.RAGRewriteNumQueries <= 0 {
		c.RAGRewriteNumQueries = 3
	}
	if c.RAGRerankPreviewLen <= 0 {
		c.RAGRerankPreviewLen = 200
	}

	// 记忆合并默认值
	if c.MemoryConsolidationSimilarity <= 0 {
		c.MemoryConsolidationSimilarity = 0.80
	}
	if c.MemoryConsolidationDedup <= 0 {
		c.MemoryConsolidationDedup = 0.95
	}
	if c.MemoryConsolidationTTLDays <= 0 {
		c.MemoryConsolidationTTLDays = 30
	}
	if c.MemoryConsolidationDecayRate <= 0 {
		c.MemoryConsolidationDecayRate = 0.995
	}
	if c.MemoryConsolidationMinImport <= 0 {
		c.MemoryConsolidationMinImport = 0.3
	}
	if c.MemoryConsolidationTrigger <= 0 {
		c.MemoryConsolidationTrigger = 5
	}

	// Neo4j 默认值
	if c.KGMaxHops <= 0 {
		c.KGMaxHops = 2
	}
	if c.KGWeight <= 0 {
		c.KGWeight = 0.3
	}

	// 沙箱默认值
	if c.SandboxBackend == "" {
		c.SandboxBackend = "docker"
	}
	if c.SandboxImage == "" {
		c.SandboxImage = "ubuntu:22.04"
	}
	if c.SandboxTimeoutMs <= 0 {
		c.SandboxTimeoutMs = 30000
	}
	if c.SandboxMaxOutput <= 0 {
		c.SandboxMaxOutput = 65536
	}
	if c.SandboxMemoryMB <= 0 {
		c.SandboxMemoryMB = 256
	}
	if c.SandboxCPUPercent <= 0 {
		c.SandboxCPUPercent = 50
	}
	if c.SandboxMaxPIDs <= 0 {
		c.SandboxMaxPIDs = 64
	}

	// 安全校验默认值
	if c.SecMaxCmdLength <= 0 {
		c.SecMaxCmdLength = 500
	}

	// 图运行时默认值
	if c.GraphMaxParallel <= 0 {
		c.GraphMaxParallel = 2
	}
	if c.GraphRaceTimeoutMs <= 0 {
		c.GraphRaceTimeoutMs = 30000
	}
}
