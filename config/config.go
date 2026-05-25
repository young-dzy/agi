package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// APIConfig 整合所有阶段的 API + 基础设施配置
type APIConfig struct {
	// ===== LLM 聊天模型 API =====
	LLMAPIUrl   string
	LLMAPIKey   string
	LLMModel    string
	Temperature float64

	// ===== Embedding 向量化模型 API =====
	EmbeddingAPIUrl string
	EmbeddingAPIKey string
	EmbeddingModel  string

	// ===== Milvus 向量数据库 =====
	MilvusHost string
	MilvusPort int

	// ===== PostgreSQL 关系型数据库 =====
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string

	// ===== Elasticsearch =====
	ESAddresses []string
	ESUsername  string
	ESPassword  string

	// ===== Kafka =====
	KafkaBrokers []string
	KafkaTopic   string

	// ===== RAG 配置 =====
	ChunkSize          int
	ChunkOverlap       int
	TopK               int
	RRFConstantK       int
	SemanticWeight     float64
	EnableHybridSearch bool
	RAGMilvusDim       int

	// ===== Memory 配置 =====
	ShortTermMaxTurns             int
	LongTermTopK                  int
	MemoryConsolidationSimilarity float64
	MemoryConsolidationDedup      float64
	MemoryConsolidationTTLDays    int
	MemoryConsolidationDecayRate  float64
	MemoryConsolidationMinImport  float64
	MemoryConsolidationTrigger    int

	// ===== Harness 配置 =====
	MaxRetries    int
	RetryDelayMs  int
	StepTimeoutMs int
	MaxIterations int

	// ===== 搜索 API（可选，支持 Tavily 等）=====
	SearchAPIKey string
	SearchAPIURL string

	// ===== Neo4j 图数据库（知识图谱 + 图记忆共享）=====
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	KGMaxHops     int     // 图遍历最大跳数
	KGWeight      float64 // 图检索在 RRF 中的权重
	KGEnabled     bool    // 是否启用知识图谱

	// ===== 沙箱执行 =====
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

	// ===== 命令安全校验 =====
	SecMaxCmdLength  int
	SecAllowlistMode bool
	SecAllowlist     []string

	// ===== 通用配置 =====
	ServerPort string
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
	} `yaml:"rag"`
	Memory struct {
		ShortTermMaxTurns int     `yaml:"short_term_max_turns"`
		LongTermTopK      int     `yaml:"long_term_top_k"`
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
		URI       string  `yaml:"uri"`
		User      string  `yaml:"user"`
		Password  string  `yaml:"password"`
		MaxHops   int     `yaml:"max_hops"`
		Weight    float64 `yaml:"weight"`
		Enabled   bool    `yaml:"enabled"`
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
}

// DefaultConfig 从 config/config.yaml 加载配置
func DefaultConfig() *APIConfig {
	data, err := os.ReadFile("config/config.yaml")
	if err != nil {
		log.Fatalf("读取 config/config.yaml 失败: %v", err)
	}

	var y yamlFile
	if err := yaml.Unmarshal(data, &y); err != nil {
		log.Fatalf("解析 config/config.yaml 失败: %v", err)
	}

	c := &APIConfig{
		LLMAPIUrl:   y.LLM.APIUrl,
		LLMAPIKey:   y.LLM.APIKey,
		LLMModel:    y.LLM.Model,
		Temperature: y.LLM.Temperature,

		EmbeddingAPIUrl: y.Embedding.APIUrl,
		EmbeddingAPIKey: y.Embedding.APIKey,
		EmbeddingModel:  y.Embedding.Model,

		MilvusHost: y.Milvus.Host,
		MilvusPort: y.Milvus.Port,

		PGHost:     y.Postgres.Host,
		PGPort:     y.Postgres.Port,
		PGUser:     y.Postgres.User,
		PGPassword: y.Postgres.Password,
		PGDatabase: y.Postgres.Database,

		ESAddresses: y.Elasticsearch.Addresses,
		ESUsername:  y.Elasticsearch.Username,
		ESPassword:  y.Elasticsearch.Password,

		KafkaBrokers: y.Kafka.Brokers,
		KafkaTopic:   y.Kafka.Topic,

		ChunkSize:          y.RAG.ChunkSize,
		ChunkOverlap:       y.RAG.ChunkOverlap,
		TopK:               y.RAG.TopK,
		RRFConstantK:       y.RAG.RRFConstantK,
		SemanticWeight:     y.RAG.SemanticWeight,
		EnableHybridSearch: y.RAG.EnableHybridSearch,
		RAGMilvusDim:       y.RAG.RAGMilvusDim,

		ShortTermMaxTurns: y.Memory.ShortTermMaxTurns,
		LongTermTopK:      y.Memory.LongTermTopK,

		MemoryConsolidationSimilarity: y.Memory.Consolidation.SimilarityThreshold,
		MemoryConsolidationDedup:      y.Memory.Consolidation.DedupThreshold,
		MemoryConsolidationTTLDays:    y.Memory.Consolidation.TTLDays,
		MemoryConsolidationDecayRate:  y.Memory.Consolidation.DecayRate,
		MemoryConsolidationMinImport:  y.Memory.Consolidation.MinImportance,
		MemoryConsolidationTrigger:    y.Memory.Consolidation.TriggerInterval,

		MaxRetries:    y.Harness.MaxRetries,
		RetryDelayMs:  y.Harness.RetryDelayMs,
		StepTimeoutMs: y.Harness.StepTimeoutMs,
		MaxIterations: y.Harness.MaxIterations,

		SearchAPIKey: y.Search.APIKey,
		SearchAPIURL: y.Search.APIURL,

		Neo4jURI:      y.Neo4j.URI,
		Neo4jUser:     y.Neo4j.User,
		Neo4jPassword: y.Neo4j.Password,
		KGMaxHops:     y.Neo4j.MaxHops,
		KGWeight:      y.Neo4j.Weight,
		KGEnabled:     y.Neo4j.Enabled,

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

		SecMaxCmdLength:  y.Security.MaxCommandLength,
		SecAllowlistMode: y.Security.AllowlistMode,
		SecAllowlist:     y.Security.Allowlist,

		ServerPort: y.Server.Port,
	}

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

	return c
}

func (c *APIConfig) IsRealLLM() bool      { return c.LLMAPIKey != "" }
func (c *APIConfig) IsRealEmbedding() bool { return c.EmbeddingAPIKey != "" }

// PGDSN 返回 PostgreSQL 连接串
func (c *APIConfig) PGDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.PGUser, c.PGPassword, c.PGHost, c.PGPort, c.PGDatabase)
}

// MilvusAddr 返回 Milvus 地址
func (c *APIConfig) MilvusAddr() string {
	return fmt.Sprintf("%s:%d", c.MilvusHost, c.MilvusPort)
}
