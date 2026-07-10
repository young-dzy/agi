# AGI-Assistant Code Wiki

> 多模态智能体系统 - 完整代码文档

---

## 目录

1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [目录结构](#3-目录结构)
4. [核心模块详解](#4-核心模块详解)
5. [关键类与函数](#5-关键类与函数)
6. [依赖关系图](#6-依赖关系图)
7. [配置系统](#7-配置系统)
8. [数据库设计](#8-数据库设计)
9. [API 接口](#9-api-接口)
10. [运行与部署](#10-运行与部署)

---

## 1. 项目概述

### 1.1 项目简介

AGI-assistant 是一个面向个人与企业的**多模态智能体系统**，融合了检索增强生成（RAG）、三层记忆、知识图谱、沙箱执行与可恢复执行流，支持多轮对话、知识检索、工具调用与复杂推理。

### 1.2 核心特性

| 特性 | 描述 |
|------|------|
| **多阶段智能体核心** | 支持纯对话、RAG 检索、单工具调用、多工具编排（ReAct）等多种智能体模式，自动路由 |
| **RAG 检索增强生成** | 融合 Milvus 语义向量、Elasticsearch 关键词、Neo4j 知识图谱，三路 RRF 融合排序，自动降级 |
| **三层记忆系统** | 短期记忆（滑动窗口）、长期记忆（Embedding/TF）、用户偏好（LLM+规则），支持去重、合并、衰减、过期淘汰 |
| **图增强记忆** | 长期记忆叠加 Neo4j 图层，支持 FOLLOWS、SIMILAR_TO、CAUSES、BELONGS_TO 等关系 |
| **工具链与可恢复执行** | 内置时间、天气、搜索、RAG 检索、命令执行等工具，支持 ReAct 规划-执行-生成流程 |
| **沙箱执行** | 支持 Docker / Local / Mock 三种沙箱后端，资源限制（CPU/内存/PID/网络），命令白名单安全校验 |
| **高可用基础设施** | PostgreSQL 持久化、Milvus/ES/Neo4j/Kafka 可选，自动优雅降级 |

### 1.3 技术栈

- **语言**: Go 1.25+
- **Web 框架**: go-chi/chi v5
- **数据库**: PostgreSQL (lib/pq)
- **向量数据库**: Milvus v2
- **搜索引擎**: Elasticsearch v8
- **图数据库**: Neo4j v5
- **消息队列**: Kafka (segmentio/kafka-go)
- **认证**: JWT (golang-jwt/jwt v5) + bcrypt
- **配置**: YAML (gopkg.in/yaml.v3)
- **PDF 解析**: ledongthuc/pdf

---

## 2. 整体架构

### 2.1 分层架构

系统采用**整洁架构（Clean Architecture）**思想，分为以下四层：

```
┌─────────────────────────────────────────────────┐
│           Interfaces (接口层)                    │
│  HTTP Handler / Middleware / SSE                │
├─────────────────────────────────────────────────┤
│           Application (应用层)                   │
│  UnifiedAgent / Chat Service / Auth Service     │
├─────────────────────────────────────────────────┤
│           Domain (领域层)                        │
│  RAG / Memory / Tool / Sandbox / Knowledge     │
│  Auth / PromptCtx / Document / Graph           │
├─────────────────────────────────────────────────┤
│           Infrastructure (基础设施层)            │
│  Platform (PG/Milvus/ES/Kafka/Neo4j)            │
│  Persistence (Repos) / LLM / EventBus / Tool   │
└─────────────────────────────────────────────────┘
```

### 2.2 核心流程

#### 智能体路由策略（按优先级）

1. **ReAct + Harness** — 复合查询（含 2+ 子需求，需多步推理）
2. **Tool Agent** — 单一工具触发（时间 / 天气 / 搜索）
3. **RAG** — 知识库已加载且无工具触发
4. **Chat** — 直接与 LLM 对话

记忆系统作为基础层注入所有模式（偏好 + 长期记忆 → System Prompt，STM → 对话历史）

#### 主执行流

```
用户请求
   ↓
prepare 阶段
   ├─ STM 写入用户消息
   ├─ 同步规则提取偏好
   ├─ 路由决策 (chat/tool/rag/react)
   ├─ Schema-driven 上下文装配
   └─ 构建 LLM 历史消息
   ↓
dispatch 阶段（按 mode 分发）
   ├─ chat:  纯对话 LLM 调用
   ├─ tool:  单工具调用
   ├─ rag:   RAG 检索 + 生成
   └─ react: ReAct 规划-执行-生成循环
   ↓
finalize 阶段
   ├─ STM 写入助手回复
   ├─ 异步抽取长期记忆
   ├─ 异步提取 LLM 偏好
   ├─ 触发记忆合并（满足条件时）
   ├─ 发布事件到 Kafka
   └─ 填充响应元数据
   ↓
返回响应
```

### 2.3 RAG 三路混合检索

```
用户查询
   ↓
Query Rewriter (可选: history-aware + multi-query)
   ↓
┌─────────────────────────────────────────┐
│         三路并行检索                     │
│  ┌──────────┐ ┌─────────┐ ┌──────────┐ │
│  │ Milvus   │ │   ES    │ │  Neo4j   │ │
│  │ 语义向量 │ │ BM25关键词││ 知识图谱  │ │
│  └──────────┘ └─────────┘ └──────────┘ │
└─────────────────────────────────────────┘
   ↓
RRF 融合排序 (Reciprocal Rank Fusion)
   ↓
Reranker (可选: LLM listwise 精排)
   ↓
Small-to-Big (子块 → 父块回填)
   ↓
LLM 合成答案
```

### 2.4 三层记忆系统

```
┌─────────────────────────────────────────────────────┐
│                 短期记忆 (STM)                       │
│  滑动窗口 · 会话级 · 不持久化 · 每用户分桶            │
└───────────────────────────┬─────────────────────────┘
                            ↓ 跨会话
┌─────────────────────────────────────────────────────┐
│              长期记忆 (LTM)                          │
│  Embedding/TF 双层 · 语义召回 · 去重合并衰减淘汰      │
│  多租户隔离 (UserID 字段)                           │
└───────────────────────────┬─────────────────────────┘
                            ↓ 图增强
┌─────────────────────────────────────────────────────┐
│            图记忆 (GraphMemory)                      │
│  Neo4j · FOLLOWS/SIMILAR_TO/CAUSES 关系             │
│  图扩展召回 · 高中心度节点保护                       │
└───────────────────────────┬─────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────┐
│            用户偏好 (Preference)                     │
│  LLM NER + 规则双重提取 · 持久化 · 跨会话恢复        │
└─────────────────────────────────────────────────────┘
```

---

## 3. 目录结构

```
agi-assistant/
├── cmd/
│   └── server/
│       └── main.go                    # 应用入口
├── config/
│   ├── config.go                      # 配置结构体与加载逻辑
│   ├── config.yaml                    # 默认配置文件
│   └── config.docker.yaml             # Docker 环境配置
├── docs/
│   ├── architecture/                  # 架构文档
│   └── code-tutor/                    # Code Tutor 技能文档
├── frontend/
│   └── index.html                     # 单文件前端
├── internal/
│   ├── application/
│   │   ├── auth/
│   │   │   ├── service.go             # 认证应用服务
│   │   │   └── service_test.go
│   │   └── chat/                      # 智能体核心
│   │       ├── core_agent.go          # UnifiedAgent 主结构体
│   │       ├── core_types.go          # 类型定义
│   │       ├── runtime_process.go     # 主执行流编排
│   │       ├── runtime_task.go        # 任务运行时状态
│   │       ├── runtime_graph.go       # 图运行时
│   │       ├── plan_graph.go          # 规划图
│   │       ├── replanner.go           # 重规划器
│   │       ├── mode_react.go          # ReAct 模式
│   │       ├── mode_tool.go           # 工具模式
│   │       ├── ctx_builder.go         # 上下文构建
│   │       ├── ctx_llm.go             # LLM 上下文
│   │       ├── ctx_prompt.go          # Prompt 上下文
│   │       ├── mem_stack.go           # 记忆栈聚合
│   │       ├── mem_restore.go         # 记忆恢复
│   │       ├── mem_writer.go          # 记忆写入
│   │       ├── subagents.go           # 子 Agent 注册
│   │       ├── tool_registry.go       # 工具注册表
│   │       ├── tool_documents.go      # 文档工具
│   │       ├── tool_sandbox.go        # 沙箱工具
│   │       ├── documents.go           # 文档管理
│   │       ├── infra_accessor.go      # 基础设施访问
│   │       ├── infra_repos.go         # 仓储聚合
│   │       ├── infra_router.go        # 路由逻辑
│   │       ├── infra_status.go        # 状态查询
│   │       ├── infra_cancel.go        # 取消机制
│   │       ├── conflict.go            # 矛盾治理
│   │       ├── poison.go              # 投毒检测
│   │       └── doc.md
│   ├── domain/
│   │   ├── auth/
│   │   │   ├── auth.go                # 用户、密码、JWT 领域模型
│   │   │   └── auth_test.go
│   │   ├── document/
│   │   │   ├── library.go             # 文档库
│   │   │   └── parser.go              # 文档解析器
│   │   ├── graph/
│   │   │   └── graph.go               # 图抽象
│   │   ├── knowledge/
│   │   │   ├── kgstore.go             # 知识图谱存储
│   │   │   ├── extractor.go           # 实体关系抽取
│   │   │   ├── types.go               # 类型定义
│   │   │   └── doc.md
│   │   ├── memory/
│   │   │   ├── graph/
│   │   │   │   └── graphmem.go        # 图增强记忆
│   │   │   ├── longterm/
│   │   │   │   ├── longterm.go        # 长期记忆
│   │   │   │   ├── conflict_test.go
│   │   │   │   ├── multiuser_test.go
│   │   │   │   └── quarantine_test.go
│   │   │   ├── preference/
│   │   │   │   └── preference.go      # 用户偏好
│   │   │   └── shortterm/
│   │   │       └── shortterm.go       # 短期记忆
│   │   ├── promptctx/
│   │   │   ├── assembler.go           # 上下文装配器
│   │   │   ├── context.go             # 上下文类型
│   │   │   ├── schema.go              # Schema 定义
│   │   │   ├── slot.go                # 槽位定义
│   │   │   ├── source.go              # Source 接口
│   │   │   ├── source_constraints.go  # 约束源
│   │   │   ├── source_planner.go      # 规划器源
│   │   │   ├── source_profile.go      # 档案源
│   │   │   ├── source_recall.go       # 召回源
│   │   │   ├── source_taskmem.go      # 任务记忆源
│   │   │   ├── source_tools.go        # 工具状态源
│   │   │   └── doc.md
│   │   ├── rag/
│   │   │   ├── rag.go                 # RAG 引擎
│   │   │   ├── hybrid.go              # 混合检索存储
│   │   │   ├── reranker.go            # 重排器
│   │   │   ├── rewriter.go            # 查询改写器
│   │   │   ├── splitter.go            # 文本分割器
│   │   │   ├── rag_test.go
│   │   │   └── doc.md
│   │   ├── sandbox/
│   │   │   ├── sandbox.go             # 沙箱领域模型
│   │   │   ├── types.go               # 执行请求/结果类型
│   │   │   ├── validator.go           # 安全校验器
│   │   │   └── doc.md
│   │   └── tool/
│   │       └── tool.go                # 工具抽象与选择
│   ├── infrastructure/
│   │   ├── eventbus/
│   │   │   └── eventbus.go            # 事件总线
│   │   ├── llm/
│   │   │   └── llm.go                 # LLM 客户端
│   │   ├── persistence/
│   │   │   ├── chathistory/           # 聊天历史仓储
│   │   │   ├── documentrepo/          # 文档仓储
│   │   │   ├── longterm/              # 长期记忆仓储
│   │   │   ├── preference/            # 偏好仓储
│   │   │   ├── ragchunk/              # RAG 块仓储
│   │   │   ├── snapshot/              # 快照仓储
│   │   │   └── userrepo/              # 用户仓储
│   │   ├── platform/
│   │   │   ├── es/                    # Elasticsearch 连接
│   │   │   ├── kafka/                 # Kafka 连接
│   │   │   ├── milvus/                # Milvus 连接
│   │   │   ├── neo4j/                 # Neo4j 连接
│   │   │   └── postgres/              # PostgreSQL 连接
│   │   ├── sandbox/
│   │   │   ├── docker.go              # Docker 沙箱后端
│   │   │   ├── local.go               # Local 沙箱后端
│   │   │   └── factory.go             # 沙箱工厂
│   │   └── tool/
│   │       ├── builtin.go             # 内置工具
│   │       ├── exec_command.go        # 命令执行工具
│   │       ├── mcp.go                 # MCP 工具
│   │       ├── mcp_test.go
│   │       └── tavily.go              # Tavily 搜索
│   ├── interfaces/
│   │   └── http/
│   │       ├── handler/
│   │       │   ├── handler.go         # HTTP 路由与处理器
│   │       │   ├── doc.go             # 文档相关处理器
│   │       │   ├── debug.go           # Debug/pprof
│   │       │   └── debug_test.go
│   │       └── middleware/
│   │           ├── middleware.go      # 通用中间件
│   │           ├── auth_middleware.go # 认证中间件
│   │           ├── middleware_test.go
│   │           └── auth_middleware_test.go
│   ├── pkg/
│   │   └── logger/
│   │       ├── logger.go              # 结构化日志
│   │       └── logger_test.go
│   └── usercontext/
│       └── usercontext.go             # 用户上下文
├── .dockerignore
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
└── README.md
```

---

## 4. 核心模块详解

### 4.1 应用层 - UnifiedAgent

**文件**: [core_agent.go](file:///workspace/internal/application/chat/core_agent.go)

`UnifiedAgent` 是系统的核心调度入口，整合全部 6 个阶段的能力。

#### 结构体组成

| 字段 | 类型 | 职责 |
|------|------|------|
| `cfg` | `*config.APIConfig` | 全局配置 |
| `llm` | `*llm.Client` | LLM 客户端 |
| `rag` | `*rag.Engine` | RAG 引擎 |
| `sandbox` | `*sandbox.Sandbox` | 沙箱执行器 |
| `kg` | `*knowledge.KGStore` | 知识图谱存储 |
| `mem` | `*memoryStack` | 三层记忆聚合容器 |
| `repos` | `*repoBundle` | 数据访问层聚合 |
| `pctx` | `*promptCtx` | Schema-driven Prompt 装配 |
| `tools` | `*toolRegistry` | 工具注册表（RWMutex 保护） |
| `subagents` | `*subAgentRegistry` | 子 Agent 注册表 |
| `runtime` | `*taskRuntime` | 任务运行时状态 |

#### 启动序列

```go
func New(cfg *config.APIConfig, deps Deps) *UnifiedAgent {
    // 1. 装配核心依赖
    // 2. wireRAGCallbacks: 注入 LLM/Embed/Rewriter/Reranker 回调
    // 3. bootstrapConcurrent: 4路并发IO启动
    //    - InitRAGInfra: 建 Milvus collection + ES 索引
    //    - restoreFromDB: 恢复偏好/长期记忆/聊天记录
    //    - restoreRAGFromDB: 恢复 RAG chunks
    //    - initSandbox: Docker daemon 探测 + 工具注册
    // 4. initKnowledgeGraph + buildPromptCtx: 串行执行
}
```

### 4.2 领域层 - RAG 引擎

**文件**: [rag.go](file:///workspace/internal/domain/rag/rag.go)

`Engine` 整合文本分割、混合检索与答案生成。

#### 核心流程

1. **Ingest（文档摄入）**:
   - Parent Splitter: 大块切分（默认 4 × ChunkSize）
   - Child Splitter: 小块切分（默认 ChunkSize）
   - 索引写入: Milvus + ES + PG
   - 异步建图: Neo4j 实体关系抽取

2. **Query（检索查询）**:
   - Query Rewrite: history-aware + multi-query 改写
   - Hybrid Search: 三路并行检索 + RRF 融合
   - Rerank: LLM listwise 精排（可选）
   - Small-to-Big: 子块 → 父块回填
   - LLM Generate: 基于检索上下文合成答案

#### 关键配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ChunkSize` | 200 | 子块大小（字符） |
| `ChunkOverlap` | - | 子块重叠 |
| `TopK` | - | 返回结果数 |
| `RRFConstantK` | 60 | RRF 融合常数 |
| `SemanticWeight` | 0.7 | 语义检索权重 |
| `RAGMilvusDim` | 1024 | 向量维度 |

### 4.3 领域层 - 长期记忆

**文件**: [longterm.go](file:///workspace/internal/domain/memory/longterm/longterm.go)

`LongTerm` 支持语义向量召回（embedding 优先）或 TF 词袋降级。

#### 记忆条目结构

```go
type Item struct {
    ID           int       // 自增ID
    Content      string    // 记忆内容
    Importance   float64   // 重要性 (0~1)
    Embedding    []float64 // 语义向量
    CreatedAt    time.Time // 创建时间
    LastAccessed time.Time // 最后访问时间
    UserID       string    // 多租户隔离
    Category     string    // 分类: identity/preference/fact/episodic/...
    Tags         []string  // 自由标签
    SlotHint     string    // 建议槽位
    Quarantined  bool      // 是否被隔离
    Superseded   bool      // 是否已被取代
    Supersedes   []int     // 取代了哪些旧条目
}
```

#### 记忆合并三阶段

1. **Phase 1: 重要性衰减**
   - 公式: `newImp = oldImp × DecayRate^days`
   - 默认每日衰减系数 0.995（30 天 ≈ 0.86）

2. **Phase 2: 去重 + 合并**
   - 去重阈值 (DedupThreshold): 0.95 → 保留重要性更高的
   - 合并阈值 (SimilarityThreshold): 0.80 → 内容拼接，向量加权平均

3. **Phase 3: 过期淘汰**
   - TTL: 30 天
   - 条件: 超过 TTL 且重要性 < MinImportance (0.3)
   - 图记忆保护: 高中心度节点通过 ProtectFn 钩子豁免

### 4.4 领域层 - Schema-driven Prompt 装配

**文件**: [assembler.go](file:///workspace/internal/domain/promptctx/assembler.go)

`ContextAssembler` 根据 Mode 选择 Schema，并发调用各 Source 填充槽位。

#### 槽位类型 (SlotKind)

| 槽位 | 优先级 | 说明 | Source |
|------|--------|------|--------|
| `SlotConstraints` | 最高 | 约束/政策 | ConstraintsSource |
| `SlotProfile` | 高 | 用户档案 | ProfileSource |
| `SlotPlanner` | 中高 | 规划状态 | PlannerSource |
| `SlotToolState` | 中 | 工具状态 | ToolStateSource |
| `SlotTaskMem` | 中低 | 任务记忆 | TaskMemSource |
| `SlotRecall` | 最低 | 历史召回 | RecallSource |

#### 工作原理

1. 注册所有 ContextSource 到 SourceRegistry
2. 每个 Source 声明支持的 SlotKind
3. Assemble 时按 Schema 并发填充各槽位
4. 全局 Token 预算裁剪：从低优先级槽位开始剔除

### 4.5 领域层 - 沙箱执行

**文件**: [sandbox.go](file:///workspace/internal/domain/sandbox/sandbox.go)

`Sandbox` 封装 Validator + Executor + 审计回调。

#### 执行流程

```
命令输入
   ↓
Validator 安全校验
   ├─ Block 级 → 直接拒绝，不进入执行
   └─ Warn 级 → 需要 confirm=true 才执行
   ↓
Executor 执行
   ├─ Docker 后端: 资源隔离 + 安全限制
   ├─ Local 后端: 直接执行（开发用）
   └─ Mock 后端: 测试用
   ↓
异步审计回调 (Kafka)
```

#### 安全特性

- 命令长度限制（默认 500 字符）
- 白名单模式（可配置允许的命令列表）
- Docker 资源限制: CPU/内存/PID/网络/只读文件系统

### 4.6 领域层 - 工具系统

**文件**: [tool.go](file:///workspace/internal/domain/tool/tool.go)

#### Tool 结构

```go
type Tool struct {
    Name        string
    Description string
    Parameters  []Param
    IsMCP       bool
    Execute          func(params map[string]interface{}) (string, error)
    ExecuteCtx       func(ctx context.Context, params map[string]interface{}) (string, error)
    ExecuteStructured func(ctx context.Context, params map[string]interface{}) ToolResult
}
```

#### 内置工具

| 工具名 | 说明 |
|--------|------|
| `get_time` | 获取当前时间（支持时区） |
| `get_weather` | 获取天气信息 |
| `search_web` | 互联网搜索（Tavily + LLM 降级） |
| `rag_search` | 个人知识库检索 |
| `exec_command` | 命令执行（沙箱内） |

### 4.7 领域层 - 认证

**文件**: [auth.go](file:///workspace/internal/domain/auth/auth.go)

#### 核心组件

| 组件 | 说明 |
|------|------|
| `User` | 用户实体（username + bcrypt 密码哈希） |
| `HashPassword` | bcrypt 哈希（cost=12, OWASP 推荐） |
| `VerifyPassword` | 密码验证 |
| `TokenIssuer` | JWT 签发/验证（HS256） |
| `Claims` | JWT 负载（subject=user_id, username） |

#### 安全设计

- 密码长度: 8~64 字节
- 用户名长度: 3~32 字符（UTF-8 计数）
- JWT secret: 至少 32 字节随机
- Token TTL: 默认 7 天
- 统一错误: NotFound 与 PasswordMismatch 都返回相同错误（防账号枚举）

### 4.8 接口层 - HTTP Handler

**文件**: [handler.go](file:///workspace/internal/interfaces/http/handler/handler.go)

#### 中间件顺序（外→内）

```
RequestID → PanicRecover → AccessLog → CORS → [RequireAuth]
```

#### 中间件详情

| 中间件 | 职责 |
|--------|------|
| `RequestID` | 生成/透传 X-Request-Id，写入 context |
| `PanicRecover` | 捕获 handler panic，返回 500 + request_id |
| `AccessLog` | 结构化访问日志（method/path/status/bytes/dur_ms/...） |
| `CORS` | 跨域支持（OPTIONS 预检） |
| `RequireAuth` | JWT 验证，注入 user_id/username 到 context |

---

## 5. 关键类与函数

### 5.1 核心入口函数

#### `main()` — 应用启动

**位置**: [main.go](file:///workspace/cmd/server/main.go#L74-L177)

启动流程:
1. 加载配置 (`config.DefaultConfig()`)
2. 初始化日志
3. 连接基础设施（每路独立失败降级）
4. 构建仓储层
5. 初始化 UnifiedAgent + Auth Service
6. 启动 HTTP Server
7. 监听 SIGINT/SIGTERM
8. 优雅关停（30s 超时）

#### `UnifiedAgent.New()` — 智能体构造

**位置**: [core_agent.go](file:///workspace/internal/application/chat/core_agent.go#L92-L134)

依赖注入容器 `Deps`:
- `ChatRepo`: 聊天历史仓储
- `PrefRepo`: 偏好仓储
- `SnapRepo`: 快照仓储
- `LTMRepo`: 长期记忆仓储
- `RAGChunkRepo`: RAG 块仓储
- `DocumentRepo`: 文档仓储
- `Events`: 事件总线发布者
- `InfraStatus`: 基础设施健康状态

### 5.2 RAG 关键函数

#### `Engine.Ingest()` — 文档摄入

**位置**: [rag.go](file:///workspace/internal/domain/rag/rag.go#L161-L205)

输入: 文档文本  
输出: `IngestResult` (chunk_count, parent_count, indexed_count, doc_hash)

#### `Engine.QueryWithHistory()` — 带历史检索

**位置**: [rag.go](file:///workspace/internal/domain/rag/rag.go#L251-L302)

输入: question, history  
输出: (answer, search_results)

### 5.3 记忆关键函数

#### `LongTerm.StoreClassified()` — 写入记忆

**位置**: [longterm.go](file:///workspace/internal/domain/memory/longterm/longterm.go#L224-L280)

- 自动去重（cosine ≥ DedupThreshold）
- 多租户隔离（UserID 强制）
- 分类/标签/槽位提示

#### `LongTerm.RecallByFilter()` — 条件召回

**位置**: [longterm.go](file:///workspace/internal/domain/memory/longterm/longterm.go#L349-L442)

支持过滤条件:
- UserID（强制，防跨用户泄漏）
- Categories
- RequireTags
- MinScore
- TopK
- MaxAgeHours
- IncludeQuarantined
- IncludeSuperseded

#### `LongTerm.Consolidate()` — 记忆合并

**位置**: [longterm.go](file:///workspace/internal/domain/memory/longterm/longterm.go#L661-L787)

三阶段: 衰减 → 去重合并 → 过期淘汰

### 5.4 工具注册与调用

#### `UnifiedAgent.RegisterTool()` — 注册工具

通过 `toolRegistry` 线程安全地注册工具，支持 MCP 热插拔。

#### `tool.Decide()` — 工具选择

**位置**: [tool.go](file:///workspace/internal/domain/tool/tool.go#L88-L137)

基于关键词规则的工具选择:
- "几点/时间" → get_time
- "天气" → get_weather
- "查/搜索/是什么" → search_web
- 兜底: 集合中第一个工具

### 5.5 Prompt 装配

#### `ContextAssembler.Assemble()` — 上下文装配

**位置**: [assembler.go](file:///workspace/internal/domain/promptctx/assembler.go#L65-L97)

- 并发填充各槽位
- 单槽位 budget 裁剪
- 全局 budget 裁剪（按优先级从低到高剔除）

---

## 6. 依赖关系图

### 6.1 包依赖方向

```
cmd/server (main)
    ↓
config
    ↓
interfaces/http/handler
    ↓
application/chat (UnifiedAgent)
application/auth
    ↓
domain/* (rag, memory, tool, sandbox, auth, ...)
    ↓
infrastructure/* (platform, persistence, llm, tool, ...)
    ↓
pkg/logger
usercontext
```

### 6.2 核心依赖注入链

```
main.go
  ├─ config.APIConfig
  ├─ platform/postgres (sql.DB)
  ├─ platform/milvus (milvus client)
  ├─ platform/es (es client)
  ├─ platform/kafka (kafka writer)
  ├─ persistence/* (Repo implementations)
  ├─ eventbus.Publisher
  └─ chat.Deps (all repos + events + infraStatus)
       ↓
  chat.UnifiedAgent
       ├─ llm.Client
       ├─ rag.Engine
       ├─ sandbox.Sandbox
       ├─ knowledge.KGStore
       ├─ memoryStack
       ├─ toolRegistry
       ├─ subAgentRegistry
       ├─ taskRuntime
       └─ promptCtx
```

### 6.3 外部依赖列表

**主要依赖** (来自 [go.mod](file:///workspace/go.mod)):

| 依赖 | 版本 | 用途 |
|------|------|------|
| `github.com/elastic/go-elasticsearch/v8` | v8.19.5 | Elasticsearch 客户端 |
| `github.com/ledongthuc/pdf` | - | PDF 文本解析 |
| `github.com/lib/pq` | v1.12.3 | PostgreSQL 驱动 |
| `github.com/milvus-io/milvus-sdk-go/v2` | v2.4.2 | Milvus 向量数据库 |
| `github.com/neo4j/neo4j-go-driver/v5` | v5.18.0 | Neo4j 图数据库 |
| `github.com/segmentio/kafka-go` | v0.4.51 | Kafka 客户端 |
| `gopkg.in/yaml.v3` | v3.0.1 | YAML 配置解析 |
| `github.com/go-chi/chi/v5` | v5.3.0 | HTTP 路由 |
| `github.com/golang-jwt/jwt/v5` | v5.3.1 | JWT 认证 |
| `golang.org/x/crypto` | v0.53.0 | bcrypt 密码哈希 |

---

## 7. 配置系统

### 7.1 配置加载流程

**文件**: [config.go](file:///workspace/config/config.go)

```
.env 文件 → 环境变量扩展 → config/config.yaml → YAML 解析 → 默认值填充 → APIConfig
```

### 7.2 配置分组

| 配置组 | 结构体 | 说明 |
|--------|--------|------|
| 服务配置 | `ServerConfig` | 端口 |
| LLM 配置 | `LLMConfig` | API URL/Key/模型/温度 |
| Embedding 配置 | `EmbeddingConfig` | 向量化模型 |
| Milvus 配置 | `MilvusConfig` | 向量数据库连接 |
| PostgreSQL 配置 | `PostgresConfig` | 关系型数据库连接 |
| ES 配置 | `ESConfig` | Elasticsearch 连接 |
| Kafka 配置 | `KafkaConfig` | 事件总线连接 |
| Neo4j 配置 | `Neo4jConfig` | 知识图谱连接 |
| RAG 配置 | `RAGConfig` | 检索增强参数 |
| 记忆配置 | `MemoryConfig` | 三层记忆参数 |
| Harness 配置 | `HarnessConfig` | 任务执行框架参数 |
| 搜索配置 | `SearchConfig` | Tavily 搜索 API |
| 沙箱配置 | `SandboxConfig` | 沙箱执行参数 |
| 安全配置 | `SecurityConfig` | 命令安全校验 |
| 图运行时配置 | `GraphRuntimeConfig` | 图调度与重规划 |
| 认证配置 | `AuthConfig` | JWT 参数 |
| 可观测配置 | `ObservabilityConfig` | pprof 等 |

### 7.3 关键默认值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RRFConstantK` | 60 | RRF 融合常数 |
| `SemanticWeight` | 0.7 | 语义权重 |
| `RAGMilvusDim` | 1024 | 向量维度 |
| `ShortTermMaxTurns` | - | 短期记忆轮数 |
| `MemoryConsolidationDedup` | 0.95 | 去重阈值 |
| `MemoryConsolidationSimilarity` | 0.80 | 合并阈值 |
| `MemoryConsolidationTTLDays` | 30 | 过期天数 |
| `MemoryConsolidationDecayRate` | 0.995 | 日衰减系数 |
| `KGMaxHops` | 2 | 图遍历最大跳数 |
| `KGWeight` | 0.3 | 图检索权重 |
| `SandboxBackend` | "docker" | 默认沙箱后端 |
| `SandboxTimeoutMs` | 30000 | 沙箱超时 |
| `JWTSecret` | - | **必须配置，无默认值** |
| `JWTTTLHours` | 0 → 7天 | Token 有效期 |

---

## 8. 数据库设计

### 8.1 PostgreSQL 表结构

**文件**: [postgres.go](file:///workspace/internal/infrastructure/platform/postgres/postgres.go#L41-L150)

#### `users` — 用户表

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | SERIAL PK | 用户 ID |
| `username` | TEXT UNIQUE | 用户名 |
| `password_hash` | TEXT NOT NULL | bcrypt 密码哈希 |
| `created_at` | TIMESTAMP | 创建时间 |
| `last_login_at` | TIMESTAMP | 最后登录时间 |

#### `user_preferences` — 用户偏好表

| 字段 | 类型 | 说明 |
|------|------|------|
| `user_id` | TEXT | 用户 ID |
| `key` | TEXT | 偏好键 |
| `value` | TEXT | 偏好值 |
| `updated_at` | TIMESTAMP | 更新时间 |
| **PK** | (user_id, key) | 复合主键 |

#### `task_snapshots` — 任务快照表

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | TEXT PK | 任务 ID |
| `state` | JSONB NOT NULL | 快照状态 |
| `created_at` | TIMESTAMP | 创建时间 |

#### `chat_history` — 聊天历史表

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | SERIAL PK | 消息 ID |
| `role` | TEXT NOT NULL | 角色 (user/assistant) |
| `content` | TEXT NOT NULL | 消息内容 |
| `created_at` | TIMESTAMP | 创建时间 |

#### `long_term_memory` — 长期记忆表

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | SERIAL PK | 记忆 ID |
| `content` | TEXT NOT NULL | 记忆内容 |
| `importance` | FLOAT | 重要性 |
| `embedding` | JSONB | 语义向量 |
| `created_at` | TIMESTAMP | 创建时间 |
| `last_accessed` | TIMESTAMP | 最后访问时间 |
| `user_id` | TEXT | 多租户 |
| `category` | TEXT | 分类 |
| `tags` | TEXT[] | 标签数组 (GIN 索引) |
| `slot_hint` | TEXT | 槽位提示 |
| `quarantined` | BOOLEAN | 是否隔离 |
| `quarantine_reason` | TEXT | 隔离原因 |
| `superseded` | BOOLEAN | 是否被取代 |
| `superseded_at` | TIMESTAMP | 取代时间 |
| `supersedes` | INT[] | 取代的 ID 列表 |

#### `rag_chunks` — RAG 块表

存储 RAG 文档块内容（用于 small-to-big 回填和持久化）。

---

## 9. API 接口

### 9.1 认证接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/auth/register` | 用户注册 | 否 |
| POST | `/api/auth/login` | 用户登录 | 否 |
| GET | `/api/auth/me` | 当前用户信息 | 是 |

### 9.2 对话接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/chat` | 同步对话 | 是 |
| POST | `/api/chat/stream` | SSE 流式对话 | 是 |
| POST | `/api/chat/cancel` | 取消当前任务 | 是 |

**请求体**:
```json
{
  "message": "用户问题",
  "use_rag": false,
  "selected_tools": [],
  "explicit": false
}
```

### 9.3 文档接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/upload` | 上传文档到 RAG | 是 |
| POST | `/api/docs/delete` | 删除文档 chunks | 是 |
| GET | `/api/documents/` | 列出文档库 | 是 |
| POST | `/api/documents/` | 创建文档 | 是 |
| GET | `/api/documents/{id}` | 获取文档 | 是 |
| POST | `/api/documents/{id}/ingest` | 入库 RAG | 是 |

### 9.4 记忆接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| GET | `/api/memory/` | 三层记忆状态 | 是 |
| GET | `/api/memory/quarantined` | 隔离记忆列表 | 是 |
| POST | `/api/memory/quarantine` | 隔离记忆 | 是 |
| POST | `/api/memory/unquarantine` | 解除隔离 | 是 |
| GET | `/api/memory/superseded` | 被取代记忆 | 是 |

### 9.5 工具接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| GET | `/api/tools` | 工具列表 | 是 |
| POST | `/api/tools/mcp` | 注册 MCP 工具 | 是 |

### 9.6 系统接口

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| GET | `/api/snapshots` | 任务快照列表 | 是 |
| GET | `/api/status` | 系统状态 | 是 |
| GET | `/healthz` | 健康检查 | 否 |
| GET | `/readyz` | 就绪检查 | 否 |

---

## 10. 运行与部署

### 10.1 本地运行

```bash
# 1. 安装依赖
go mod tidy

# 2. 启动基础设施（需要 Docker Desktop）
docker compose up -d

# 3. 配置 API Key
# 编辑 config/config.yaml，填入:
#   - llm.api_key: 火山引擎 Ark 对话模型 API Key
#   - embedding.api_key: 火山引擎 Embedding 模型 API Key
#   - search.api_key: Tavily 搜索 API Key（可选）
#   - auth.jwt_secret: ≥32 字节随机字符串

# 4. 启动应用
go run cmd/server/main.go

# 5. 访问 http://localhost:8090
```

### 10.2 Docker 部署

```bash
# 编译
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o final-agent .

# 启动全部服务
docker compose up -d --build
```

### 10.3 环境变量

配置文件支持 `${ENV_VAR}` 形式的环境变量替换，常用变量:

| 变量 | 说明 |
|------|------|
| `JWT_SECRET` | JWT 签名密钥（≥32 字节） |
| `PPROF_ADMIN_TOKEN` | pprof 访问令牌 |

### 10.4 优雅降级

所有基础设施（Milvus/PG/ES/Kafka/Neo4j）均为可选，连接失败自动降级:

| 组件 | 降级策略 |
|------|----------|
| PostgreSQL | 内存模式（数据不持久化） |
| Milvus | 跳过语义检索，仅 ES + Neo4j |
| Elasticsearch | 跳过关键词检索，仅 Milvus + Neo4j |
| Neo4j | 跳过图检索/图记忆，仅 Milvus + ES |
| Kafka | 事件发布变为 no-op |
| LLM API | Mock 模式（返回模板回复） |
| Embedding API | TF 词袋降级（长期记忆 + RAG） |

### 10.5 观测与诊断

- **pprof**: 配置 `observability.pprof.enabled=true` 后，访问 `/debug/pprof/`（需 `X-Admin-Token` header）
- **访问日志**: 结构化日志（method/path/status/bytes/dur_ms/request_id/user_id）
- **健康检查**: `/healthz` 和 `/readyz`

---

## 附录：核心设计原则

### A.1 分层与依赖方向

- **依赖倒置**: domain 层定义接口，infrastructure 层实现
- **依赖注入**: main.go 组装所有依赖，application 层接收 Deps 结构体
- **无循环依赖**: 严格的上层依赖下层

### A.2 并发安全

- 记忆栈: RWMutex 保护用户分桶
- 工具注册表: RWMutex 保护工具 map
- 任务运行时: Mutex 保护共享状态
- 长期记忆: RWMutex 保护 Items 列表

### A.3 多租户隔离

- STM/Preference: 按 userID 分桶（map + 懒加载）
- LTM: UserID 字段 + RecallByFilter 强制过滤
- 设计原则: "忘传 UserID 就什么都看不到"，而非"忘传就看到全部"

### A.4 安全设计

- JWT secret ≥ 32 字节，缺失启动失败
- pprof 启用必须配 admin token，否则启动失败
- 密码 bcrypt cost=12
- 沙箱命令白名单 + 资源限制
- 记忆隔离（Quarantine）: 软删除，审计可追溯
- 矛盾治理（Supersede）: 旧条目保留，新条目记录替代关系

---

*文档生成时间: 2026-07-10*
