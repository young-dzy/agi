
# AGI-assistant：多模态智能体系统

AGI-assistant 是一个面向个人与企业的多模态智能体系统，融合了检索增强生成（RAG）、三层记忆、知识图谱、沙箱执行与可恢复执行流，支持多轮对话、知识检索、工具调用与复杂推理。

---
## 项目特性

- **多阶段智能体核心**：支持纯对话、RAG 检索、单工具调用、多工具编排（ReAct）等多种智能体模式，自动路由。
- **RAG 检索增强生成**：融合 Milvus 语义向量、Elasticsearch 关键词、Neo4j 知识图谱，三路 RRF 融合排序，自动降级，支持文档分块与异步实体关系抽取。
- **三层记忆系统**：短期记忆（滑动窗口）、长期记忆（Embedding/TF）、用户偏好（LLM+规则），支持去重、合并、衰减、过期淘汰。
- **图增强记忆**：长期记忆叠加 Neo4j 图层，支持 FOLLOWS、SIMILAR_TO、CAUSES、BELONGS_TO 等关系，提升历史联想与推理能力。
- **工具链与可恢复执行**：内置时间、天气、搜索、RAG 检索、命令执行等工具，支持 ReAct 规划-执行-生成流程，任务快照与重试机制保障稳定性。
- **沙箱执行**：支持 Docker / Local / Mock 三种沙箱后端，资源限制（CPU/内存/PID/网络），命令白名单安全校验。
- **高可用基础设施**：PostgreSQL 持久化、Milvus/ES/Neo4j/Kafka 可选，自动优雅降级，适配多种部署环境。

---


## 整体架构图

```mermaid
graph TB
    subgraph Frontend["前端 (index.html)"]
        CHAT["对话区"]
        SIDEBAR["侧边栏<br/>知识库上传 / 近期对话"]
        CTRL["控制栏<br/>知识库开关 / 工具选择"]
    end

    subgraph Router["智能路由层"]
        R["Router"]
    end

    subgraph Core["核心能力"]
        CHAT_ENGINE["Stage 1: 多轮对话<br/>LLM + STM 历史注入"]
        RAG_ENGINE["Stage 2: RAG<br/>Milvus + ES + Neo4j 三路检索 → RRF融合 → LLM合成"]
        TOOL_ENGINE["Stage 3: 工具调用<br/>time / weather / search / exec_command"]
        REACT_ENGINE["Stage 4: ReAct<br/>Planner → Executor → Generator"]
    end

    subgraph Memory["Stage 5: 三层记忆"]
        STM["短期记忆<br/>滑动窗口"]
        LTM["长期记忆<br/>Embedding语义 + Neo4j图关系"]
        PREF["用户偏好<br/>LLM NER提取"]
    end

    subgraph Harness["Stage 6: 稳定执行"]
        RETRY["重试机制"]
        SNAP["快照恢复"]
    end

    subgraph Sandbox["沙箱执行"]
        DOCKER["Docker 后端<br/>资源隔离 + 安全限制"]
        LOCAL["Local 后端"]
        MOCK["Mock 后端"]
    end

    subgraph Infra["基础设施 (全部可选, 优雅降级)"]
        PG["PostgreSQL<br/>偏好/LTM/RAG Chunk持久化"]
        MIL["Milvus<br/>语义向量近邻搜索"]
        ES["Elasticsearch<br/>BM25全文检索"]
        NEO["Neo4j<br/>知识图谱 + 图增强记忆"]
        KAFKA["Kafka<br/>事件流"]
    end

    CHAT --> R
    CTRL --> R

    R -->|纯对话| CHAT_ENGINE
    R -->|知识检索| RAG_ENGINE
    R -->|单工具| TOOL_ENGINE
    R -->|多工具编排| REACT_ENGINE

    CHAT_ENGINE --> Memory
    RAG_ENGINE --> Memory
    TOOL_ENGINE --> Memory
    REACT_ENGINE --> Memory
    REACT_ENGINE --> Harness

    TOOL_ENGINE --> Sandbox
    REACT_ENGINE --> Sandbox
    Sandbox --> DOCKER
    Sandbox --> LOCAL
    Sandbox --> MOCK

    RETRY --> SNAP
    SNAP --> PG

    STM -.->|多轮历史| CHAT_ENGINE
    LTM -.->|跨会话恢复| CHAT_ENGINE
    PREF -.->|个性化上下文| CHAT_ENGINE

    LTM --> PG
    LTM --> NEO
    PREF --> PG
    RAG_ENGINE --> MIL
    RAG_ENGINE --> ES
    RAG_ENGINE --> NEO
    CHAT_ENGINE --> KAFKA

    SIDEBAR -->|上传文档| RAG_ENGINE
```


## 核心流程时序图

```mermaid
sequenceDiagram
    actor User
    participant FE as 前端
    participant Router as 智能路由
    participant LLM as LLM API
    participant Planner as Planner LLM
    participant Executor as Executor
    participant Tool as Tool / RAG / Sandbox
    participant Generator as Generator LLM
    participant Memory as 三层记忆
    participant DB as PostgreSQL

    User->>FE: 输入消息 + 选择工具
    FE->>Router: POST /api/chat {message, tools}

    alt 纯对话 (无工具)
        Router->>Memory: 加载 STM 历史 + LTM + 偏好
        Memory-->>Router: 上下文消息列表
        Router->>LLM: Chat(systemPrompt + 历史 + 当前消息)
        LLM-->>Router: 自然语言回答
        Router->>Memory: 异步提取偏好 + 存储长期记忆

    else 工具编排 (ReAct)
        Router->>Planner: 分析query + 工具列表 → 执行计划
        Planner-->>Router: [{tool, params, reason}, ...]

        loop 按计划逐步执行
            Router->>Executor: 执行 tool(params)
            Executor->>Tool: 调用具体工具
            Tool-->>Executor: 观察结果
            Executor-->>Router: 步骤结果 (思考 → 动作 → 观察)
            Router->>DB: 保存快照
        end

        Router->>Generator: 合成所有观察 → 最终答案
        Generator-->>Router: 自然语言回答
        Router->>Memory: 异步存储长期记忆 + 提取偏好
    end

    Router-->>FE: {answer, steps, memories}
    FE-->>User: 渲染回答 + 思考过程
```


## RAG 三路混合检索流程图

```mermaid
sequenceDiagram
    actor User
    participant RAG as RAG Engine
    participant EMB as Embedding API
    participant MIL as Milvus
    participant ES as Elasticsearch
    participant NEO as Neo4j
    participant PG as PostgreSQL
    participant LLM as LLM API

    User->>RAG: 查询: "量子计算的应用领域"
    RAG->>EMB: Embed(query)
    EMB-->>RAG: query向量 [0.12, -0.34, ...]

    par 三路并行检索
        RAG->>MIL: MilvusSearch(query向量, topK)
        MIL-->>RAG: 语义结果 [{pg_id, distance}, ...]
        RAG->>ES: BM25Search(query, topK)
        ES-->>RAG: 关键词结果 [{pg_id, score}, ...]
        RAG->>NEO: GraphSearch(实体, maxHops=2)
        NEO-->>RAG: 图谱结果 [{pg_id, weight}, ...]
    end

    RAG->>RAG: RRF融合排序<br/>score = Σ(1/(k+rank_i)) × weight_i<br/>语义0.7 + BM25权重 + 图0.3

    RAG->>PG: LoadRAGChunksByIDs(top_pg_ids)
    PG-->>RAG: [{id, content}, ...]

    RAG->>LLM: Chat(系统提示 + 检索上下文 + 用户问题)
    LLM-->>RAG: 基于知识的回答

    RAG-->>User: 回答 + 引用来源
```


## 记忆系统详细流程图

```mermaid
sequenceDiagram
    actor User
    participant Agent as Agent
    participant STM as 短期记忆<br/>(滑动窗口 N×2)
    participant LLM as LLM API
    participant EMB as Embedding API
    participant LTM as 长期记忆<br/>(Embedding+TF双层)
    participant GRAPH as Neo4j图增强
    participant PREF as 用户偏好<br/>(LLM NER+规则双重)
    participant PG as PostgreSQL

    Note over User,PG: ═══════════ 服务启动: 跨会话恢复 ═══════════
    Agent->>PG: LoadPreferences(userID)
    PG-->>Agent: 历史偏好 [{key, value}, ...]
    Agent->>PREF: SaveBatch(恢复偏好到内存)
    Agent->>PG: LoadLongTermItems()
    PG-->>Agent: 历史LTM [{id, content, embedding, importance}, ...]
    Agent->>LTM: StoreItem(逐条恢复到内存索引)
    Note right of LTM: 重建TF词表<br/>恢复Embedding向量
    Agent->>GRAPH: 重建记忆节点与关系
    Agent->>STM: 初始化空窗口

    Note over User,PG: ═══════════ 每轮对话: 读取阶段 ═══════════
    User->>Agent: "你好，我叫小明，我喜欢打篮球"
    Agent->>STM: Add(user, 消息)

    Agent->>LTM: Recall(query, topK=3, queryEmbedding?)
    alt Embedding API 可用
        Agent->>EMB: Embed(query)
        EMB-->>LTM: query向量
        loop 遍历所有LTM条目
            LTM->>LTM: cosine(queryEmb, itemEmb)
            LTM->>LTM: score = sim×0.7 + importance×0.3
            alt score ≥ 0.4 阈值
                LTM->>LTM: 更新item.LastAccessed
                LTM->>LTM: 加入候选集
            else score < 0.4
                Note right of LTM: 过滤噪声，不注入
            end
        end
    else 降级: TF词袋
        LTM->>LTM: buildVocab(query) 扩充词表
        LTM->>LTM: textToVector(query) → TF向量
        loop 遍历所有LTM条目
            LTM->>LTM: cosine(queryTF, itemTF)
            LTM->>LTM: score = sim×0.7 + importance×0.3
        end
    end
    LTM-->>Agent: 召回记忆 [{content, score}, ...]

    Agent->>GRAPH: GraphRecall(相关节点, maxHops=2)
    GRAPH-->>Agent: 图扩展记忆 [关联历史, ...]

    Agent->>PREF: BuildContext()
    PREF-->>Agent: "【用户偏好】\n姓名: 小明\n喜好: 篮球"

    Agent->>LLM: Chat(systemPrompt + 偏好 + LTM记忆 + 图记忆 + STM历史 + 当前消息)
    LLM-->>Agent: "你好小明！喜欢篮球很棒..."

    Note over User,PG: ═══════════ 每轮对话: 写入阶段 ═══════════
    Agent->>STM: Add(assistant, 回答内容)

    Agent->>LTM: Store(用户消息, importance, embedding?)
    alt Embedding API 可用
        Agent->>EMB: Embed(消息内容)
        EMB-->>LTM: 语义向量
        loop 去重检测: vs 每条已有条目
            LTM->>LTM: cosine(newEmb, itemEmb)
            alt sim ≥ 0.95 (去重阈值)
                LTM->>LTM: 更新已有条目重要性+访问时间
            else sim < 0.95
                LTM->>LTM: 新增条目
            end
        end
        LTM->>PG: SaveLongTermItem(content, vector, importance)
    else 降级: TF词袋
        LTM->>LTM: buildVocab + textToVector
        LTM->>PG: SaveLongTermItem(content, nil, importance)
    end

    Agent->>GRAPH: 新增记忆节点 + 关系<br/>(FOLLOWS/SIMILAR_TO/CAUSES)

    par 异步: LLM NER偏好提取
        Agent->>LLM: "从以下对话提取用户偏好: ..."
        LLM-->>Agent: {"姓名":"小明","喜好":"篮球"}
        Agent->>PREF: SaveBatch(kvs)
        PREF->>PG: SavePreference(key, value)
    and 同步: 规则兜底 (立即生效)
        Agent->>PREF: ExtractAndSave("我喜欢打篮球")
        PREF-->>Agent: key="喜好", value="打篮球", ok=true
        PREF->>PG: SavePreference(key, value)
    end

    Note over User,PG: ═══════════ 合并触发: 每5条新记忆 ═══════════
    LTM->>LTM: NeedConsolidation()?
    alt storeCount ≥ TriggerInterval(5)
        Note over LTM: Phase 1: 重要性衰减
        LTM->>LTM: importance × DecayRate^days<br/>(每日×0.995, 30天≈0.86)
        Note over LTM: Phase 2: 去重 + 合并
        loop 两两比较相似度
            alt sim ≥ 0.95 (DedupThreshold)
                LTM->>LTM: 保留importance更高的, 删除另一条
                LTM->>PG: DELETE removed IDs
                LTM->>GRAPH: 删除对应图节点
            else sim ≥ 0.80 (SimilarityThreshold)
                LTM->>LTM: mergeItems(): 内容拼接/保留较长
                LTM->>PG: UPDATE merged item, DELETE被合并条目
                LTM->>GRAPH: 合并图关系, 保护高中心度节点
            end
        end
        Note over LTM: Phase 3: 过期淘汰
        loop 检查每条记忆
            alt days > TTL(30) AND importance < Min(0.3)
                LTM->>LTM: 删除过期条目
                LTM->>PG: DELETE expired IDs
            end
        end
        LTM->>LTM: rebuildVocab() 重建词表
    end

    Note over User,PG: ═══════════ 会话结束 ═══════════
    Note right of STM: 进程消亡, STM清除<br/>不持久化（设计如此）
    Note right of LTM: 已实时持久化到PG<br/>Consolidation结果已同步
    Note right of GRAPH: 图关系已持久化到Neo4j<br/>下次启动恢复
    Note right of PREF: 已实时持久化到PG<br/>下次启动LoadPreferences恢复
```


## 技术实现亮点

- **RAG 检索增强**：
    - 支持三路混合检索（Milvus 语义向量、ES BM25 关键词、Neo4j 知识图谱），RRF 融合排序。
    - 文本分块采用窗口重叠，提升召回覆盖率。
    - 检索模式自动切换，单路故障自动降级，支持企业级高可用。
    - 检索结果结构化，便于 LLM 合成与追溯。

- **三层记忆系统**：
    - 短期记忆：滑动窗口保存最近 N 轮对话。
    - 长期记忆：Embedding/TF 双层，支持去重、合并、衰减、过期淘汰。
    - 偏好记忆：LLM+规则自动提取用户偏好，持久化跨会话恢复。

- **图增强记忆**：
    - 记忆写入时自动建立时序（FOLLOWS）、相似（SIMILAR_TO）等关系。
    - 支持图扩展召回，发现间接关联历史记忆。
    - 合并淘汰时保护高中心度节点，防止核心知识丢失。

- **智能体与工具链**：
    - 路由优先级：ReAct 复合推理 > 单工具 > RAG 检索 > 纯对话。
    - 工具链支持自定义扩展，RAG 检索作为知识库工具无缝集成。
    - ReAct 规划-执行-生成流程，任务快照与重试机制保障稳定性。

- **沙箱执行**：
    - 支持 Docker（资源隔离 + 安全限制）、Local（直接执行）、Mock（测试）三种后端。
    - 命令长度限制、白名单校验、资源配额（CPU/内存/PID/网络/只读文件系统）。

- **工程与基础设施**：
    - PostgreSQL 持久化所有关键数据。
    - Milvus/ES/Neo4j/Kafka 可选，自动降级，适配多种部署环境。
    - 前后端解耦，支持多端接入。

---

## 快速开始

### 本地运行

```bash
# 1. 安装依赖
go mod tidy

# 2. 启动基础设施（需要 Docker Desktop）
docker compose up -d

# 3. 启动应用
go run .

# 4. 访问 http://localhost:8090
```

### Docker 部署

```bash
# 编译 + 启动全部服务
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o final-agent .
docker compose up -d --build
```

### 配置

编辑 `config/config.yaml`，填入 API Key：

- `llm.api_key` — 火山引擎 Ark 对话模型 API Key
- `embedding.api_key` — 火山引擎 Embedding 模型 API Key
- `search.api_key` — Tavily 搜索 API Key（可选）

> 所有基础设施（Milvus/PG/ES/Kafka/Neo4j）均为可选，连接失败自动降级为内存模式，不影响启动。

---

## 目录结构

```
├── config/                   配置加载（YAML → 结构体）
│   ├── config.go
│   └── config.yaml
├── internal/
│   ├── agent/                智能体核心与调度（ReAct + Harness + 路由）
│   ├── graph/                知识图谱（Neo4j 实体关系抽取 + 图检索）
│   ├── handler/              HTTP API 路由处理
│   ├── infra/                基础设施连接（Milvus / PG / ES / Kafka）
│   ├── llm/                  LLM/Embedding 客户端（真实 API + Mock 降级）
│   ├── memory/               三层记忆系统（短期 / 长期 / 用户偏好 + 图增强）
│   ├── rag/                  RAG 引擎（三路混合检索 + RRF 融合）
│   ├── sandbox/              沙箱执行（Docker / Local / Mock + 安全校验）
│   └── tools/                工具定义与调用（time/weather/search/exec_command）
├── frontend/                 单文件前端 HTML
├── main.go                   入口
├── docker-compose.yml        基础设施编排
├── Dockerfile                应用容器镜像
└── go.mod
```

---

## 致谢

本项目受多模态智能体、RAG、知识图谱、记忆增强等前沿研究启发，欢迎交流与合作。
