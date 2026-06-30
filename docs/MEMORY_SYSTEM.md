# 记忆系统设计与面试解析

> **面向读者**：希望快速理解项目记忆系统整体设计，并能从容回答相关面试问题的工程师。
>
> **阅读路径**：第一部分通读，建立整体认知；第二部分按问题索引，针对性深挖。

---

## 第一部分：整体链路（5 分钟看清全貌）

### 1.1 记忆系统在 Agent 中处于什么位置

```
用户提问 ──► [上下文装配] ──► LLM 推理 ──► [记忆抽取] ──► 用户响应
              ▲                              │
              │                              ▼
              └──────── 记忆系统 ◄────── [合并/衰减/淘汰]
```

记忆系统是 agent 的**认知中枢**，它在三个时刻参与：
- **每轮推理前**：装配 prompt 前缀（个性化、相关回忆、任务状态）
- **每轮推理后**：从 LLM 回答中抽取值得长期记住的事实
- **周期性**：合并相似记忆、衰减重要性、淘汰过期条目

### 1.2 五种记忆形态

不同的"使用模式"对应不同的存储设计——不是把所有数据塞一个表。

| 形态 | 数据 | 持久化 | 用途 |
|---|---|---|---|
| **ShortTerm** | 最近 N 轮对话 | 否（PG 回放） | 上下文连贯 |
| **Preference** | 结构化 KV | PG | 用户身份/偏好 |
| **LongTerm** | Item{Content, Embedding, Importance, Category, Tags} | PG | 跨会话事实 |
| **GraphMemory** | LongTerm + Neo4j 图节点/边 | PG + Neo4j | 关联召回 + 中心度保护 |
| **TaskMemBuffer** | 任务步骤观察 ring buffer | 否 | ReAct 步骤记忆 |

### 1.3 四阶段管线

整套系统可以归纳成四个阶段，每个阶段解决一类问题：

```
┌─────────────────────────────────────────────────────────┐
│  Stage 1：写入即分类                                      │
│  LLM 抽事实 → 规则/LLM 双通道分类 → embedding → 去重判定     │
└──────────────┬──────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 2：召回即过滤                                      │
│  按槽位走不同策略：身份枚举 / 语义召回 / 图扩展                │
│  综合分 = sim×0.7 + Importance×0.3                       │
└──────────────┬──────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 3：合并即衰减（异步周期触发）                         │
│  Phase 1 衰减 → Phase 2 去重/合并 → Phase 3 过期           │
│                                  → Phase 4 保护钩子       │
└──────────────┬──────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────┐
│  Stage 4：装配即编排（promptctx）                         │
│  按 Mode 选 Schema → 6 槽位并发填 → 双层预算裁剪            │
│  Constraints 永不丢                                      │
└─────────────────────────────────────────────────────────┘
```

### 1.4 端到端时序

```
用户发问 query
   │
   ▼
ShortTerm.Add(user, query)
   │
   ▼ [Stage 4: 装配]
buildContextPrefix(query, mode)
   ├─ 并发 fillSlot × 6
   │    ├─ ProfileSource     (FilterByCategory 枚举)
   │    ├─ RecallSource      (RecallByFilter 向量+图扩展)
   │    ├─ TaskMemSource     (ring buffer)
   │    ├─ ToolStateSource
   │    ├─ PlannerSource
   │    └─ ConstraintsSource
   └─ applyGlobalBudget (按优先级裁剪)
   │
   ▼
LLM 推理 → answer
   │
   ▼ [Stage 1: 异步抽取]
extractMemoryFromReply
   ├─ LLM 抽 k-v 事实
   ├─ classifyMemoryContent (规则)
   ├─ llmClassifyMemory (LLM 兜底)
   └─ ltm.StoreClassified  ◄── 写入即去重
   │
   ▼ [Stage 3: 异步合并]
if NeedConsolidation (storeCount ≥ 5):
   GraphAwareConsolidate
   ├─ Phase 1 衰减     (importance × 0.995^days)
   ├─ Phase 2 去重/合并 (sim ≥ 0.95 / 0.80)
   ├─ Phase 3 过期     (age > TTL AND imp < min)
   ├─ Phase 4 保护钩子 (图入度 ≥ 3 撤回删除)
   └─ 物理删除 + 同步 PG/Neo4j
```

### 1.5 关键代码地图

| 组件 | 文件 |
|---|---|
| 记忆栈聚合 | `internal/application/chat/mem_stack.go` |
| 写入逻辑 | `internal/application/chat/mem_writer.go` |
| 启动恢复 | `internal/application/chat/mem_restore.go` |
| 长期记忆核心 | `internal/domain/memory/longterm/longterm.go` |
| 图增强层 | `internal/domain/memory/graph/graphmem.go` |
| 短期记忆 | `internal/domain/memory/shortterm/shortterm.go` |
| 偏好记忆 | `internal/domain/memory/preference/preference.go` |
| 装配引擎 | `internal/domain/promptctx/assembler.go` |
| Schema 定义 | `internal/domain/promptctx/schema.go` |

---

## 第一部分补充：合并链路深度解析

> 合并是整套记忆系统**最关键、也最难讲清楚**的部分。Stage 3 的"合并即衰减"是项目区别于 mem0、独立完成的工程化设计。本节从触发机制讲到落库提交，覆盖每个 Phase 的代码、阈值含义、并发模型、异常路径。

### A. 为什么需要合并

如果只有"写入"和"召回"，会出现三类问题：

1. **重复积累**：用户多次表达同一件事，向量库里堆十几条"用户喜欢咖啡"的近义条目，召回时 Top-K 被同义重复挤占
2. **信息陈旧**：三个月前说过的临时偏好（"最近在减肥"）没法自然遗忘，永远参与召回
3. **存储膨胀**：长期使用下条目数线性增长，召回 O(n²) 越来越慢

合并机制的目标：**让记忆库自我演进——重复变浓缩、过时变淡化、相似变聚合、关键被保护**。

### B. 触发模型：为什么不是定时器

**`NeedConsolidation`**（`longterm.go:271-277`）：
```go
return m.consolidationCfg != nil &&
    m.consolidationCfg.TriggerInterval > 0 &&
    m.storeCount >= m.consolidationCfg.TriggerInterval   // 默认 5
```

**调用点**（`runtime_process.go:228-239`）：
```go
a.goSafe("process.consolidate", func() {
    if a.mem.ltm.NeedConsolidation() {
        if a.mem.graphMem != nil {
            result = a.mem.graphMem.GraphAwareConsolidate()
        } else {
            result = a.mem.ltm.Consolidate()
        }
        a.syncConsolidationToDB(result)
    }
})
```

**三个设计选择**：

1. **计数触发而非定时**：低活跃期不空转，高活跃期及时清理
2. **`finalize` 末尾异步触发**：在用户已经收到 LLM 回答之后跑，**绝不阻塞用户响应**
3. **`goSafe` 包裹**：合并代码 panic 时通过 `recover` 兜住，避免拖垮整个进程
4. **`storeCount` 在 `Consolidate` 开头清零**——防止合并慢时多次堆叠触发

### C. 四阶段管线（重点）

整段 `Consolidate` 在 `mu.Lock()` 临界区内串行执行（`longterm.go:413-565`）。**核心理念：四个 Phase 共享同一个 `removed map` 和 `result` 结构，最后一次性物理删除——保证原子性**。

```
┌──────────────────────── Consolidate (持 mu.Lock) ────────────────────────┐
│                                                                         │
│  Phase 1：衰减     不动 m.Items 的 removed 状态，只改 Importance          │
│       │                                                                 │
│       ▼                                                                 │
│  Phase 2：去重/合并  按相似度分流，标记 removed[i] = true                  │
│       │                                                                 │
│       ▼                                                                 │
│  Phase 3：过期淘汰  双门槛筛选，标记 removed[i] = true                    │
│       │                                                                 │
│       ▼                                                                 │
│  Phase 4：保护钩子  ProtectFn(DeleteFromDB) → 撤回部分 removed 标记       │
│       │                                                                 │
│       ▼                                                                 │
│  Commit：物理删除 m.Items + 重建 vocab                                   │
│       │                                                                 │
│       ▼                                                                 │
│  返回 ConsolidationResult                                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    syncConsolidationToDB
                ├─ Delete(DeleteFromDB)            一条 SQL DELETE IN
                ├─ Update(UpdateInDB)              逐条 UPDATE 合并结果
                └─ UpdateImportanceBatch(DecayUpdates)  unnest 单条 SQL 批量
                            │
                            ▼
                    Neo4j 异步 deleteMemoryNode
```

#### Phase 1：指数衰减（含落库）

**代码**（`longterm.go:423-437`）：
```go
const minDecayDelta = 0.01
for i := range m.Items {
    days := time.Since(m.Items[i].CreatedAt).Hours() / 24
    oldImp := m.Items[i].Importance
    newImp := oldImp * math.Pow(m.consolidationCfg.DecayRate, days)
    m.Items[i].Importance = newImp
    if oldImp-newImp >= minDecayDelta {
        result.DecayUpdates = append(result.DecayUpdates, DecayUpdate{
            ID:         m.Items[i].ID,
            Importance: newImp,
        })
    }
}
```

**关键点**：

| 设计选择 | 原因 |
|---|---|
| 按 `CreatedAt` 而非"上次衰减时间" | 幂等——重复跑不累积错误 |
| `0.995^days` 指数曲线 | 平滑、生物学合理（Ebbinghaus 遗忘曲线） |
| `Δ ≥ 0.01` 才入 `DecayUpdates` | 控制写放大——日衰减 0.5%，约 2 天才会触发一次落库 |
| 不立即删除低 importance | 衰减只是"降低权重"，是否淘汰在 Phase 3 决定 |

**衰减节奏对照**：
```
Day 0:    importance = 1.000   (新鲜)
Day 7:    importance = 0.965   (一周后小幅衰减)
Day 30:   importance = 0.860   (30 天后约 86%)
Day 100:  importance = 0.606   (3 个月后约 60%)
Day 365:  importance = 0.161   (一年后约 16%)
```

**召回端的连锁影响**：召回综合分 `sim×0.7 + Importance×0.3`，衰减让老条目即使语义匹配也排不到前面，给新条目让路。

#### Phase 2：去重 + 合并（双阈值分流）

**代码**（`longterm.go:442-473`）：
```go
for i := 0; i < len(m.Items); i++ {
    if removed[i] { continue }
    for j := i + 1; j < len(m.Items); j++ {
        if removed[j] { continue }
        sim := m.itemSimilarity(m.Items[i], m.Items[j])

        if sim >= m.consolidationCfg.DedupThreshold {        // ≥0.95
            // 去重：保留 importance 高的
            if m.Items[j].Importance >= m.Items[i].Importance {
                removed[i] = true; result.Deduped++
                result.DeleteFromDB = append(result.DeleteFromDB, m.Items[i].ID)
            } else {
                removed[j] = true; result.Deduped++
                result.DeleteFromDB = append(result.DeleteFromDB, m.Items[j].ID)
            }
        } else if sim >= m.consolidationCfg.SimilarityThreshold {  // 0.80~0.95
            // 合并：mergeItems
            merged := m.mergeItems(m.Items[i], m.Items[j])
            m.Items[i] = merged
            removed[j] = true; result.Merged++
            result.DeleteFromDB = append(result.DeleteFromDB, m.Items[j].ID)
            result.UpdateInDB = append(result.UpdateInDB, merged)
        }
    }
}
```

**双阈值分流的语义**：

| 区间 | 含义 | 动作 | 例子 |
|---|---|---|---|
| `sim ≥ 0.95` | 同义改写 | **去重**：删 importance 低的 | "用户喜欢咖啡" / "用户偏好咖啡" |
| `0.80 ≤ sim < 0.95` | 语义相近但表达不同 | **合并**：信息融合 | "用 Go 写后端" / "日常工作 Go 开发" |
| `sim < 0.80` | 不同事实 | **保留两条** | "喜欢咖啡" / "讨厌甜食" |

**`mergeItems` 细节**（`longterm.go:522-559`）：
```go
func (m *LongTerm) mergeItems(a, b Item) Item {
    base, other := a, b
    if b.Importance > a.Importance {
        base, other = b, a
    }
    merged := Item{
        ID:           base.ID,
        Importance:   math.Max(base.Importance, other.Importance),
        Embedding:    base.Embedding,
        CreatedAt:    base.CreatedAt,
        LastAccessed: time.Now(),
    }
    // 内容合并
    if !strings.Contains(base.Content, other.Content) && !strings.Contains(other.Content, base.Content) {
        merged.Content = base.Content + "；" + other.Content
    } else if len(other.Content) > len(base.Content) {
        merged.Content = other.Content
    } else {
        merged.Content = base.Content
    }
    // Embedding 加权平均
    if len(base.Embedding) > 0 && len(other.Embedding) > 0 {
        wA, wB := base.Importance, other.Importance
        total := wA + wB
        merged.Embedding = make([]float64, len(base.Embedding))
        for i := range base.Embedding {
            merged.Embedding[i] = (base.Embedding[i]*wA + other.Embedding[i]*wB) / total
        }
    }
    return merged
}
```

**合并的字段语义**：

| 字段 | 策略 | 设计意图 |
|---|---|---|
| `ID` | 取 base（importance 高的）的 ID | 保留稳定标识，外部引用不失效 |
| `Importance` | 取 max | 不丢失重要性信号 |
| `Content` | 子串关系取长，否则 `；` 拼接 | 保留全部信息 |
| `Embedding` | 按 Importance 加权平均 | 语义连续，召回行为不突变 |
| `CreatedAt` | 取 base | 衰减节奏不被合并打乱 |
| `LastAccessed` | now | 视为"最新触达" |

**为什么不调 LLM 合并**：
- 确定性：相同输入必定相同输出，可单测
- 低延迟：纯计算，微秒级完成
- 可观测：每个字段如何变化都可追踪
- 代价：长期累积"；"拼接（"咖啡；拿铁；早晨咖啡"）—— 已知短板，可演进为 LLM rewriter

**O(n²) 复杂度**：双重循环两两比较。万级规模需要演进为 ANN 索引（HNSW / pgvector），目前千级以下完全够用。

#### Phase 3：双门槛过期淘汰

**代码**（`longterm.go:476-488`）：
```go
for i := range m.Items {
    if removed[i] { continue }
    days := time.Since(m.Items[i].CreatedAt).Hours() / 24
    if m.consolidationCfg.TTLDays > 0 &&
        days > float64(m.consolidationCfg.TTLDays) &&
        m.Items[i].Importance < m.consolidationCfg.MinImportance {
        removed[i] = true
        result.Expired++
        result.DeleteFromDB = append(result.DeleteFromDB, m.Items[i].ID)
    }
}
```

**双门槛 AND 关系**：

```
                       Importance
                         ▲
                  保留   │   保留
              ───────────┼───────────
                         │
     MinImportance ──────┼──────────  (默认 0.3)
                         │
                  保留   │   淘汰  ◄── 这个象限才会被删
              ───────────┴───────────►
                       TTLDays         days
                       (默认 30)
```

**为什么必须 AND 而不是 OR**：

- 只看 TTL：用户姓名（importance 0.95）一年后被删——明显错误
- 只看 importance：所有低 importance 立即删，新记忆没机会成长——明显错误
- AND：**老 + 不重要才删**，给所有维度的容错空间

**Phase 3 的 `if removed[i] { continue }` 很关键**：避免对 Phase 2 已经标记删除的条目重复入 `DeleteFromDB`，避免 PG 端的 ID 重复。

#### Phase 4：图中心度保护钩子（最近一次重构）

**问题背景（修复前的真实 Bug）**：

```
旧实现：
  ltm.Consolidate()                       ← 内部已 m.Items = newItems (物理删除)
       ↓
  GraphAwareConsolidate 后处理
       ├─ getHighCentralityMemoryIDs(DeleteFromDB)
       └─ filtered := DeleteFromDB - protected
       ↓
  syncConsolidationToDB(result)           ← PG 不删 protected 节点

后果：保护节点已经从 m.Items 物理消失了
      但 PG 还在 → 重启后又被恢复，但此期间召回不到
      内存/PG/Neo4j 三处状态不一致
```

**修复方案**：把保护逻辑从"后处理"提前到"LTM 内部"——同临界区原子完成。

**`ConsolidationConfig` 加钩子**（`longterm.go:48-64`）：
```go
type ConsolidationConfig struct {
    // ... 其他字段
    // ProtectFn 是删除前的保护钩子。给定 candidates（即将物理删除的 ID 集合），
    // 返回其中需要保留的 ID 子集。
    ProtectFn func(candidates []int) (protected []int)
}
```

**Phase 4 实现**（`longterm.go:494-528`）：
```go
if m.consolidationCfg.ProtectFn != nil && len(result.DeleteFromDB) > 0 {
    protected := m.consolidationCfg.ProtectFn(result.DeleteFromDB)
    if len(protected) > 0 {
        protSet := make(map[int]bool, len(protected))
        for _, id := range protected {
            protSet[id] = true
        }
        // 1) 撤回 removed 标记 → m.Items 不被物理删除
        for i := range m.Items {
            if removed[i] && protSet[m.Items[i].ID] {
                removed[i] = false
            }
        }
        // 2) 同步剔除 DeleteFromDB → PG 不会被删
        filtered := result.DeleteFromDB[:0]
        for _, id := range result.DeleteFromDB {
            if !protSet[id] {
                filtered = append(filtered, id)
            }
        }
        // 3) 计数回退：保护节点不计入 Deduped/Expired/Merged
        rescued := len(result.DeleteFromDB) - len(filtered)
        result.DeleteFromDB = filtered
        for _, n := range []*int{&result.Deduped, &result.Expired, &result.Merged} {
            if rescued <= 0 { break }
            take := rescued
            if take > *n { take = *n }
            *n -= take
            rescued -= take
        }
    }
}
```

**GraphMemory 注入钩子**（`graphmem.go:381-420`）：
```go
func (gm *GraphMemory) installProtectHookOnce() {
    gm.protectOnce.Do(func() {
        cfg := gm.ltm.ConsolidationCfg()
        if cfg == nil { return }
        cfg.ProtectFn = func(candidates []int) []int {
            if !gm.neoAvailable() || len(candidates) == 0 {
                return nil
            }
            protected := gm.getHighCentralityMemoryIDs(candidates, 3)
            if len(protected) > 0 {
                log.Printf("🛡️  图中心度保护：%d 条记忆免于删除（入度≥3）", len(protected))
            }
            return protected
        }
        gm.ltm.SetConsolidationConfig(cfg)
    })
}
```

**入度查询 Cypher**（`graphmem.go:179-184`）：
```cypher
MATCH (m:Memory) WHERE m.mem_id IN $ids
WITH m, size([(m)<-[]-() | 1]) AS indegree
WHERE indegree >= $threshold
RETURN m.mem_id AS id
```

**修复带来的好处**：

| 维度 | 修复前 | 修复后 |
|---|---|---|
| 一致性 | 内存/PG 不同步窗口 | 同临界区原子完成 |
| 解耦 | LongTerm 知道"图"概念 | 钩子函数注入，LTM 不感知 |
| 可扩展 | 只支持图保护 | 任何策略都可挂（重要性 > 0.9 永远保留等） |
| 幂等 | 多次调用重复挂 | `sync.Once` 保证一次 |

**为什么钩子是"撤回"语义而非"过滤"**：

- 钩子在 Phase 1-3 标记完 `removed` 之后才执行
- 此时已经知道哪些条目即将被删
- 保护策略只需要回答"这些里面哪些不能删"
- 比"在每个 Phase 里事先排除"更灵活——能保护跨 Phase 的删除（去重、合并、过期都可保护）

#### Commit 阶段：物理删除 + 重建词表

**代码**（`longterm.go:530-540`）：
```go
var newItems []Item
for i, item := range m.Items {
    if !removed[i] {
        newItems = append(newItems, item)
    }
}
m.Items = newItems
m.rebuildVocab()
```

**`rebuildVocab`**（`longterm.go:550-557`）：
```go
m.vocabID = make(map[string]int)
m.vocab = nil
for _, item := range m.Items {
    m.buildVocab(item.Content)
}
```

**为什么要重建词表**：删除/合并后，原 vocab 里有些 token 已经没有任何 Item 引用——TF 兜底召回时这些 token 是噪声。重建保证 vocab 与 Items 一致。

### D. 持久化提交

**`syncConsolidationToDB`**（`mem_writer.go:126-148`）：
```go
func (a *UnifiedAgent) syncConsolidationToDB(result longterm.ConsolidationResult) {
    if len(result.DeleteFromDB) > 0 {
        a.repos.ltm.Delete(result.DeleteFromDB)
    }
    for _, item := range result.UpdateInDB {
        embJSON, _ := json.Marshal(item.Embedding)
        a.repos.ltm.Update(item.ID, item.Content, item.Importance, embJSON)
    }
    if len(result.DecayUpdates) > 0 {
        updates := make([]ltmrepo.ImportanceUpdate, 0, len(result.DecayUpdates))
        for _, d := range result.DecayUpdates {
            updates = append(updates, ltmrepo.ImportanceUpdate{ID: d.ID, Importance: d.Importance})
        }
        a.repos.ltm.UpdateImportanceBatch(updates)
    }
}
```

**三类写入对应三种 SQL**：

| 类型 | SQL 形式 | 量级 |
|---|---|---|
| `DeleteFromDB` | `DELETE FROM ... WHERE id IN (...)` | 单条 SQL，几条到几十条 ID |
| `UpdateInDB` | 逐条 `UPDATE ... WHERE id = ?` | 通常合并不会很多，单次 1-3 条 |
| `DecayUpdates` | `UPDATE ... FROM (SELECT unnest(...))` 单条 | 量大，但合成一条 SQL |

**`UpdateImportanceBatch` 关键 SQL**（`infrastructure/persistence/longterm/longterm.go`）：
```sql
UPDATE long_term_memory AS m
SET importance = v.importance
FROM (SELECT unnest($1::BIGINT[]) AS id,
             unnest($2::DOUBLE PRECISION[]) AS importance) AS v
WHERE m.id = v.id
```

**写放大控制**：
- 1k 条记忆假设全部需要衰减更新 → 不是 1k 次 UPDATE，而是 **1 次 SQL** 完成
- 配合 Phase 1 的 `Δ ≥ 0.01` 阈值 → 实际每次 Consolidate 只有几百条入列
- 比单条 UPDATE 快 10-50 倍

### E. Neo4j 同步

**`GraphAwareConsolidate`**（`graphmem.go:367-379`）：
```go
gm.installProtectHookOnce()           // 一次性挂 ProtectFn
result := gm.ltm.Consolidate()        // LTM 内部完成保护决策

if !gm.neoAvailable() {
    return result
}

goSafe("graphmem.consolidate-delete", func() {
    for _, id := range result.DeleteFromDB {
        gm.deleteMemoryNode(id)        // DETACH DELETE 节点 + 全部边
    }
})
return result
```

**Cypher**（`graphmem.go:151-164`）：
```cypher
MATCH (m:Memory {mem_id: $id}) DETACH DELETE m
```

`DETACH DELETE` 一次性删节点和所有连接的边，避免悬空边。

**异步 + recover**：Neo4j 写入失败不影响主流程，但日志会记录。

### F. 异常路径与边界情况

**Q：合并执行期间用户请求会怎样？**
- `Consolidate` 持 `mu.Lock()`，期间所有写入和召回会等锁
- 但合并是异步的，不在用户请求的关键路径上
- 用户感知到的延迟仅是召回排队等待——通常几十毫秒

**Q：`Consolidate` 一半时进程崩溃会怎样？**
- 内存状态：进程消失，重启从 PG 恢复——回到崩溃前的稳定状态
- PG 状态：因为 LTM 内部还没提交（`syncConsolidationToDB` 没跑），PG 不变
- 一致性保证：要么全成功要么全失败（伪事务语义）

**Q：Phase 4 撤回后，Phase 1 给保护节点做的衰减还在吗？**
- **还在**——Phase 4 只撤回 `removed` 标记，不动 `Importance`
- 这是合理的：图保护是"不删"，不是"重置"
- 保护节点继续按衰减节奏演进，下次 Consolidate 时如果不再被保护，仍可能被淘汰

**Q：`storeCount` 计数何时递增/清零？**
- 递增：`StoreClassified` 新增条目时（`longterm.go:222`）
- 清零：`Consolidate` 开头（`longterm.go:420`）
- 注意：写入去重命中时 `storeCount` **不递增**——补丁更新不算新条目

### G. 合并对各组件的连锁影响

```
       ┌─────────────────┐
       │   Consolidate   │
       └────────┬────────┘
                │
   ┌────────────┼────────────┬───────────────┐
   ▼            ▼            ▼               ▼
LTM.Items    PG表        Neo4j节点        召回结果
 物理重建    删除/更新     异步删除         立即变化
   │           │             │               │
   ▼           ▼             ▼               ▼
 vocab      持久化       图扩展候选      新一轮装配
 重建       生效         减少              prompt 不同
```

合并不是孤立操作——它会影响：
1. **下次召回**：`m.Items` 变化、综合分变化、TF 词表变化
2. **下次装配**：召回结果变 → prompt 内容变 → LLM 输出可能不同
3. **图扩展**：节点删除 → 1-hop 邻居数量减少
4. **ID 稳定性**：保留的合并主体 ID 不变，外部引用仍可用

### H. 调参指南

| 参数 | 默认值 | 调高 | 调低 |
|---|---|---|---|
| `DedupThreshold` | 0.95 | 减少误合并（保守） | 加强归一（激进） |
| `SimilarityThreshold` | 0.80 | 合并范围窄 | 合并范围宽 |
| `DecayRate` | 0.995 | 衰减慢，记忆持久 | 衰减快，遗忘快 |
| `TTLDays` | 30 | 保留更久 | 更激进淘汰 |
| `MinImportance` | 0.3 | 更激进淘汰 | 保留更多边缘记忆 |
| `TriggerInterval` | 5 | 合并频率低，省 CPU | 合并频率高，更及时 |

**典型场景**：
- **高质量长期助手**：`DecayRate 0.998 / TTLDays 90 / MinImportance 0.2` —— 让事实更持久
- **任务密集型 agent**：`TriggerInterval 3 / DedupThreshold 0.93` —— 快速合并任务步骤产生的相似条目
- **对话场景多变用户**：`SimilarityThreshold 0.75` —— 加强归一，避免同义条目堆积

---

## 第二部分：面试 Q&A 实战

### 🎯 整体设计类

#### Q1：讲讲你这个 agent 的记忆系统是怎么设计的？

**标准回答**：

记忆系统按**四阶段管线**组织，对应 agent 在不同时刻的认知需求：

1. **写入阶段**——LLM 抽事实，规则+LLM 双通道分类（identity/preference/episodic 等 7 类），写入前做向量去重（`Cosine ≥ 0.95` 命中即补丁更新而非新增）
2. **召回阶段**——三种策略：身份按 Category 枚举、长期事实走向量+TF 兜底、图增强层做 1-hop 扩展；综合分 `sim×0.7 + Importance×0.3`
3. **合并阶段**——计数触发（每 5 条新增），四阶段串行：指数衰减 → 双阈值去重/合并 → 双门槛过期淘汰 → 图中心度保护钩子
4. **装配阶段**——promptctx 按 Mode 选 Schema，6 个认知槽位并发填，双层预算裁剪（Constraints 永不丢）

存储上分**五种形态**：短期窗口、偏好 KV、长期向量、图增强、任务步骤——不同生命周期、不同访问模式。持久化用 PostgreSQL + Neo4j。

#### Q2：为什么不直接把所有记忆放一个表？分这么多层是不是过度设计？

**标准回答**：

不是过度设计，是**生命周期不同**决定的：

- **短期对话** = 几分钟，几十条，要的是连续性 → 内存窗口足够
- **用户偏好** = 几年，几十条，要的是确定性 → 结构化 KV
- **长期事实** = 几个月，上千条，要的是相关性 → 向量召回
- **任务步骤** = 一次任务，几十条，要的是顺序性 → ring buffer
- **图记忆** = 长期，但要表达关联 → 图层

如果都塞一张表：
- 偏好被 episodic 挤出 Top-K
- 任务步骤污染长期召回
- 图关联无法表达
- 衰减策略对偏好和事实需求完全不同

**反过来想**：mem0 最早用单一 store，后来也加了 mem0g 图层、user_id 隔离等机制——本质上是补回这层"按用途分"的能力。

---

### 🎯 写入与去重类

#### Q3：记忆是怎么去重的？

**标准回答**：

**双层去重**：

1. **写入时去重**（同步、O(n)）——`StoreClassified` 入库前扫全表算 `Cosine`，`≥ DedupThreshold(0.95)` 命中：
   - **不新增**，对已有条目做"补丁更新"
   - `Importance = max(new, old)`
   - 类别升级（`general` 被更具体类别覆盖）
   - 标签合并去重

2. **合并时去重**（异步、O(n²)）——`Consolidate` Phase 2 两两比较：
   - `sim ≥ 0.95` → 删除 importance 低的
   - `0.80 ≤ sim < 0.95` → mergeItems 合并

**关键设计**：去重不是"丢弃"，而是"加固"——多次提到的事实自然 Importance 累积，活跃度（LastAccessed）刷新。

#### Q4：为什么阈值选 0.95 / 0.80？怎么调出来的？

**标准回答**：

阈值是**经验值 + 可调参数**：

- `0.95`（去重）：Cosine 在 0.95 以上基本是同义改写（"用户喜欢咖啡" / "用户偏好咖啡"），合一不丢信息
- `0.80`（合并）：处于"语义相近但说法不同"区间（"用户用 Go 写后端" / "用户日常工作是 Go 开发"），合并比并存更紧凑
- `<0.80`：保留两条，避免过度聚合丢失细节

调参原则：
- 噪声多的场景调高（比如 0.97/0.85），减少误合并
- 用户表达多样的场景调低（0.93/0.75），加强归一
- 全部暴露在 `ConsolidationConfig` 里，运行时可改

#### Q5：为什么写入要分类？分类用什么策略？

**标准回答**：

**分类是为了让召回精准**——这是把 Top-K 污染问题在工程上解决的关键。

举例：用户问"我喜欢吃什么"，应该召回 preference 类，不应该召回 "上次报错信息" 这种 tool_failure 类——单纯按相似度排序无法做到这点。

**双通道分类**：
1. **规则层（同步、零延迟）**——`classifyMemoryContent` 关键词命中 7 类：
   ```go
   "叫/名字" → identity
   "喜欢/偏好/习惯" → preference
   "失败/错误/报错" → tool_failure
   "禁止/必须/不能" → policy
   ```
2. **LLM 兜底**——规则未命中时调一次 LLM 返回 JSON，失败回落 `general`

**为什么两条通道**：
- 单规则覆盖率窄
- 单 LLM 慢且贵
- 组合：90% 走规则毫秒级，10% 走 LLM 保证长尾

#### Q6：偏好提取为什么有"规则 + LLM"两条路？只用 LLM 不行吗？

**标准回答**：

**核心矛盾是延迟与覆盖率**：

- 用户说"我叫张三"，下一轮立即问"我叫什么"——LLM 异步抽取还没完成，第二轮回答"不知道"
- 这是**即时一致性问题**

解决方案：
- 规则路（`Preference.ExtractAndSave`）在用户说话的**同一轮同步**做提取，用关键词命中
- LLM 路在 assistant 回答之后**异步**做精确抽取，处理规则覆盖不到的复杂表达

效果：
- 简单表达（"我叫 X"/"我喜欢 Y"）→ 规则秒提，下一轮就生效
- 复杂表达（"我父亲是医生所以我从小..."）→ LLM 兜底，准确度高

---

### 🎯 召回类

#### Q7：怎么召回记忆？

**标准回答**：

**三种召回策略对应三类槽位**——不同性质的记忆走不同路径：

| 槽位 | 召回方式 | 函数 | 适用场景 |
|---|---|---|---|
| **Profile** | 按 Category 枚举 | `FilterByCategory` | 身份/偏好（必须稳定输出） |
| **Recall** | 向量召回 + 1-hop 图扩展 | `RecallByFilter` | episodic/fact（按相关性） |
| **TaskMem** | ring buffer 取最近 K | `Buffer.Snapshot` | 任务步骤（按顺序） |

**召回主循环**（向量路）：
1. 类别过滤 → 标签过滤 → 年龄过滤
2. 算综合分 `s = sim×0.7 + Importance×0.3`
3. 阈值过滤（默认 `MinScore = 0.4`）
4. 排序 + TopK 截断
5. **顺路刷新 `LastAccessed`**——访问触达即激活

**图扩展**：基于 seed IDs 沿 `FOLLOWS|SIMILAR_TO` 走 1 跳，扩展条目固定打 `Score = 0.45`，能进 prompt 但不压过强相关命中。

#### Q8：为什么不只用向量召回？

**标准回答**：

只用向量有**三个问题**：

1. **Top-K 污染**：用户问"做菜"时，"我叫张三"被排到 K 之外——身份信息丢失
2. **长尾覆盖**：embedding 服务故障时整个召回挂掉
3. **关联发现**：只有相似没有关联——"昨天熬夜了"和"今天没精神"语义不近但事实相关

解决方案：
1. **分类槽位**：身份信息走 `FilterByCategory` 不算相似度
2. **TF 兜底**：embedding 缺失时走词袋（`Tokenize` 中文逐字、英文按词，`textToVector` 算 TF）
3. **图扩展**：向量召回拿 seed，图层沿 `FOLLOWS/SIMILAR_TO` 走 1 跳补充

#### Q9：综合分公式 `sim×0.7 + Importance×0.3` 怎么设计的？

**标准回答**：

权重分配的考虑：

- **相似度（0.7）必须主导**——用户问 X 必须召回 X 的相关信息
- **重要性（0.3）作为次要信号**——相同语义下，越重要越优先；让指数衰减真正影响排序
- **不能让 Importance 主导**——否则老旧高 importance 记忆永远霸榜，新信息进不来

边界考虑：
- 若设 0.5/0.5，重要的不相关记忆会挤出相关的中等重要性记忆
- 若设 0.9/0.1，衰减几乎无效，老记忆和新记忆等价

实际效果：30 天的高 importance 记忆 (0.7) 衰减后约 0.6，进入综合分贡献 0.18；而新近的中等记忆 (0.5) 贡献 0.15——老记忆仍有优势但不会霸榜。

#### Q10：召回的时候用的写锁还是读锁？为什么？

**标准回答**：

**用的写锁**（`mu.Lock()`，不是 `RLock`），**这其实是个权衡**：

为什么必须写锁：
- 召回过程要刷新 `LastAccessed`（行 348）
- TF 兜底走的时候要 `buildVocab(query)` 增量扩词表（行 309）

代价：
- 并发召回串行，QPS 受限

可优化方向：
- 把 `LastAccessed` 异步队列化，召回路径只读不写
- 词表预热到稳态后只读
- 这是**已知短板**，写在文档里诚实交代

---

### 🎯 衰减/合并类

#### Q11：衰减是怎么做的？为什么用指数衰减？

**标准回答**：

**公式**（`Consolidate` Phase 1）：
```go
days := time.Since(item.CreatedAt).Hours() / 24
item.Importance *= math.Pow(DecayRate, days)   // DecayRate = 0.995
```

**为什么指数衰减**：
- **平滑**：30 天 ≈ 86%，100 天 ≈ 61%，不会陡崖
- **可调**：单个系数控制全局节奏
- **生物学合理**：人类遗忘曲线就是指数（Ebbinghaus 1885）

**关键工程细节**：
- 按 `CreatedAt` 算而非"上次衰减时间"——**幂等**，重复跑不累积错误
- `Δ ≥ 0.01` 才入 `DecayUpdates` 落 PG——控制写放大，配合 `unnest` 单条 SQL 批量更新

#### Q12：什么时候记忆会被淘汰？

**标准回答**：

**双门槛 AND 条件**（Phase 3）：
```go
if days > TTLDays(30) AND Importance < MinImportance(0.3):
    delete
```

两个门槛**必须同时满足**：

- **只过 TTL 不删**——重要的老记忆继续保留（"用户姓名"哪怕一年前提到，重要性 0.9，不删）
- **只低 importance 不删**——新记忆给改善的机会
- **两者同时**——既老又不重要，才是真正可淘汰的

这是真正的设计意图：**TTL 不是单方面"到期就删"**，而是"老 + 不重要"的复合判定。

#### Q13：合并和去重的区别是什么？

**标准回答**：

**双阈值分流**：

| 区间 | 动作 | 处理 |
|---|---|---|
| `sim ≥ 0.95` | 去重 | 删除 importance 低的，保留 importance 高的 |
| `0.80 ≤ sim < 0.95` | 合并 | mergeItems：保留较长内容、加权平均 embedding、取 max importance |
| `< 0.80` | 不动 | 两条独立保留 |

**合并细节**（`mergeItems`）：
- 主体：`Importance` 高的为 base
- 内容：子串关系取长，否则 `；` 拼接
- Embedding：**按 Importance 加权平均**——保留语义连续性
- LastAccessed：刷新到 now

**为什么不调 LLM 合并**：
- 确定性、低延迟、可单测
- LLM 改写在大规模下成本爆炸
- 缺点是长期会出现 "用户偏好咖啡；用户喜欢拿铁；用户早上喝咖啡" 累积——可演进为 LLM rewriter

#### Q14：合并是异步触发的，会不会有并发问题？

**标准回答**：

**触发器**：`storeCount ≥ TriggerInterval(5)`，由 `finalize` 在 `goSafe` 里异步跑。

**并发安全保证**：
1. `Consolidate` 整段持 `mu.Lock()` 写锁——执行期间不接受新写入和召回
2. `LongTerm` 内部所有公开方法都持锁
3. PG 写入用事务（`Delete`、`UpdateImportanceBatch`）

**潜在问题**：
- 合并执行期间用户请求会等锁——但 Consolidate 异步触发不阻塞主链路
- 合并较慢时多次触发可能堆积——`storeCount = 0` 在 `Consolidate` 开头清零，下次至少要等 5 条新增

#### Q15：衰减结果会持久化吗？重启后会不会"还原"？

**标准回答**：

**会持久化**。这是项目最近修复的一个问题：

- **修复前**：Phase 1 衰减只改内存的 `m.Items[i].Importance`，没进 `result.UpdateInDB`，重启后 PG 拉回的还是原始 importance——**丢精度**
- **修复后**：
  - 加 `result.DecayUpdates []DecayUpdate` 字段
  - Phase 1 末尾把 `Δ ≥ 0.01` 的条目入列
  - `syncConsolidationToDB` 调 `UpdateImportanceBatch`
  - PG 端用 `unnest($1::BIGINT[], $2::DOUBLE PRECISION[])` 单条 SQL 批量更新

**写放大控制**：
- 0.995 日衰减系数下，每天只衰减 0.5%
- `Δ ≥ 0.01` 阈值意味着新记忆约 2 天才会落库一次
- 1k 条记忆 → 大概几百次更新合并成 1 条 SQL

---

### 🎯 图层类

#### Q16：为什么要引入图记忆？

**标准回答**：

**两个能力是纯向量召回做不到的**：

1. **关联召回**：发现"间接相关"的记忆
   - 用户问"上次咖啡"，向量召回拿到"喜欢咖啡"
   - 图层补回时序相邻的"昨晚熬夜了"
   - 这种"主动联想"对 episodic 推理价值极大

2. **拓扑级保护**：
   - 入度高的节点是"语义骨干"——很多其他记忆都关联到它
   - 衰减/合并时即使 importance 降低也不应淘汰
   - 这是纯字段层面看不出来的

**实现**：
- 节点：`(:Memory {mem_id, content, importance})`
- 边：`FOLLOWS`（时序）+ `SIMILAR_TO`（自动建）+ `CAUSES`/`BELONGS_TO`（预留）
- 召回：先向量拿 seed，再 1-hop 扩展
- 保护：合并前查入度，撤回删除决定

#### Q17：图保护是什么？怎么保证一致性？

**标准回答**：

**问题**：合并时入度 ≥3 的高中心度节点应该免于淘汰。

**最初的 Bug**：
```
旧方案：
  ltm.Consolidate()         ← 已物理删除 m.Items
       ↓
  GraphAware 后处理         ← 才查入度做保护
       ↓
  从 DeleteFromDB 移除保护节点
```
**问题**：保护节点已经从内存 `m.Items` 删了，PG 又被保留——**内存/PG 不一致**。

**修复方案**：把保护逻辑提前到 LTM 内部。

`ConsolidationConfig` 加钩子：
```go
ProtectFn func(candidates []int) (protected []int)
```

`Consolidate` 加 Phase 4：
```go
if cfg.ProtectFn != nil && len(result.DeleteFromDB) > 0 {
    protected := cfg.ProtectFn(result.DeleteFromDB)
    // 1) 撤回 removed 标记
    // 2) 同步剔除 DeleteFromDB
    // 3) 计数回退
}
```

GraphMemory 注入钩子：
```go
gm.protectOnce.Do(func() {
    cfg.ProtectFn = func(ids []int) []int {
        return gm.getHighCentralityMemoryIDs(ids, 3)
    }
})
```

**修复后保证**：
- **原子一致**：保护决策与物理删除发生在**同一个 `mu.Lock()` 临界区**
- **三处对齐**：保护节点既不从 `m.Items` 删除、也不进 `DeleteFromDB`、Neo4j 节点保留
- **解耦**：LongTerm 不知道"图"的存在，钩子是策略注入
- **幂等**：`sync.Once` 保证多次调用只挂一次钩子

#### Q18：图边是怎么建的？写入时建图会不会拖慢主链路？

**标准回答**：

**两类边自动建**（写入时）：
- `FOLLOWS`：上一条记忆 → 当前记忆
- `SIMILAR_TO`：扫最近 50 条，对 `Cosine ≥ simThresh` 的建边

**异步建图**：
```go
goSafe("graphmem.store-node", func() {
    gm.upsertMemoryNode(newID, content, importance)
    if gm.prevID >= 0 {
        gm.addMemoryEdge(gm.prevID, newID, "FOLLOWS", 1.0)
    }
    gm.linkSimilarEdges(newItem, newID)
})
```

**保护机制**：
- `goSafe` 包裹——Neo4j 断连时驱动可能 panic，捕获 recover 防进程崩
- 限 50 条扫描——不是全表，时序相邻的相似性最高
- 异步——不阻塞主链路

**降级**：`neoAvailable()` 不可用时直接退回纯 LongTerm，不影响功能。

---

### 🎯 装配类（promptctx）

#### Q19：记忆怎么进 prompt？

**标准回答**：

**Schema-driven 装配**（promptctx）：

1. **6 个认知槽位**：Constraints / Planner / TaskMem / ToolState / Profile / Recall
2. **4 个 Mode Schema**：chat/tool/rag/react，决定哪些槽位必填、过滤参数、TokenBudget
3. **并发装配**：每个槽位一个 goroutine，wall-clock = max 而非 sum
4. **双层预算**：
   - 单槽位 TokenBudget → `trimByBudget` 硬截
   - 全局 2400 字符 → 按 `slotPriority` 倒序裁

**优先级表**：
```
Constraints (0)  ◄── 永不被裁
Planner     (1)
TaskMem     (2)
ToolState   (3)
Profile     (4)
Recall      (5)  ◄── 第一个被砍
```

**好处**：
- 任何预算压力下，硬性安全约束（"禁止 rm -rf"）永远在 prompt 里
- Recall 是锦上添花，砍了不致命
- 这是普通 `string +` 拼接做不到的

#### Q20：为什么要分 4 个 Mode？

**标准回答**：

**不同任务类型对上下文的需求差异巨大**：

| Mode | 必填槽位 | 场景 |
|---|---|---|
| chat | — | 闲聊，profile + recall 够用 |
| tool | ToolState | 单工具调用，需要工具描述和近期调用 |
| rag | — | 知识库问答，主要靠检索内容，记忆是辅助 |
| react | Constraints + Planner + ToolState | 多步推理，需要完整状态机 |

**反例**：如果一套 Schema 走天下：
- chat 模式塞了一堆 ToolState 浪费 token
- react 模式可能漏 Constraints 导致 LLM 越权操作
- rag 模式过度召回干扰检索结果

**实现**：通过 `RuntimeContextSchema` 声明，每个 Mode 不同的 `Slots[]` + `SlotFilter`。未知 Mode fallback 到 chat。

#### Q21：6 个槽位是怎么定义的？为什么是 6 个不是 5 个或 7 个？

**标准回答**：

**6 个槽位是按"认知功能"切分**，不是按"数据类型"：

- **Constraints**：硬性安全边界（沙箱政策）—— 必须存在，最高优先级
- **Planner**：任务规划状态 —— ReAct 任务的状态机
- **TaskMem**：当前任务的步骤观察 —— episodic 但极短期
- **ToolState**：可用工具 + 近期调用 —— 工具决策依据
- **Profile**：用户画像 —— 个性化基础
- **Recall**：兜底语义召回 —— 长期事实

为什么不是别的数：
- 少于 6：合并后某些槽位职责模糊（比如把 TaskMem 塞进 Recall，污染长期召回）
- 多于 6：过细的槽位反而难以维护
- 这 6 个对应 agent 的核心认知需求：**约束、规划、近期记忆、工具、用户、远期记忆**

**可扩展**：通过 `SlotKind` 枚举 + `ContextSource` 接口，新增槽位只要实现接口。

---

### 🎯 工程实践类

#### Q22：并发安全怎么处理？

**标准回答**：

**每个组件内部持锁**：

| 组件 | 锁策略 |
|---|---|
| ShortTerm | `sync.RWMutex`，所有方法持锁 |
| Preference | `sync.RWMutex`，所有方法持锁 |
| LongTerm | `sync.RWMutex`，召回也持写锁（要刷 LastAccessed） |
| GraphMemory | 无独立锁，复用 LTM 锁 + Neo4j 客户端线程安全 |
| TaskMemBuffer | `sync.RWMutex` |

**Snapshot 模式**：暴露 `Snapshot()` 返回副本，避免调用方持锁遍历：
```go
func (m *LongTerm) Snapshot() []Item {
    m.mu.RLock(); defer m.mu.RUnlock()
    cp := make([]Item, len(m.Items))
    copy(cp, m.Items)
    return cp
}
```

**装配并发**：`ContextAssembler.Assemble` 用 goroutine 并发填槽，`sync.WaitGroup + Mutex` 同步结果。

**异步任务安全**：所有 `goSafe` 包裹的 goroutine 都有 panic recover，避免 Neo4j 断连等异常导致进程崩。

#### Q23：持久化方案？重启后怎么恢复？

**标准回答**：

**分层持久化**：
- **PostgreSQL**：偏好、长期记忆、聊天历史、RAG chunks
- **Neo4j**：图节点和边
- **内存**：短期窗口、任务步骤缓冲（不持久化）

**启动恢复链路**（`restoreFromDB`）：
```go
prefs := repos.pref.Load("default")    → mem.pref.SaveBatch
rows := repos.ltm.Load()                → mem.ltm.StoreItem (保留原 ID)
history := repos.chat.Load(N)           → mem.stm.Add (回放)
```

**ID 一致性保证**：
- `StoreItem` 保留 PG 自增 ID
- `m.nextID = max(row.ID) + 1`，避免新写入冲突
- Neo4j 节点用同一 `mem_id`，启动后 `gm.SyncPrevID()` 对齐

**短期记忆为什么不持久化**：
- 短期 = 最近 N 轮，从 PG 的 `chat_history` 回放即可
- 单一数据源（chat_history）避免双写一致性问题

#### Q24：降级方案？

**标准回答**：

**每一层都有兜底**——这是和实验性框架最大的差别：

| 依赖 | 故障 | 降级 |
|---|---|---|
| Embedding 服务 | 不可用 | 切 TF 词袋（中文逐字、英文按词） |
| Neo4j | 不可用 | GraphMemory 退化为纯 LongTerm |
| LLM 分类 | JSON 失败 | 兜底 `general` |
| LLM 抽取 | 解析失败 | 跳过本轮记忆写入 |
| PostgreSQL | 未连 | 内存继续运行，重启丢失 |

**核心原则**：
- 单点故障不致命
- 功能可降级但不能崩溃
- 关键路径都有 panic recover（`goSafe`）

#### Q25：这套系统的性能瓶颈在哪？

**标准回答**：

**已知瓶颈**：

1. **去重 O(n²) 扫描**——`StoreClassified` 每次写入扫全表
   - 万级规模会慢
   - 演进：上 ANN/HNSW 或 pgvector

2. **Consolidate O(n²) 两两比较**——同上

3. **召回持写锁**——并发召回串行
   - 演进：`LastAccessed` 异步队列化，召回只读

4. **写入即调 LLM 抽取**——每轮回答后异步抽取调一次 LLM
   - 已经是异步，不阻塞用户响应
   - 演进：批量抽取多轮一起做

5. **Neo4j 写入**——每条记忆 3-5 次 Cypher 调用
   - 已经异步
   - 演进：批量写、连接池

**实测下界**：
- 千级条目，召回 < 50ms
- 万级条目，召回 200ms+，需要优化

---

### 🎯 对比类

#### Q26：和 mem0 有什么区别？

**标准回答**：

**核心差异**：

| 维度 | mem0 | 本项目 |
|---|---|---|
| 写入决策 | LLM 决定 ADD/UPDATE/DELETE | 阈值 + 字段补丁（确定性） |
| 合并 | LLM 改写新内容 | 加权平均 + 字符串拼 |
| 衰减 | **无原生衰减** | 指数衰减 + TTL + MinImportance |
| 图层 | mem0g 实体抽取 | 结构性边 + 中心度合并保护 |
| 召回 | 向量 + user_id 隔离 | 向量 + TF + SlotFilter + 1-hop 图扩展 |
| Prompt 组织 | 召回结果直接拼 | 6 认知槽位 + 双层预算 + 安全优先级 |

**取舍**：
- mem0 把记忆当"LLM 管理的数据库"——灵活但慢且贵，所有判断走 LLM
- 本项目把记忆当"工作记忆模型 + 可调存储引擎"——确定性、低延迟、可单测、可调参
- 代价是阈值调参，对"语义相同字面不同"的容忍度比 LLM 弱

#### Q27：和 Viking 框架有什么关系？

**标准回答**：

**Viking 解决的是"上下文如何组织"**，本项目的 promptctx 借鉴了它的思想：
- runtime state / planner state / task memory / tool state / context assembly
- 把 prompt 当作运行时状态机而非字符串拼接

**本项目对 Viking 的扩展**：
- 在 6 槽位之外补了**双层预算 + 优先级裁剪**
- Constraints 永不丢的安全语义
- Provider 反向注入（promptctx 不依赖 agent 包，DDD 反向依赖）

**整体定位**：用 mem0 思想解决"怎么存记忆"（fact extraction、dedup、entity graph、semantic retrieval），用 Viking 思想解决"怎么组织上下文"（runtime state、planner state、task memory、tool state、context assembly）。

---

### 🎯 短板/拷打类

#### Q28：你这套系统有什么问题？

**标准回答**（**主动暴露而非被动承认**）：

1. **去重/合并 O(n²)**——万级规模需要 ANN
2. **召回持写锁**——并发受限，可异步队列化 LastAccessed
3. **合并是字符串拼接**——长期演进会累积，可上 LLM rewriter
4. **STM 没摘要压缩**——超长对话信息丢失，可加 summarizer
5. **ProtectFn 钩子目前只挂图保护**——可扩展更多策略
6. **阈值调参**——没有自动调优机制

**最近修过的两个**：
- 衰减不落 PG（重启后还原）→ 加 `DecayUpdates` + 批量 `unnest` 落库
- 图保护内存/PG 不一致 → 提前到 LTM 内部 `ProtectFn` 钩子

**态度**：知道问题在哪、知道怎么演进，比假装完美重要。

#### Q29：如果让你重新设计，你会改什么？

**标准回答**：

会改的：
1. **存储后端换 pgvector**——内置向量索引，去掉 O(n²) 扫描
2. **召回路径无锁化**——LastAccessed 改异步事件
3. **合并引入 LLM rewriter**——但加 token 预算控制
4. **STM 三段式**——最近 N 轮原文 + K 个摘要片段 + 跨会话事实

不会改的：
1. **四阶段管线**结构——清晰、可测、可演进
2. **写入即分类**——这是召回精准的根本
3. **6 槽位 + 双层预算**——viking 思想的工程化是核心创新点
4. **ProtectFn 钩子模式**——保护决策与物理删除原子一致

**核心立场**：架构对了，性能问题都是优化；架构错了，性能再好也是技术债。

#### Q30：怎么测试这套系统？

**标准回答**：

**分层测试**：

1. **单元测试**：
   - LongTerm：去重阈值、合并逻辑、衰减幂等性、过期双门槛
   - GraphMemory：ProtectFn 撤回、计数回退顺序
   - Preference：规则提取边界、并发写
   - promptctx：每个 source 独立测、Schema 装配、预算裁剪

2. **集成测试**：
   - 写入 → 召回 → 装配 端到端
   - LLM 失败时的降级路径
   - PG/Neo4j 不可用时的兜底

3. **回归测试**：
   - 启动恢复后 ID 一致性
   - Consolidate 触发后内存/PG/Neo4j 三处对齐
   - 长跑测试：连续 1000 条写入后召回准确率

4. **性能测试**：
   - 写入吞吐（含去重）
   - 召回延迟（含图扩展）
   - Consolidate 执行时间随 n 增长曲线

**当前覆盖**：promptctx 包有完整单测（profile/recall/constraints/assembler），longterm/graphmem 单测待补——这是已知短板。

---

## 附录：一句话总结

> 这套记忆系统的核心不是"存了什么"，而是"在每一阶段如何做选择"——
>
> **写入时按认知意图分类，召回时按声明式过滤，合并时让确定性管线 + 衰减自然遗忘，装配时让安全约束在任何预算下永不丢失**。
>
> 它的优雅之处不在某个组件，而在于这四阶段相互咬合：分类让召回能精准，召回让 importance 有意义，importance 让衰减有方向，衰减让装配的预算永远花在最值得的事实上。
>
> 一句话——**记忆系统不是数据库，是 agent 的认知作业系统**。
