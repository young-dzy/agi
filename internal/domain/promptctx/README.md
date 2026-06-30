# promptctx — Schema-driven Runtime Context Assembly

> 每轮 LLM 推理前，把"喂给模型的 System Prompt 前缀"按 Mode 编排、并发装配、预算裁剪后产出。
> 它不是一个"字符串拼接工具"，而是 agent 的 **Prompt 编排引擎 / 认知中间件**。

---

## 1. 模块定位

```
用户输入 ──► agent ──► ContextAssembler.Assemble(Query{Mode})
                                │
                                ├─ 1. 选 Schema（chat / tool / rag / react）
                                ├─ 2. 并发调用 6 类 ContextSource 填充槽位
                                ├─ 3. 单槽位 TokenBudget 裁剪
                                └─ 4. 全局 budget 按 slotPriority 倒序裁剪
                                ▼
                  RuntimeContext.Render() ──► System Prompt 前缀 ──► LLM
```

它是 agent 与下列领域模块之间的**汇聚点**：

| 上游领域 | 接入方式 | 服务的认知槽位 |
|---|---|---|
| `memory/preference` | `ProfileSource` | Profile |
| `memory/longterm` | `ProfileSource` / `RecallSource` | Profile / Recall |
| `memory/graph`（可选） | 实现 `Recaller` 接口 | Recall |
| `agent`（任务状态） | `PlannerProvider` 函数注入 | Planner |
| `agent`（工具注册） | `ToolRegistryProvider` 函数注入 | ToolState |
| `tool` 调用历史 | `ToolStateTracker` | ToolState |
| 任务步骤观察 | `TaskMemBuffer`（ring buffer） | TaskMem |
| `sandbox` | `ConstraintsSource` | Constraints |

> ✨ **反向依赖**：`agent` 通过 `Provider` 函数把状态注入 promptctx，而 promptctx **不引用 agent 包**。这是清晰的 DDD 反转。

---

## 2. 核心抽象

### 2.1 `SlotKind`：6 类认知槽位

```
SlotProfile      用户画像（稳定身份 / 偏好）
SlotPlanner      任务规划状态（阶段 / 进度 / 下一步）
SlotTaskMem      当前任务的步骤观察缓冲
SlotToolState    可用工具 + 近期调用记录
SlotConstraints  沙箱政策 / 硬性安全约束
SlotRecall       兜底语义召回（episodic / fact / general）
```

不是按"数据类型"组织 prompt，而是按**认知功能**组织——更接近 **工作记忆模型**，而非消息列表。

### 2.2 `SlotFilter`：声明式过滤约束

```go
type SlotFilter struct {
    Categories  []string // 命中其一即可
    RequireTags []string // 必须全部包含
    MinScore    float64  // 召回综合分阈值
    TopK        int      // 单槽位最多返回项数
    MaxAgeHours int      // 最大年龄
    TokenBudget int      // 单槽位字符预算
}
```

> "想要什么"和"召回什么"在 Schema 层就对齐——避免把 episodic 记忆塞进 identity 槽这类 Top-K 污染。

### 2.3 `RuntimeContextSchema`：Mode 与槽位编排表

| Mode | Constraints | Profile | Planner | TaskMem | ToolState | Recall |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| **chat** | ✓ | ✓ | — | — | — | ✓ |
| **tool** | ✓ | ✓ | — | — | **必填** | ✓ |
| **rag** | ✓ | ✓ | — | — | — | ✓ |
| **react** | **必填** | ✓ | **必填** | ✓ | **必填** | ✓ |

未知 Mode 自动 fallback 到 `chat`。

### 2.4 `ContextSource`：槽位数据提供者

```go
type ContextSource interface {
    ID() string
    Supports(SlotKind) bool
    Fetch(ctx context.Context, slot Slot, q Query) ([]ContextItem, error)
}
```

- 一个 source 可 `Supports` 多个 SlotKind（如 GraphMemory 同时填 Profile/Recall）
- 各 source 独立可测，按 SlotKind 注册到 `SourceRegistry`

---

## 3. 装配流程（`assembler.go`）

```go
func (a *ContextAssembler) Assemble(ctx context.Context, q Query) *RuntimeContext {
    schema := a.schemas[q.Mode]            // ① 选 Schema（fallback chat）
    rc := &RuntimeContext{Schema: schema, Filled: make([]FilledSlot, len(schema.Slots))}

    // ② 并发填充各槽位
    var wg sync.WaitGroup
    for idx, slot := range schema.Slots {
        wg.Add(1)
        go func(idx int, slot Slot) {
            defer wg.Done()
            rc.Filled[idx] = a.fillSlot(ctx, slot, q)   // 内部按 TokenBudget 裁剪
        }(idx, slot)
    }
    wg.Wait()

    // ③ 全局预算超限 → 按 priority 从低到高裁剪
    a.applyGlobalBudget(rc)
    return rc
}
```

### 3.1 双层 Budget 控制

- **单槽位 budget**：source 自治，超额自动截断（`trimByBudget`）
- **全局 budget**：默认 `2400` 字符，超限按 `slotPriority` 倒序裁剪：

```
slotPriority（数字越小越优先保留）：
Constraints (0)  ◄── 永不被裁
Planner     (1)
TaskMem     (2)
ToolState   (3)
Profile     (4)
Recall      (5)  ◄── 第一个被砍
```

> ✨ 这一条保证了**安全约束在任何预算压力下都不会丢**——这是普通字符串拼接做不到的。

---

## 4. 渲染（`context.go`）

`RuntimeContext.Render()` 把 `FilledSlot` 渲染为 zh-CN System Prompt 前缀：

```
【硬性约束】
- [禁止] 不允许执行 rm -rf
- [告警] 网络访问需要审批

【任务规划】
- 任务 t-001 状态=running 阶段=executing
- 进度：第 2/5 步
- 下一步：调用 weather_api（工具=http_get）

【可用工具】
- get_time — 获取当前时间
- weather_api — 查询天气（必填 city）
- 近期调用 weather_api [成功]: {"temp":22}

【用户画像】
- 城市: 北京
- 语言: 中文
- 用户叫张三

【相关回忆】
- 上次问过天气API（重要性=0.70, 综合分=0.82）
```

- `Skipped` 或 `Items` 为空的槽位不渲染
- 顺序由 Schema 决定（Schema 顺序即渲染顺序）

---

## 5. 与"普通字符串拼接"的差异

| 维度 | 普通做法 | promptctx |
|---|---|---|
| 组织方式 | 按数据类型（history/docs/tools） | 按**认知槽位**（profile/planner/...） |
| Mode 区分 | 一套 prompt 走天下 | Schema 驱动，4 Mode 各取所需 |
| 召回策略 | Top-K 全塞 | `SlotFilter` 声明式过滤 |
| 预算控制 | 估个总长度截断 | **双层 budget + 优先级裁剪** |
| 安全约束 | 可能被截断丢失 | Constraints 优先级 0，永不丢 |
| 数据获取 | 串行 | **goroutine 并发** |
| 数据源耦合 | 直接 import 各模块 | `Provider` 函数反向注入 |
| 可换插 | 难 | `Recaller` 接口，LongTerm/GraphMemory 热插拔 |
| 可测试性 | 拼好的字符串难断言 | 每个 source 独立单测 |

一句话：
> **普通 agent 是 `string +`，promptctx 是一套微型 Prompt 编排引擎。**

---

## 6. 典型用法

### 6.1 装配阶段（agent 启动时一次）

```go
reg := promptctx.NewSourceRegistry()
reg.Register(promptctx.NewProfileSource(pref, ltm))
reg.Register(promptctx.NewConstraintsSource(sandbox.PolicySnapshot()))
reg.Register(promptctx.NewRecallSource(ltm))   // 或 graphMem，实现 Recaller 即可
reg.Register(promptctx.NewToolStateSource(toolRegistry, toolTracker))
reg.Register(promptctx.NewTaskMemSource(taskMemBuf))
reg.Register(promptctx.NewPlannerSource(plannerProvider))

asm := promptctx.NewAssembler(promptctx.DefaultSchemas(), reg)
```

### 6.2 推理前（每轮调用）

```go
rc := asm.Assemble(ctx, promptctx.Query{
    Mode:      "react",
    Text:      userInput,
    Embedding: queryEmb,
    TaskID:    currentTaskID,
})

systemPrompt := rc.Render() + "\n\n" + baseSystemPrompt
llm.Complete(systemPrompt, userInput)
```

### 6.3 步骤反馈（agent 每执行完一步）

```go
taskMemBuf.Push(promptctx.StepObservation{
    StepID: 2, ToolName: "weather_api",
    Result: "{\"temp\":22}", Success: true,
})
toolTracker.Record(promptctx.ToolCallTrace{
    ToolName: "weather_api", Success: true, Summary: `{"temp":22}`,
})
```

下一轮 `Assemble` 时，这些观察会自动出现在 TaskMem / ToolState 槽位。

---

## 7. 文件导航

| 文件 | 职责 |
|---|---|
| `slot.go` | `SlotKind` / `SlotFilter` / `Slot` / `ContextItem` / `FilledSlot` 定义 |
| `schema.go` | 4 个内置 Schema + `slotPriority` |
| `source.go` | `Query` / `ContextSource` 接口 |
| `assembler.go` | `SourceRegistry` + `ContextAssembler.Assemble` |
| `context.go` | `RuntimeContext.Render()` 渲染层 |
| `source_profile.go` | Profile 槽 ← Preference + LTM(identity/preference) |
| `source_planner.go` | Planner 槽 ← `PlannerProvider` |
| `source_taskmem.go` | TaskMem 槽 ← `TaskMemBuffer` ring buffer |
| `source_tools.go` | ToolState 槽 ← `ToolRegistryProvider` + `ToolStateTracker` |
| `source_constraints.go` | Constraints 槽 ← `sandbox.PolicySnapshot()` |
| `source_recall.go` | Recall 槽 ← 任意实现 `Recaller` 的存储 |
| `assembler_test.go` 等 | 各 source 与装配流程的单测 |

**学习入口建议**：
1. `schema.go` —— 看 4 个 Mode 的槽位编排，理解"为什么 react 必填 Constraints / Planner / ToolState"
2. `assembler.go` —— 看 `Assemble` 流程：选 Schema → 并发填充 → 双层 budget
3. `source_*.go` —— 各槽位数据来源，挑感兴趣的看
4. `context.go` —— 最后看渲染层，得到 prompt 前缀

---

## 8. 设计要点小结

1. **Schema-driven**：把"如何拼 prompt"声明化为 Schema，避免命令式硬编码
2. **认知槽位抽象**：按功能而非数据类型组织 prompt
3. **双层 Budget**：单槽位自治 + 全局 priority 裁剪，安全约束永不丢
4. **并发装配**：`max(source 耗时)` 而非 `sum`
5. **Source 多对多**：一个 source 可服务多个 SlotKind
6. **Provider 函数反向注入**：promptctx 不依赖 agent
7. **Required + 占位 + fallback**：未知 Mode 不崩、空槽位可 debug
8. **可换插**：实现 `Recaller` 接口即可替换底层记忆存储