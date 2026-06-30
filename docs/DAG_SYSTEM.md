# 任务 DAG 设计与面试解析

> **面向读者**：希望理解项目 ReAct 任务图（DAG）整体设计，并能从容回答相关面试问题的工程师。
>
> **阅读路径**：第一部分通读，建立整体认知；第二部分按问题索引，针对性深挖。
>
> **配套阅读**：[记忆系统设计](./MEMORY_SYSTEM.md)——DAG 中每个节点的执行结果会被记入 TaskMemBuffer 和 LTM。

---

## 第一部分：整体链路（5 分钟看清全貌）

### 1.1 DAG 在 Agent 中处于什么位置

```
              ┌─────────────────────────────┐
              │   ChatOptions / 路由决策     │
              └──────────────┬──────────────┘
                             ▼
            ┌───────────────────────────────┐
            │ 4 个 Mode：chat / tool / rag / react │
            └────────────────┬──────────────┘
                             ▼
                      ┌──────────────┐
                      │  ReAct Mode  │ ◄── DAG 在这里登场
                      └──────┬───────┘
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
            Planner       GraphRuntime  Generator
            LLM 规划      并行+竞速调度  LLM 合成
                │            │            │
                ▼            ▼            ▼
          一组 planNode   Topological     最终答案
          (带依赖+竞速)   级并行执行     综合所有观察
```

DAG **只服务于 ReAct 模式**——多步推理 + 多工具协作的场景。其他三个模式（chat/tool/rag）是单步或线性流程，不走 DAG。

### 1.2 三个核心抽象

| 抽象 | 代码 | 职责 |
|---|---|---|
| **Node** | `internal/domain/graph/graph.go:40-54` | 单个执行单元（工具调用 / 子 Agent / 思考 / 聚合） |
| **TaskGraph** | `internal/domain/graph/graph.go:59-64` | 有向无环图，带邻接表 + 入度表 + 拓扑层缓存 |
| **GraphRuntime** | `internal/application/chat/runtime_graph.go:67-78` | 调度器：拓扑分层 + 信号量并发 + 竞速执行 |

### 1.3 Node 的关键字段

```go
type Node struct {
    ID         NodeID            // 节点唯一标识
    Type       NodeType          // tool / sub_agent / think / aggregate
    Name       string            // Planner 给出的 reason（人类可读）
    ToolName   string            // tool 节点必填
    AgentName  string            // sub_agent 节点必填
    Goal       string            // 子 Agent 的任务目标
    Params     map[string]string // 工具参数
    DependsOn  []NodeID          // 入边：依赖哪些节点
    RaceGroup  string            // 空=独立；同 group=竞速
    Status     NodeStatus        // pending/running/done/failed/skipped/cancelled
    Result     string
    Error      string
    RetryCount int
}
```

**两个超出常规 DAG 的字段**：
- **`RaceGroup`**：同组节点**竞速**执行，首个成功的获胜，其余被取消
- **`AgentName`** + **`Goal`**：节点可以是**子 Agent**，不只是工具

### 1.4 三阶段执行链路

```
┌────────────────────────────────────────────────────────────┐
│  Stage 1：Planning（Planner LLM 规划）                       │
│  query + 工具集 + 子 Agent → JSON 节点数组（含依赖和竞速）    │
│  失败降级：rulePlanNodes 关键词规则                          │
└──────────────────────────┬─────────────────────────────────┘
                           ▼
┌────────────────────────────────────────────────────────────┐
│  Stage 2：Build & Validate（构图）                          │
│  NewTaskGraph 计算邻接表 / 入度                              │
│  Validate 检测环 + 悬空依赖                                  │
│  校验失败降级：DependsOn 清空 → 全并行                       │
└──────────────────────────┬─────────────────────────────────┘
                           ▼
┌────────────────────────────────────────────────────────────┐
│  Stage 3：Execute（GraphRuntime 调度）                       │
│  TopologicalLevels Kahn 算法分层                            │
│  每层：按 RaceGroup 分组 → 信号量并发 → 竞速 / 普通          │
│  每节点：状态机 + 重试 + ctx 中断 + TaskMem 写入             │
└──────────────────────────┬─────────────────────────────────┘
                           ▼
                  ┌─────────────────┐
                  │ Generator LLM   │
                  │ 合成最终答案     │
                  └─────────────────┘
```

### 1.5 端到端时序

```
用户提问 → 路由到 ReAct 模式 (mode_react.go:25)
   │
   ▼
llmPlanGraph                      plan_graph.go:66
   ├─ 构造工具描述 + 子 Agent 描述
   ├─ Planner LLM 输出 planNode JSON
   ├─ 失败兜底：解析 legacy 格式 / 关键词规则 rulePlanNodes
   └─ 过滤：只保留实际存在的工具/Agent
   │
   ▼
NewTaskGraph                      graph.go:67
   ├─ 注册节点 → AdjList / InDegree
   └─ Validate (悬空依赖 + 环检测)
   │
   ▼  失败降级：DependsOn=nil 全并行重建
   │
   ▼
GraphRuntime.Execute              runtime_graph.go:99
   │
   ├─ TopologicalLevels (Kahn 算法)
   │  levels[0] = [n1, n2]
   │  levels[1] = [n3]   (依赖 n1, n2)
   │  levels[2] = [n4]   (依赖 n3)
   │
   ├─ 对每一层：
   │   ├─ groupByRace 分组
   │   ├─ 同 RaceGroup → raceGroup goroutine（竞速）
   │   ├─ 独立节点 → executeNode goroutine（普通）
   │   ├─ 信号量 sem 限并发
   │   ├─ wg.Wait 等本层完成
   │   └─ saveSnapshot 持久化
   │
   ├─ executeSingleNode：
   │   ├─ 推送 node_start / thought / action SSE
   │   ├─ run() → 工具 t.Execute 或 子 Agent sa.Run
   │   ├─ 失败重试 maxRetries 次
   │   ├─ TaskMem.Push + ToolTracker.Record
   │   └─ 推送 node_done / observation SSE
   │
   └─ buildResult / buildInterruptedResult
   │
   ▼
llmGenerate                       mode_react.go:187
   └─ 把所有 observations 喂给 Generator LLM
   │
   ▼
最终答案 + ReActStep 列表
```

### 1.6 关键代码地图

| 组件 | 文件 |
|---|---|
| 图领域模型 | `internal/domain/graph/graph.go` |
| 任务规划器 | `internal/application/chat/plan_graph.go` |
| 图运行时 | `internal/application/chat/runtime_graph.go` |
| ReAct 模式入口 | `internal/application/chat/mode_react.go` |
| 子 Agent 注册 | `internal/application/chat/subagents.go` |
| TaskMem 缓冲 | `internal/domain/promptctx/source_taskmem.go` |

---

## 第一部分补充：与 LangGraph 的对比

### A. LangGraph 是什么

LangGraph（LangChain 团队的产品）的核心抽象：
- **StateGraph**：节点是函数，边是条件转移
- **Channel/State**：所有节点共享一个状态对象，通过更新 state 字段触发下游
- **Conditional Edge**：边可以带条件函数，运行时动态决定走哪条
- **持久化**：内置 checkpointer（SQLite/Postgres），任意节点可恢复

**典型用法**：
```python
graph = StateGraph(AgentState)
graph.add_node("plan", plan_fn)
graph.add_node("act", act_fn)
graph.add_node("reflect", reflect_fn)
graph.add_edge("plan", "act")
graph.add_conditional_edges("act", should_continue, {True: "reflect", False: END})
```

### B. 本项目的取舍

| 维度 | LangGraph | 本项目 |
|---|---|---|
| **状态模型** | StateGraph，共享 State 对象 | Node 局部 Result + DependsOn 拉取 |
| **边语义** | 静态边 + 条件边（动态分支） | 静态依赖边 + 拓扑分层 |
| **执行模型** | 单步推进（step-by-step），每步可回放 | 拓扑分层并行 + 同层竞速 |
| **图构建** | 编译期（add_node/add_edge） | **运行时由 LLM 输出 JSON 生成** |
| **循环支持** | 支持回路（while-style） | **强制 DAG，禁止环**（Validate 检测） |
| **持久化** | Checkpointer 自动 | TaskState 手动 saveSnapshot |
| **并发** | 通过 Send/asyncio 显式触发 | **拓扑同层自动并行 + 信号量** |
| **竞速** | 无原生支持 | **RaceGroup 同组竞速，首胜取消其余** |
| **子 Agent** | 子图嵌套 | **节点直接挂 SubAgent**（一等公民） |
| **中断恢复** | Checkpoint 重放 | ctx 取消 + 节点状态保留 + 快照 |
| **降级路径** | 无内置 | LLM 失败 → 规则；校验失败 → 全并行 |

### C. 本项目的独到之处

#### 1. 运行时 DAG 而非编译时 DAG

**LangGraph**：图结构在代码里写死，运行时不变。
```python
graph.add_node("a", ...)
graph.add_edge("a", "b")  # 编译期定义
```

**本项目**：图结构**由 LLM 动态产出**——同一个用户问题，不同的 LLM 输出可能产生完全不同的图。

```go
planNodes := a.llmPlanGraph(ctx, query, ts, memPrefix)
tg := graph.NewTaskGraph(planNodes)
```

**好处**：
- Agent 真正"会规划"——不是按固定流程走
- 工具集变化时 Planner 自动适配，不用改代码
- 用户问"研究 X 写报告"，Planner 自动生成 research → writer → review 三节点链

**代价**：
- 需要降级路径（LLM 解析失败 → 规则 → 全并行）
- 调试比 LangGraph 难——图结构每次都可能不同

#### 2. 同层竞速（RaceGroup）

这是 LangGraph 完全没有的能力。

**场景**：用户问"X 是什么"，可以同时调 `search_web` 和 `rag_search`——谁先返回用谁。

**Planner 输出**：
```json
[
  {"id":"n1","tool":"search_web","race_group":"search","depends_on":[]},
  {"id":"n2","tool":"rag_search","race_group":"search","depends_on":[]}
]
```

**调度细节**（`runtime_graph.go:150-229`）：
```go
ch := make(chan raceResult, len(g.NodeIDs))
raceCtx, cancel := context.WithCancel(ctx)

for _, nodeID := range g.NodeIDs {
    go func(id graph.NodeID) {
        res, err := rt.executeSingleNode(raceCtx, id)
        ch <- raceResult{nodeID: id, result: res, err: err}
    }(nodeID)
}

// 首个成功 → 取消其余
winnerFound := false
for i := 0; i < len(g.NodeIDs); i++ {
    r := <-ch
    if r.err == nil && !winnerFound {
        winnerFound = true
        cancel()                                      // 关键：取消其余
        rt.results[r.nodeID] = r.result
        rt.graph.SetNodeStatus(r.nodeID, graph.StatusDone)
    }
}
```

**好处**：
- **延迟降低**：N 路并发，wall-clock = min(各路延迟)
- **可靠性提升**：单源故障不致命，其他源继续
- **Cypher 级语义**：First-success-wins 比 LangGraph 的"全等"语义更适合 agent 场景

#### 3. 子 Agent 作为一等节点

**LangGraph** 处理子 Agent 的方式：把子图嵌套进父图，本质上还是图。

**本项目**：直接把"调用一个 SubAgent"建模为节点（`NodeTypeSubAgent`）。

```go
const (
    NodeTypeTool      NodeType = "tool"
    NodeTypeSubAgent  NodeType = "sub_agent"
    NodeTypeThink     NodeType = "think"
    NodeTypeAggregate NodeType = "aggregate"
)
```

执行时的统一接口（`runtime_graph.go:279-297`）：
```go
run := func() (string, error) {
    if node.Type == graph.NodeTypeSubAgent {
        sa, ok := rt.agent.subagents.get(node.AgentName)
        return sa.Run(ctx, SubAgentTask{
            ID: string(nodeID), Goal: node.Goal,
            Query: rt.task.Query, Upstream: rt.upstreamResults(node),
        })
    }
    t, ok := rt.tools[node.ToolName]
    return t.Execute(params)
}
```

**好处**：
- **简化心智**：调度器只面对"节点"概念，不关心是工具还是 Agent
- **统一并发**：子 Agent 也能加入 RaceGroup、可以并行
- **Upstream 传递**：节点的依赖结果（`upstreamResults`）自动注入子 Agent 任务

**例子**：用户问"研究 React 18 写一份报告并保存到知识库"——Planner 生成：
```
n1 [sub_agent: research_agent]  ← 研究
n2 [sub_agent: writer_agent]    ← 写报告，depends_on: [n1]
n3 [sub_agent: review_agent]    ← 审查，depends_on: [n2]
n4 [sub_agent: doc_agent]       ← 保存到 RAG，depends_on: [n2, n3]
```

#### 4. 拓扑分层 vs 单步推进

**LangGraph**：每次推进一个"step"，状态机驱动，需要 conditional edge 决定下一步。
- 优点：可控性强、便于回放
- 缺点：并发需要手动 Send，复杂度上来

**本项目**：Kahn 算法分层，**同层自动并行**。
```go
levels[0] = [n1, n2]    // 入度=0，可立即执行
levels[1] = [n3]        // 入度=2（依赖 n1, n2），等 L0 完成
levels[2] = [n4]        // 入度=1（依赖 n3）
```

**调度伪代码**（`runtime_graph.go:114-143`）：
```go
for levelIdx, level := range levels {
    groups := rt.groupByRace(level)
    var wg sync.WaitGroup
    for _, g := range groups {
        if g.RaceGroup != "" {
            wg.Add(1)
            go rt.raceGroup(ctx, g, &wg)
        } else {
            for _, nodeID := range g.NodeIDs {
                wg.Add(1)
                go rt.executeNode(ctx, nodeID, &wg)
            }
        }
    }
    wg.Wait()
}
```

**好处**：
- **自动并行**：开发者不用思考"哪些可以并发"
- **延迟下界**：wall-clock = sum(各层最长节点) 而非 sum(全部节点)
- **简单确定**：一层完成才进下一层，状态清晰

**代价**：
- 不支持环（while 循环）—— 但 Agent 场景大部分是 DAG
- 同层最慢节点决定该层延迟

#### 5. 多层降级 + 异常路径

**降级矩阵**：

| 故障 | 降级行为 | 代码位置 |
|---|---|---|
| LLM 不可用 | rulePlanNodes 关键词规则 | `plan_graph.go:71` |
| Planner JSON 解析失败 | 尝试 legacy 格式 → function-calling 格式 → 规则 | `plan_graph.go:145-182` |
| 工具不存在 | 过滤掉该节点 | `plan_graph.go:199` |
| 图校验失败（环/悬空） | DependsOn 清空 → 全并行 | `mode_react.go:55-61` |
| 节点执行失败 | maxRetries 重试 | `runtime_graph.go:304-317` |
| 竞速全部失败 | 标记全部 Failed + lastErr | `runtime_graph.go:218-228` |
| 用户中断 ctx.Done | 标记 pending/running 为 cancelled | `runtime_graph.go:436-441` |

每一层都有兜底——LangGraph 把这些都留给业务代码。

#### 6. SSE 流式可视化

整个 DAG 执行过程通过 SSE 推送 7 类事件，前端可实时绘制：

| 事件 | 触发时机 |
|---|---|
| `graph_ready` | 拓扑分层完成 |
| `node_start` | 节点开始执行 |
| `step` (thought) | 推送节点意图 |
| `step` (action) | 推送工具调用 |
| `step` (observation) | 推送执行结果 |
| `race_won` | 竞速组胜出 |
| `node_done` | 节点完成 |

LangGraph 需要业务代码自己实现这种可视化。

---

## 第二部分：面试 Q&A 实战

### 🎯 整体设计类

#### Q1：讲讲你这个项目的任务图（DAG）是怎么设计的？

**标准回答**：

DAG 只服务于 **ReAct 模式**（多步推理 + 多工具协作场景），核心是把"串行 ReAct"升级为"图调度"。

三个核心抽象：

1. **Node**——执行单元，可以是工具、子 Agent、思考、聚合；带 `DependsOn` 依赖边和 `RaceGroup` 竞速组
2. **TaskGraph**——DAG 容器，构建时计算邻接表 + 入度，运行前 `Validate` 检测环和悬空依赖
3. **GraphRuntime**——拓扑分层（Kahn 算法）+ 信号量并发 + 竞速调度（First-success-wins）

执行三阶段：
- **Planning**：Planner LLM 输出 JSON 节点数组（含依赖和竞速），降级到规则
- **Build & Validate**：构图 + 校验，失败降级为全并行
- **Execute**：层内并行 + 竞速 + 信号量 + ctx 取消 + 重试

#### Q2：为什么要做 DAG 而不是简单的 ReAct 循环？

**标准回答**：

传统 ReAct 是**严格串行**：think → act → observe → think → ...，每步等待上一步。

但实际任务往往**天然并行**：
- 用户问"对比北京和上海的天气" → 两次 `get_weather` 可以并发
- 用户问"X 是什么" → 可以同时调 `search_web` 和 `rag_search` 竞速取最快
- 用户问"研究 X 写报告" → 研究和审查可以并行，写报告等研究

DAG 把这些自然并行揭示出来：
- **延迟下降**：wall-clock = max(各路) 而非 sum
- **可靠性提升**：竞速场景下单源故障不致命
- **可解释性**：图结构可视化、可回放、可中断

代价是引入了图调度复杂度，但通过 `levels + 信号量 + RaceGroup` 三个机制控制住了。

---

### 🎯 与 LangGraph 对比类

#### Q3：你这个 DAG 和 LangGraph 有什么区别？

**标准回答**：

**核心差异在四个维度**：

1. **图构建时机**：LangGraph 编译时定义（add_node/add_edge 写死），本项目**运行时由 Planner LLM 动态生成**——Agent 真正会规划
2. **执行模型**：LangGraph 单步推进 + Send/asyncio 手动并发；本项目**拓扑分层自动并行**
3. **竞速机制**：LangGraph 无原生支持；本项目 `RaceGroup` 同组节点竞速取最快
4. **循环支持**：LangGraph 支持回路（while-style）；本项目**强制 DAG**，环检测直接拒绝

**取舍**：
- LangGraph 是通用的"状态机图"，灵活但需要业务代码处理并发/竞速/降级
- 本项目是"Agent 专用调度器"，针对 Agent 场景做了 opinionated 选择（DAG only + 自动并行 + 竞速 + 多层降级）

#### Q4：LangGraph 支持循环你为什么不支持？

**标准回答**：

**两个原因**：

1. **Agent 场景大部分是 DAG**：用户问"做 X" → 规划 → 执行 → 合成答案，这是天然的有向无环结构。需要循环的场景（self-reflection、迭代优化）可以通过"上层重试"实现，不必在调度器层支持

2. **DAG 让调度简单且强类型**：
   - Kahn 拓扑分层 → 自动决定并发
   - 入度=0 即就绪 → 简单状态机
   - 环检测 → `Validate` 直接拒绝错误图
   - 如果支持环，需要引入"步数上限"、"状态收敛判定"等机制，复杂度指数级上升

如果未来需要循环（比如 self-correcting agent），更倾向"在外层包装重试循环"——`for { result := executeGraph(); if !needRetry(result) { break } }`，而不是改调度器。

#### Q5：LangGraph 有 Checkpointer 自动持久化，你怎么做？

**标准回答**：

本项目用 **TaskState + 手动 saveSnapshot** 方式：

```go
task := &TaskState{
    TaskID: ..., Query: query,
    Status: "running", Phase: "executing",
    Steps: taskSteps, Graph: tg,
}
a.setTask(task)
a.saveSnapshot(task)   // 每层执行完后再调一次
```

调用点（`runtime_graph.go:142`）：每一层执行完后 `saveSnapshot`，保证中间状态被持久化。

**取舍**：
- 比 LangGraph Checkpointer 简单——只在层边界存
- 不能恢复到"节点中间执行的某个状态"，但 Agent 场景节点级别已经足够
- 中断恢复通过：ctx.Done → 标记 pending/running 为 cancelled → buildInterruptedResult 返回当前进度

**自定义点**：未来要做更细粒度恢复，可以加节点级 checkpoint hook。

#### Q6：LangGraph 的条件边（conditional edge）你怎么实现？

**标准回答**：

本项目**没有条件边**，但通过**两种方式覆盖同类需求**：

1. **Planner 阶段已决策**：Planner LLM 看到全部上下文后产出图，本身就是"动态规划"的结果。如果需要"看到 A 结果再决定调 B 还是 C"，可以在 Planner 输出时就明确

2. **节点失败不阻塞下游**：调度器并不会因为一个节点失败就停止整个图，下游节点照常根据 `upstreamResults` 取已成功的结果。这相当于"软条件"

如果场景真的需要严格的运行时条件分支（比如"如果 A 返回 error 则跳到 C，否则跳到 B"），目前的方式是**通过 Agent 重新规划**——拿到失败结果后再调一次 Planner 生成新图。

这个权衡是**牺牲灵活性换取简洁性**：80% 的 Agent 场景不需要复杂条件边。

#### Q7：为什么不直接用 LangGraph 而要自己写一个？

**标准回答**：

**三个原因**：

1. **语言生态**：项目是 Go，LangGraph 是 Python——重写为 Go 实现的成本和自己设计差不多

2. **Agent 专用 vs 通用框架**：LangGraph 是通用 state machine graph，能做 Agent，也能做其他流程；本项目是 **Agent-first** 的，所有设计都为 Agent 场景优化：
   - RaceGroup 竞速 → 多搜索源场景
   - SubAgent 一等节点 → 多 Agent 协作场景
   - 多层降级 → LLM 输出不稳定场景
   - SSE 流式可视化 → Agent UI 场景

3. **可控性**：自己写的 800 行代码完全可读、可改、可测；LangGraph 是 8k+ LOC 的库，遇到问题难定位

适用场景区分：
- 通用工作流（DAG/状态机/分支） → LangGraph
- Go 项目 + Agent 专用场景 → 本项目

---

### 🎯 节点与执行类

#### Q8：节点有哪几种类型？

**标准回答**：

四种 `NodeType`：

| 类型 | 含义 | 执行方式 |
|---|---|---|
| `tool` | 工具调用 | `t.Execute(params)` |
| `sub_agent` | 子 Agent 任务 | `sa.Run(ctx, SubAgentTask{...})` |
| `think` | 推理思考（预留） | 当前未实装 |
| `aggregate` | 数据聚合（预留） | 当前未实装 |

实际项目主用前两种。统一接口（`runtime_graph.go:279-297`）：
```go
run := func() (string, error) {
    if node.Type == graph.NodeTypeSubAgent {
        return sa.Run(ctx, ...)
    }
    return t.Execute(params)
}
```

调度器只面对"节点"概念，不区分是工具还是 Agent——这是和 LangGraph 子图嵌套的根本区别。

#### Q9：节点之间怎么传数据？

**标准回答**：

**通过 `DependsOn` + `upstreamResults`**：

下游节点声明 `DependsOn: [n1, n2]`，执行时调度器自动从已完成节点收集结果（`runtime_graph.go:381-393`）：
```go
func (rt *GraphRuntime) upstreamResults(node *graph.Node) map[string]string {
    out := make(map[string]string)
    for _, depID := range node.DependsOn {
        if dep, ok := rt.graph.Nodes[depID]; ok && dep.Result != "" {
            key := fmt.Sprintf("%s:%s", depID, executorName(dep))
            out[key] = dep.Result
        }
    }
    return out
}
```

返回值作为 `SubAgentTask.Upstream` 注入子 Agent，子 Agent 在 prompt 里看到所有依赖结果。

**对比 LangGraph**：
- LangGraph 用共享 State 对象，每个节点 read/write 同一个 dict
- 本项目用"拉取式"——下游节点显式声明依赖，只能看到声明的上游结果
- 好处：**显式依赖更可读**，调试时一眼看出谁影响谁
- 代价：失去 State 的全局可见性，但 Agent 场景下不是问题

#### Q10：节点失败怎么处理？

**标准回答**：

**三层兜底**：

1. **重试**（`runtime_graph.go:304-317`）：
   ```go
   for attempt := 0; attempt < maxRetries; attempt++ {
       result, execErr = run()
       if execErr == nil { break }
       rt.graph.SetNodeRetryCount(nodeID, attempt+1)
       time.Sleep(retryDelay)
   }
   ```

2. **失败不阻塞**：节点失败标记为 `StatusFailed`，下游节点照常执行——只是 `upstreamResults` 里看不到这条失败的依赖

3. **TaskMem 记录**：失败信息写入 `TaskMemBuffer`，下次 Planner 规划时能看到"上次 X 工具失败了"，避免重复犯错

**竞速场景特殊处理**（`runtime_graph.go:218-228`）：
- 所有节点都失败 → 标记 `StatusFailed` + 记录 `lastErr`
- 至少一个成功 → 其余被 `cancel()` 取消，标记 `StatusSkipped`

#### Q11：怎么支持中断恢复？

**标准回答**：

**ctx 取消 + 状态保留 + 快照**三件套：

1. **ctx 取消传播**：
   - 用户取消请求 → ctx.Done
   - GraphRuntime 在每层结束时检查 `ctx.Err()`，立即停止后续层
   - 当前正在执行的节点通过 `select { case <-ctx.Done() }` 内部停止

2. **状态保留**：
   ```go
   func (rt *GraphRuntime) buildInterruptedResult(ctx context.Context, msg string) *GraphResult {
       for _, n := range rt.graph.Nodes {
           if n.Status == StatusPending || n.Status == StatusRunning {
               rt.graph.SetNodeStatus(n.ID, StatusCancelled)
           }
       }
       result := rt.buildResult()
       result.Interrupted = true
       return result
   }
   ```
   已完成的节点保留 Result，未完成的标记 Cancelled。

3. **快照持久化**：每层结束 `saveSnapshot(task)`，TaskState 含完整 TaskGraph，重启可恢复

**中断后的恢复**：通过 `buildInterruptMessageFromGraph` 生成可读摘要："已完成 3/5 步：n1(get_weather)→晴；n2(search_web)→...；未执行：n4、n5"——下次用户继续时把这些作为上下文喂回 LLM。

---

### 🎯 并发与竞速类

#### Q12：怎么控制并发度？为什么用信号量？

**标准回答**：

用 **buffered channel 实现信号量**（`runtime_graph.go:90`）：
```go
sem: make(chan struct{}, cfg.MaxParallel)
```

每个节点执行前：
```go
rt.sem <- struct{}{}                // 占一个槽
defer func() { <-rt.sem }()         // 释放
```

**为什么用信号量而非 worker pool**：
- 信号量更轻量——不需要预先分配 N 个 goroutine
- 节点数动态变化，goroutine 池难以适配
- channel 阻塞自带"等待"语义，代码简洁

**默认并发度** `MaxParallel = 2`（`runtime_graph.go:35`）——保守值，主要防止：
- LLM API 限流
- 工具并发请求被远端服务拒绝
- 子 Agent 自己内部又调 LLM，并发放大

可通过 `GraphConfig.MaxParallel` 调整。

#### Q13：竞速（RaceGroup）具体怎么实现？

**标准回答**：

**核心算法**：N 个 goroutine + buffered channel + context cancel。

```go
func (rt *GraphRuntime) raceGroup(ctx context.Context, g raceGroup, wg *sync.WaitGroup) {
    ch := make(chan raceResult, len(g.NodeIDs))
    raceCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    // 启动所有竞速节点
    for _, nodeID := range g.NodeIDs {
        go func(id graph.NodeID) {
            rt.sem <- struct{}{}; defer func() { <-rt.sem }()
            if raceCtx.Err() != nil { return }      // 已被取消
            res, err := rt.executeSingleNode(raceCtx, id)
            ch <- raceResult{nodeID: id, result: res, err: err}
        }(nodeID)
    }

    // 等待首个成功
    winnerFound := false
    for i := 0; i < len(g.NodeIDs); i++ {
        r := <-ch
        if r.err == nil && !winnerFound {
            winnerFound = true
            cancel()                                  // 关键：取消其余
            rt.results[r.nodeID] = r.result
            rt.graph.SetNodeStatus(r.nodeID, StatusDone)
        }
    }
}
```

**关键点**：
1. `raceCtx` 是从父 ctx 派生的子 ctx，`cancel()` 只取消竞速组内部
2. `ch` 是 buffered channel，避免 goroutine 阻塞泄漏
3. **不立即 return**——必须把所有 N 个结果收齐，否则 goroutine 泄漏

**好处**：wall-clock = min(各节点延迟)。如果 `search_web` 2s、`rag_search` 200ms → 取 200ms 结果。

#### Q14：竞速失败的节点怎么处理？

**标准回答**：

**三种状态**：
- **胜出节点**：`StatusDone` + 结果保留
- **被取消（其他先胜出）**：`StatusSkipped`
- **全部失败**：所有节点标记 `StatusFailed` + 保留 lastErr

代码（`runtime_graph.go:218-228`）：
```go
if !winnerFound {
    for _, nodeID := range g.NodeIDs {
        rt.graph.SetNodeStatus(nodeID, StatusFailed)
        if lastErr != nil {
            rt.graph.SetNodeError(nodeID, lastErr.Error())
            rt.errors[nodeID] = lastErr.Error()
        }
    }
}
```

**Skipped 在 prompt 里的处理**（`mode_react.go:149-150`）：
```go
case graph.StatusSkipped:
    steps = append(steps, ReActStep{
        Type: StepObservation,
        Content: "[竞速跳过] 其他节点已胜出",
    })
```

让 LLM 在合成答案时知道"这个工具被跳过了"，避免重复调用。

#### Q15：如果 RaceGroup 里的工具有副作用怎么办？比如同时调两个发邮件 API

**标准回答**：

这是 RaceGroup 的**已知边界**——竞速适合**幂等只读操作**（搜索、查询、检索）。

设计上的约束：
- Planner 在生成 race_group 时应该只在多源搜索类工具上设置
- 副作用工具（发邮件、写数据库、付款）即使语义相似也不应该 race

防御性设计：
- 当前没有强制检查，依赖 Planner 的判断
- 可以演进：在 `tool.Tool` 加 `Idempotent bool` 字段，调度器检测到 race 中有非幂等工具时降级为串行
- 或者在 `RaceGroup` 配置层加白名单（"只允许 search/rag 类工具")

诚实地说：**这是当前的短板，靠 Planner Prompt 工程约束，没有运行时保证**。

---

### 🎯 拓扑排序类

#### Q16：拓扑排序怎么实现的？为什么用 Kahn 算法？

**标准回答**：

**Kahn 算法**（`graph.go:97-141`），按"层"输出而非"线性序列"：

```go
for {
    var ready []NodeID
    for id, d := range inDeg {
        if d == 0 { ready = append(ready, id) }
    }
    if len(ready) == 0 { break }
    levels = append(levels, ready)
    for _, id := range ready {
        inDeg[id] = -1
        for _, downstream := range tg.AdjList[id] {
            inDeg[downstream]--
        }
    }
}

if processed != len(tg.Nodes) {
    return nil, fmt.Errorf("task graph has cycle")
}
```

**为什么 Kahn 而非 DFS**：
- **天然分层**：DFS 输出的是线性 topological order，需要后处理才能分层；Kahn 直接给出 `[][]NodeID`
- **环检测自然**：如果有环，`processed < len(Nodes)`，直接报错
- **代码直观**：入度=0 → 就绪，符合调度直觉
- **并发友好**：同层节点可以放心 goroutine 并发，不用担心依赖

**复杂度**：O(V+E)，对几十个节点的图毫无压力。

#### Q17：拓扑层结果会缓存吗？

**标准回答**：

**会缓存**（`graph.go:63, 98-101`）：
```go
type TaskGraph struct {
    // ...
    levels [][]NodeID  // 缓存的拓扑层级
}

func (tg *TaskGraph) TopologicalLevels() ([][]NodeID, error) {
    if tg.levels != nil {
        return tg.levels, nil
    }
    // ... 计算
    tg.levels = levels
    return levels, nil
}
```

**为什么缓存**：
- 同一个 TaskGraph 在 `Execute` 中至少被调两次（一次 `Validate`，一次实际执行），加缓存避免重复
- 不需要失效——TaskGraph 一旦构建，节点结构不再变化（只变状态/结果，不变依赖）

#### Q18：环检测怎么做？

**标准回答**：

**复用 Kahn 算法的副作用**：
```go
if processed != len(tg.Nodes) {
    return nil, fmt.Errorf("task graph has cycle: processed %d/%d nodes",
                            processed, len(tg.Nodes))
}
```

Kahn 算法每次从入度=0 的节点出发，移除后再找新的入度=0 节点。如果存在环，环上的节点入度永远不会归零——`processed` 最终小于总节点数。

加上**悬空依赖检查**（`graph.go:181-187`）：
```go
for _, n := range tg.Nodes {
    for _, dep := range n.DependsOn {
        if _, ok := tg.Nodes[dep]; !ok {
            return fmt.Errorf("node %s depends on nonexistent node %s", n.ID, dep)
        }
    }
}
```

**Validate 顺序**：先检查悬空依赖（错误信息更明确），再跑 Kahn 检测环。

**校验失败的兜底**（`mode_react.go:55-61`）：
```go
if err := tg.Validate(); err != nil {
    for _, n := range planNodes {
        n.DependsOn = nil
    }
    tg = graph.NewTaskGraph(planNodes)  // 全并行
}
```

不让 LLM 的 bug 把整个流程搞挂——这是关键的鲁棒性保证。

---

### 🎯 Planner LLM 类

#### Q19：Planner 怎么把用户问题转成 DAG？

**标准回答**：

**核心 Prompt 结构**（`plan_graph.go:96-117`）：

```
你是一个任务规划器。根据用户问题，从可用工具和可用子 Agent 中选出
需要调用的节点，并标注它们之间的依赖关系。

规则：
- 给每个节点分配一个唯一 id（如 n1, n2, n3...）
- type 只能是 "tool" 或 "sub_agent"
- 如果节点 B 需要节点 A 的输出，则 B 的 depends_on 包含 A 的 id
- 如果两个工具功能类似（如多个搜索源），设相同的 race_group

用户问题：...
可用工具：...
可用子 Agent：...

请以 JSON 数组格式输出执行计划：
[{"id":"n1","type":"sub_agent","agent":"research_agent","goal":"研究目标",
"params":{},"reason":"...","depends_on":[],"race_group":""}]
```

**输出后处理**（`plan_graph.go:144-216`）：
- 解析失败 → 尝试 legacy 格式 `[{"tool":...,"params":...}]`
- 再失败 → 尝试 function-calling 格式 `[{"name":...,"parameters":...}]`
- 全部失败 → 关键词规则 `rulePlanNodes`
- 过滤不存在的工具/Agent
- 自动补全 id/depends_on/race_group

**例子**：
- 输入："对比北京和上海明天的天气"
- 输出：
  ```json
  [
    {"id":"n1","tool":"get_weather","params":{"city":"北京"},"depends_on":[]},
    {"id":"n2","tool":"get_weather","params":{"city":"上海"},"depends_on":[]}
  ]
  ```
- 这是个**两节点全并行的 DAG**，wall-clock = max(n1, n2)

#### Q20：Planner LLM 输出不稳定怎么办？

**标准回答**：

**三层兜底**：

1. **格式兼容**：尝试 3 种 JSON 格式（图格式 → legacy → function-calling）
2. **过滤校验**：不存在的工具/Agent 直接 drop，自动补字段
3. **规则兜底**：所有解析都失败 → `rulePlanNodes` 关键词命中

`rulePlanNodes` 例子（`plan_graph.go:260-271`）：
```go
if _, ok := ts["get_time"]; ok {
    if strings.Contains(q, "时间") || strings.Contains(q, "几点") {
        params := map[string]string{}
        if strings.Contains(q, "东京") { params["timezone"] = "Asia/Tokyo" }
        nodes = append(nodes, &graph.Node{
            ID: nextID(), Type: NodeTypeTool, ToolName: "get_time",
            Params: params, Name: "查询当前时间", DependsOn: nil,
        })
    }
}
```

覆盖核心场景（时间/天气/搜索/执行命令/RAG 检索/MCP 工具）。规则不需要覆盖 100% 长尾——LLM 已经处理了大多数。

**长期演进**：可以让规则路径"记忆"成功的 Plan 模板，下次类似问题直接复用。

#### Q21：Planner 怎么决定加 RaceGroup？

**标准回答**：

**靠 Prompt 工程**：
```
如果两个工具功能类似（如多个搜索源），设相同的 race_group，
系统会并行执行谁先返回用谁
```

LLM 看到工具描述后判断哪些功能相似——比如 `search_web` 和 `rag_search` 都是"搜索类"，会自动放进 `race_group: "search"`。

**规则兜底也内置了 race**（`plan_graph.go:295, 315`）：
```go
ToolName: "search_web", RaceGroup: "search",
ToolName: "rag_search", RaceGroup: "search",
```

**当前局限**：
- 完全依赖 LLM 的"语义判断"——没有运行时校验
- 如果 LLM 把"发邮件"和"发短信"放一组就出问题（见 Q15）
- 可演进：工具描述里加 `category` 标签，限制只有同 category 才能 race

---

### 🎯 子 Agent 类

#### Q22：子 Agent 是什么？和工具有什么区别？

**标准回答**：

**子 Agent = 一个独立的、可被父 Agent 调用的小 Agent**。

```go
type SubAgent interface {
    Name() string
    Description() string
    Run(ctx context.Context, task SubAgentTask) (string, error)
}
```

**例子**（项目内置）：
- `research_agent`：多轮检索 + 证据整理
- `writer_agent`：基于研究结果写 Markdown 报告
- `review_agent`：审查报告质量
- `doc_agent`：保存到本地文档库 + 写入 RAG

**与工具的区别**：

| 维度 | 工具 | 子 Agent |
|---|---|---|
| 抽象级别 | 单次原子调用 | 多步任务 |
| 内部状态 | 无 | 可能有自己的循环和记忆 |
| 接口 | `Execute(params)` | `Run(ctx, SubAgentTask)` |
| 上游传递 | params | params + Upstream + Goal + Query |
| 一等节点 | ✓ | ✓ |

**为什么把子 Agent 也建模为节点**：
- 父 Agent 的 Planner 不需要区分"调工具"和"调子 Agent"
- 子 Agent 也能加入 RaceGroup（两个 research_agent 不同策略竞速）
- 调度器逻辑统一，复杂度不上升

#### Q23：子 Agent 怎么和父 Agent 通信？

**标准回答**：

**单向数据流**（父 → 子）：

```go
type SubAgentTask struct {
    ID       string
    Goal     string             // Planner 指定的任务目标
    Query    string             // 父 Agent 收到的原始用户问题
    Upstream map[string]string  // 依赖节点的结果
}
```

调度器调用时（`runtime_graph.go:285-290`）：
```go
return sa.Run(ctx, SubAgentTask{
    ID:       string(nodeID),
    Goal:     node.Goal,
    Query:    rt.task.Query,
    Upstream: rt.upstreamResults(node),
})
```

**关键**：
- `Goal` 是 Planner 给的任务指令（"研究 React 18 新特性"）
- `Query` 是原始用户问题（"帮我写一份 React 18 报告"）——子 Agent 可以参考原始意图
- `Upstream` 是依赖节点的结果（research_agent 的结果传给 writer_agent）

**返回值**：子 Agent 返回 string，作为节点 Result——下游节点的 Upstream 里能看到

**为什么单向**：
- 简化心智——下游只看上游，不会出现"子 Agent 反过来通知父 Agent"
- 状态可追踪——子 Agent 的输出就是它的全部贡献

#### Q24：子 Agent 内部能调用工具吗？

**标准回答**：

**能，但是独立的工具栈**。

子 Agent 内部可以有自己的工具集（比如 `research_agent` 内部循环调 `search_web` 和 `rag_search`），但这些调用**不会暴露给父 Agent 的 GraphRuntime**。

**好处**：
- 隔离——子 Agent 内部失败不会污染父图状态
- 抽象——父 Agent 的 Planner 不需要关心子 Agent 内部细节
- 可替换——`research_agent` 内部从"循环搜索"换成"单次 RAG"不影响外部

**代价**：
- 父 Agent 看不到子 Agent 内部的工具调用——可观测性下降
- 演进方向：子 Agent 的内部 step 也可以通过 SSE 推上来（嵌套事件）

---

### 🎯 与记忆系统集成类

#### Q25：DAG 执行过程怎么和记忆系统交互？

**标准回答**：

**两条写入路径**（`runtime_graph.go:326-353`）：

1. **TaskMem 写入**（每个节点执行后）：
   ```go
   rt.agent.pctx.pushTaskMem(promptctx.StepObservation{
       StepID:   nodeStepID(nodeID),
       ToolName: executor,
       Result:   result, Success: true,
   })
   ```
   写入 `TaskMemBuffer`（ring buffer，最近 20 条），下次 LLM 调用时被装配进 prompt 的 TaskMem 槽

2. **ToolTracker 写入**（同上）：
   ```go
   rt.agent.pctx.recordToolCall(promptctx.ToolCallTrace{
       ToolName: executor, Success: true, Summary: result,
   })
   ```
   写入 ToolStateTracker，下次 LLM 看到"最近调用 X 工具成功了"

**还有间接影响**：
- 节点失败信息进 TaskMem → 下次 Planner 看到 "上次 X 失败了" 会避免再选
- Generator LLM 在合成答案时，prompt 里同时包含 TaskMem（本次任务）和 Recall（长期记忆）

**与 LangGraph 区别**：
- LangGraph 把所有状态塞 State 对象，没有"短期 vs 长期记忆"区分
- 本项目把"任务步骤"（TaskMem，ring buffer）和"长期事实"（LTM，向量召回）分开存——见 [记忆系统文档](./MEMORY_SYSTEM.md)

#### Q26：新任务开始时怎么清理 TaskMem？

**标准回答**：

```go
a.pctx.resetTaskMem()
```

调用点（`mode_react.go:71`）：进入 ReAct 模式、构图之前 reset。

**为什么必须 reset**：
- TaskMem 是**任务级**的，不能跨任务污染
- 上一个任务的 "X 工具失败了" 会让本次 Planner 错误回避
- ring buffer 不 reset 会保留无关历史

**对比 LangGraph**：每次 Graph.invoke 是一次性的，新调用就是新 state——本项目通过 `resetTaskMem` 显式实现等价语义。

---

### 🎯 性能与扩展类

#### Q27：DAG 的性能瓶颈在哪？

**标准回答**：

**已知瓶颈**：

1. **Planner LLM 延迟**——每次都要调一次 LLM 生成图，1-3s 起步
   - 演进：成功 Plan 模板缓存（相似 query 复用）

2. **MaxParallel=2 偏保守**——多节点场景下其他节点排队
   - 演进：根据节点类型动态调（IO 重的可以高并发，CPU/LLM 重的需要限）

3. **同层最慢节点决定该层延迟**——一个慢节点拖累整层
   - 演进：节点级超时（当前 RaceGroup 有超时，普通节点没有）

4. **每层结束都 saveSnapshot**——PG 写入累积延迟
   - 演进：异步快照 + 节流

**实测**：
- 简单 query（单节点）：1-2s（主要是 LLM）
- 多节点 DAG：3-5s（Planner + 执行）
- 含子 Agent 的复杂 DAG（research + writer + review）：10-30s

#### Q28：怎么扩展支持新工具？

**标准回答**：

**三步**：

1. **实现 `tool.Tool` 接口**（`internal/domain/tool/`）
2. **注册到工具集**（启动期或运行时）
3. **Planner 自动可见**——Planner Prompt 会列出所有可用工具

**无需改 DAG 任何代码**——这是节点抽象的好处。

**新增子 Agent 同理**：实现 `SubAgent` 接口 → register → Planner 自动可见。

#### Q29：怎么扩展支持新的节点类型？

**标准回答**：

如果要支持 `NodeTypeThink`（推理节点）或 `NodeTypeAggregate`（聚合节点）：

1. **`graph.go` 加常量**：
   ```go
   NodeTypeThink NodeType = "think"
   ```

2. **`executeSingleNode` 加分支**：
   ```go
   if node.Type == graph.NodeTypeThink {
       return rt.executeThink(ctx, node)
   }
   ```

3. **Planner Prompt 加描述**：告诉 LLM 何时使用这种节点

4. **降级规则补 case**：`rulePlanNodes` 加关键词命中

整体结构不动，是"开放扩展、封闭修改"的。

---

### 🎯 短板/拷打类

#### Q30：你这套 DAG 有什么短板？

**标准回答**（主动暴露）：

1. **不支持循环**——self-correcting agent 场景需要在外层包装
2. **Planner 单次 LLM 调用决定全局**——一次失败就降级到规则，没有 self-refine
3. **RaceGroup 没有副作用检查**——靠 Planner Prompt 约束，没有运行时保证（见 Q15）
4. **同层最慢拖累全层**——没有"提前推进"机制
5. **子 Agent 内部不可观测**——SSE 看不到嵌套调用
6. **Plan 缓存缺失**——每次都重新 Plan，相似 query 浪费
7. **没有节点级超时**——只有 RaceGroup 有超时

#### Q31：如果让你重新设计，你会改什么？

**标准回答**：

**会改的**：
1. **Plan 模板缓存**——相似 query 命中已成功的 Plan 模板
2. **节点级超时**——避免单节点卡死
3. **嵌套 SSE**——子 Agent 内部 step 也推上来
4. **工具元数据**——加 `Idempotent` / `Category` 让 RaceGroup 有运行时校验
5. **Plan 自纠错**——Planner 输出后用一个 critic LLM 检查再执行
6. **节点级 checkpoint**——支持更细粒度恢复

**不会改的**：
1. **运行时图生成**——Agent 真正会规划是核心价值
2. **拓扑分层 + 信号量**——简单直接，比手动 async 强
3. **RaceGroup**——竞速是 Agent 场景的真实需求
4. **子 Agent 一等节点**——比 LangGraph 子图嵌套更清晰
5. **多层降级**——LLM 输出不稳定下的必备

#### Q32：怎么测试这套 DAG？

**标准回答**：

**分层测试**：

1. **单元测试**：
   - `TaskGraph`：环检测、悬空依赖、拓扑分层正确性
   - `GraphRuntime`：节点状态机、重试逻辑、ctx 取消
   - `raceGroup`：胜出取消其余、全失败兜底
   - `Planner`：JSON 解析降级、工具过滤

2. **集成测试**：
   - 端到端：模拟 LLM → Planner → 构图 → 执行 → Generator
   - 中断恢复：执行到一半 ctx cancel → 验证 InterruptedAt
   - 降级路径：Mock LLM 失败 → 验证走 rulePlanNodes

3. **真实 LLM 测试**（手动）：
   - 多种 query 类型覆盖不同图形状
   - 验证 RaceGroup 实际取最快
   - 验证子 Agent 链路（research → writer → review）

**当前覆盖**：`subagents_test.go` 等部分 case。**节点级单测和 race 边界 case 是已知短板**。

---

## 附录：一句话总结

> 这套 DAG 不是"通用图调度器"，是"为 Agent 场景定制的运行时图引擎"。
>
> 它和 LangGraph 的根本区别在于：**LangGraph 是工程师写图，本项目是 LLM 写图**。这个差异决定了一切设计——必须有多层降级（LLM 不稳定）、必须支持竞速（Agent 场景多源冗余）、必须把子 Agent 当一等节点（Agent 协作天然分形）、必须强制 DAG（环检测让 LLM 错误能被拒绝）。
>
> 它的优雅之处不在"图算法"，而在于**让 LLM 规划能力落到实处**——Agent 真正会"想清楚再做"，且想错了能兜底、做的过程能并行、跑出来能中断、做完了能继承到记忆系统。
>
> 一句话——**DAG 不是数据结构，是 Agent 的执行计划语言**。
