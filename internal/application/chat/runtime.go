// runtime.go — GraphRuntime：任务图的并行调度 + 竞速执行引擎。
//
// 核心调度策略：
//   - 拓扑排序按层调度，同层节点可并行执行
//   - 同 RaceGroup 的节点竞速执行（First-success-wins）
//   - 信号量控制最大并行度
//   - 支持 context 取消 + 中断恢复
package chat

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"agi-assistant/internal/domain/graph"
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/tool"
)

// ─────────────────────────────── GraphConfig ──────────────────────────────────

// GraphConfig 控制图运行时行为
type GraphConfig struct {
	MaxParallel   int  `yaml:"max_parallel"`   // 最大并行数，默认 2
	RaceTimeoutMs int  `yaml:"race_timeout_ms"` // 竞速组超时（毫秒），默认 30000
	EnableRacing  bool `yaml:"enable_racing"`  // 是否启用竞速，默认 true
}

// DefaultGraphConfig 返回默认配置
func DefaultGraphConfig() GraphConfig {
	return GraphConfig{MaxParallel: 2, RaceTimeoutMs: 30000, EnableRacing: true}
}

// ─────────────────────────────── GraphResult ──────────────────────────────────

// GraphResult 是图执行完毕后的汇总结果
type GraphResult struct {
	Observations    []string             // 所有成功节点的观察
	NodeResults     map[graph.NodeID]NodeResult
	Interrupted     bool
	InterruptedAt   graph.NodeID         // 被中断时正在执行的节点
	InterruptedMsg  string
}

// NodeResult 单节点的执行结果
type NodeResult struct {
	Status graph.NodeStatus
	Result string
	Error  string
}

// ─────────────────────────────── raceGroup ──────────────────────────────────

// raceGroup 是同一层中属于同 RaceGroup 的节点集合
type raceGroup struct {
	RaceGroup string
	NodeIDs   []graph.NodeID
}

// ─────────────────────────────── GraphRuntime ──────────────────────────────────

// GraphRuntime 负责按拓扑层级并行调度执行任务图
type GraphRuntime struct {
	graph   *graph.TaskGraph
	agent   *UnifiedAgent
	cfg     GraphConfig
	tools   map[string]tool.Tool // 允许调用的工具集
	sem     chan struct{}         // 并发信号量
	mu      sync.Mutex
	results map[graph.NodeID]string
	errors  map[graph.NodeID]string
	task    *TaskState             // 共享 TaskState，用于快照 / 中断恢复
	onEvent func(StreamEvent)      // SSE 事件回调（nil = 静默模式）
}

// NewGraphRuntime 创建图运行时
func NewGraphRuntime(tg *graph.TaskGraph, agent *UnifiedAgent, cfg GraphConfig, tools map[string]tool.Tool, task *TaskState, onEvent func(StreamEvent)) *GraphRuntime {
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 2
	}
	return &GraphRuntime{
		graph:   tg,
		agent:   agent,
		cfg:     cfg,
		tools:   tools,
		sem:     make(chan struct{}, cfg.MaxParallel),
		results: make(map[graph.NodeID]string),
		errors:  make(map[graph.NodeID]string),
		task:    task,
		onEvent: onEvent,
	}
}

// Execute 执行整个任务图，逐层并行调度
func (rt *GraphRuntime) Execute(ctx context.Context) *GraphResult {
	levels, err := rt.graph.TopologicalLevels()
	if err != nil {
		log.Printf("⚠️  GraphRuntime: 图校验失败 %v", err)
		return &GraphResult{InterruptedMsg: fmt.Sprintf("图校验失败: %v", err)}
	}

	// 推送图就绪事件
	if rt.onEvent != nil {
		rt.onEvent(NewStreamEvent("graph_ready", map[string]interface{}{
			"levels": levels,
			"nodes":  rt.graph.Nodes,
		}))
	}

	for levelIdx, level := range levels {
		if ctx.Err() != nil {
			return rt.buildInterruptedResult(ctx, fmt.Sprintf("在第 %d 层执行前被中断", levelIdx))
		}

		// 按 race_group 分组：同组竞速，其余独立并行
		groups := rt.groupByRace(level)

		var wg sync.WaitGroup
		for _, g := range groups {
			if g.RaceGroup != "" && rt.cfg.EnableRacing {
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

		// 检查 ctx
		if ctx.Err() != nil {
			return rt.buildInterruptedResult(ctx, fmt.Sprintf("在第 %d 层执行后被中断", levelIdx))
		}

		// 持久化快照
		rt.agent.saveSnapshot(rt.task)
	}

	return rt.buildResult()
}

// ─────────────────────────── 竞速执行（First-success-wins）──────────────────

func (rt *GraphRuntime) raceGroup(ctx context.Context, g raceGroup, wg *sync.WaitGroup) {
	defer wg.Done()

	type raceResult struct {
		nodeID graph.NodeID
		result string
		err    error
	}

	ch := make(chan raceResult, len(g.NodeIDs))
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 并行启动所有竞速节点
	for _, nodeID := range g.NodeIDs {
		go func(id graph.NodeID) {
			// 获取信号量
			rt.sem <- struct{}{}
			defer func() { <-rt.sem }()

			// 如果已被取消（其他节点先胜出了），直接跳过
			if raceCtx.Err() != nil {
				ch <- raceResult{nodeID: id, err: fmt.Errorf("竞速取消")}
				return
			}

			res, execErr := rt.executeSingleNode(raceCtx, id)
			ch <- raceResult{nodeID: id, result: res, err: execErr}
		}(nodeID)
	}

	// 等待首个成功结果
	winnerFound := false
	var lastErr error
	for i := 0; i < len(g.NodeIDs); i++ {
		r := <-ch
		if r.err == nil && !winnerFound {
			// 首个成功 → 取消其余 + 标记胜出
			winnerFound = true
			cancel()
			rt.mu.Lock()
			rt.results[r.nodeID] = r.result
			rt.graph.SetNodeStatus(r.nodeID, graph.StatusDone)
			rt.graph.SetNodeResult(r.nodeID, r.result)
			rt.mu.Unlock()

			// 推送竞速胜出事件
			if rt.onEvent != nil {
				node := rt.graph.Nodes[r.nodeID]
				rt.onEvent(NewStreamEvent("race_won", map[string]string{
					"race_group": g.RaceGroup, "winner": string(r.nodeID),
					"tool": node.ToolName,
				}))
				rt.onEvent(NewStreamEvent("step", ReActStep{
					Type: StepObservation, Content: r.result,
					Tool: node.ToolName, Params: node.Params,
				}))
			}
		} else if !winnerFound {
			lastErr = r.err
		}
		// 非胜出的节点标记 skipped
		if winnerFound && r.nodeID != r.nodeID {
			rt.graph.SetNodeStatus(r.nodeID, graph.StatusSkipped)
		}
	}

	// 如果没有胜出者，标记所有失败
	if !winnerFound {
		for _, nodeID := range g.NodeIDs {
			rt.graph.SetNodeStatus(nodeID, graph.StatusFailed)
			if lastErr != nil {
				rt.graph.SetNodeError(nodeID, lastErr.Error())
				rt.mu.Lock()
				rt.errors[nodeID] = lastErr.Error()
				rt.mu.Unlock()
			}
		}
	}
}

// ─────────────────────────── 单节点执行 ──────────────────────────────────────

func (rt *GraphRuntime) executeNode(ctx context.Context, nodeID graph.NodeID, wg *sync.WaitGroup) {
	defer wg.Done()

	// 获取信号量
	rt.sem <- struct{}{}
	defer func() { <-rt.sem }()

	result, err := rt.executeSingleNode(ctx, nodeID)
	if err != nil {
		rt.mu.Lock()
		rt.errors[nodeID] = err.Error()
		rt.mu.Unlock()
		return
	}

	rt.mu.Lock()
	rt.results[nodeID] = result
	rt.mu.Unlock()
}

// executeSingleNode 执行单个节点：获取工具 → 重试执行 → 记录结果
func (rt *GraphRuntime) executeSingleNode(ctx context.Context, nodeID graph.NodeID) (string, error) {
	node := rt.graph.Nodes[nodeID]

	// 推送节点开始事件
	if rt.onEvent != nil {
		rt.onEvent(NewStreamEvent("node_start", map[string]string{
			"id": string(nodeID), "tool": node.ToolName,
		}))
	}

	// Thought 步骤
	thoughtStep := ReActStep{Type: StepThought, Content: node.Name}
	actionStep := ReActStep{Type: StepAction, Content: fmt.Sprintf("调用 %s", node.ToolName), Tool: node.ToolName, Params: node.Params}
	if rt.onEvent != nil {
		rt.onEvent(NewStreamEvent("step", thoughtStep))
		rt.onEvent(NewStreamEvent("step", actionStep))
	}

	rt.graph.SetNodeStatus(nodeID, graph.StatusRunning)

	// 查找工具
	t, ok := rt.tools[node.ToolName]
	if !ok {
		errMsg := fmt.Sprintf("工具 %s 不在允许列表中", node.ToolName)
		rt.graph.SetNodeStatus(nodeID, graph.StatusFailed)
		rt.graph.SetNodeError(nodeID, errMsg)
		if rt.onEvent != nil {
			rt.onEvent(NewStreamEvent("step", ReActStep{Type: StepObservation, Content: errMsg}))
		}
		return "", fmt.Errorf(errMsg)
	}

	// 重试执行
	params := make(map[string]interface{}, len(node.Params))
	for k, v := range node.Params {
		params[k] = v
	}

	var result string
	var execErr error
	maxRetries := rt.agent.cfg.MaxRetries
	retryDelay := time.Duration(rt.agent.cfg.RetryDelayMs) * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if ctx.Err() != nil {
			rt.graph.SetNodeStatus(nodeID, graph.StatusCancelled)
			return "", fmt.Errorf("被用户中断")
		}
		result, execErr = t.Execute(params)
		if execErr == nil {
			break
		}
		rt.graph.SetNodeRetryCount(nodeID, attempt+1)
		if attempt < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}

	if execErr != nil {
		errMsg := execErr.Error()
		rt.graph.SetNodeStatus(nodeID, graph.StatusFailed)
		rt.graph.SetNodeError(nodeID, errMsg)
		if rt.onEvent != nil {
			rt.onEvent(NewStreamEvent("step", ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", errMsg)}))
		}
		if rt.agent.taskMem != nil {
			rt.agent.taskMem.Push(promptctx.StepObservation{
				StepID: nodeStepID(nodeID), ToolName: node.ToolName,
				Error: errMsg, Success: false,
			})
		}
		if rt.agent.toolTracker != nil {
			rt.agent.toolTracker.Record(promptctx.ToolCallTrace{
				ToolName: node.ToolName, Success: false, Summary: errMsg,
			})
		}
		return "", execErr
	}

	// 成功
	rt.graph.SetNodeStatus(nodeID, graph.StatusDone)
	rt.graph.SetNodeResult(nodeID, result)

	if rt.onEvent != nil {
		rt.onEvent(NewStreamEvent("node_done", map[string]string{
			"id": string(nodeID), "tool": node.ToolName, "status": "done",
		}))
		rt.onEvent(NewStreamEvent("step", ReActStep{Type: StepObservation, Content: result}))
	}

	if rt.agent.taskMem != nil {
		rt.agent.taskMem.Push(promptctx.StepObservation{
			StepID: nodeStepID(nodeID), ToolName: node.ToolName,
			Result: result, Success: true,
		})
	}
	if rt.agent.toolTracker != nil {
		rt.agent.toolTracker.Record(promptctx.ToolCallTrace{
			ToolName: node.ToolName, Success: true, Summary: result,
		})
	}

	return result, nil
}

// ─────────────────────────── 辅助方法 ──────────────────────────────────────

// nodeStepID 从 NodeID（如 "n1", "n2"）中提取数字 ID，失败则返回 0
func nodeStepID(id graph.NodeID) int {
	s := string(id)
	if len(s) > 1 && s[0] == 'n' {
		if n, err := strconv.Atoi(s[1:]); err == nil {
			return n
		}
	}
	return 0
}

// groupByRace 将同一层中的节点按 race_group 分组
func (rt *GraphRuntime) groupByRace(level []graph.NodeID) []raceGroup {
	groupMap := make(map[string][]graph.NodeID)
	var noGroup []graph.NodeID

	for _, id := range level {
		node := rt.graph.Nodes[id]
		if node.RaceGroup != "" {
			groupMap[node.RaceGroup] = append(groupMap[node.RaceGroup], id)
		} else {
			noGroup = append(noGroup, id)
		}
	}

	var groups []raceGroup
	for rg, ids := range groupMap {
		groups = append(groups, raceGroup{RaceGroup: rg, NodeIDs: ids})
	}
	// 无竞速组的节点，每个独立为一组
	for _, id := range noGroup {
		groups = append(groups, raceGroup{RaceGroup: "", NodeIDs: []graph.NodeID{id}})
	}

	// 按 race_group 排序保证确定性
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].RaceGroup < groups[j].RaceGroup
	})
	return groups
}

func (rt *GraphRuntime) buildResult() *GraphResult {
	observations := rt.graph.SuccessfulResults()
	nodeResults := make(map[graph.NodeID]NodeResult, len(rt.graph.Nodes))
	for id, n := range rt.graph.Nodes {
		nr := NodeResult{Status: n.Status, Result: n.Result, Error: n.Error}
		nodeResults[id] = nr
	}
	return &GraphResult{Observations: observations, NodeResults: nodeResults}
}

func (rt *GraphRuntime) buildInterruptedResult(ctx context.Context, msg string) *GraphResult {
	// 标记所有 pending/running 节点为 cancelled
	for _, n := range rt.graph.Nodes {
		if n.Status == graph.StatusPending || n.Status == graph.StatusRunning {
			rt.graph.SetNodeStatus(n.ID, graph.StatusCancelled)
		}
	}
	result := rt.buildResult()
	result.Interrupted = true
	result.InterruptedMsg = msg
	return result
}
