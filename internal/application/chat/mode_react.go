// mode_react.go — ReAct 图任务模式。
//
//   - runReActWithTools / runReActStream  规划 → 图并行执行 → 合成
//   - llmGenerate / llmGenerateStream     基于全部观察合成最终答案
//   - extractParamsForTool                LLM 抽取参数 → 兜底规则
//   - buildInterruptMessage / truncateStr 中断恢复 / 文本截断辅助
package chat

import (
	"agi-assistant/internal/domain/graph"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (a *UnifiedAgent) runReActWithTools(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string, histMsgs []llm.Message) (string, []ReActStep, *TaskState) {
	var reactSteps []ReActStep

	// ── Step 1: Planner LLM 输出带依赖的图节点 ──────────────────────────
	planNodes := a.llmPlanGraph(ctx, query, ts, memPrefix)

	// 若 Planner 决定不需要任何工具，直接走 LLM 对话
	if len(planNodes) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		answer := a.llm.ChatContext(ctx, systemPrompt, histMsgs)
		reactSteps = append(reactSteps, ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"})
		reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
		if ctx.Err() != nil {
			return "[已中断] 规划完成但生成被中断", reactSteps, nil
		}
		return answer, reactSteps, nil
	}

	// ── Step 2: 构建 TaskGraph ─────────────────────────────────────────
	tg := graph.NewTaskGraph(planNodes)
	if err := tg.Validate(); err != nil {
		// 图校验失败，降级为全并行
		for _, n := range planNodes {
			n.DependsOn = nil
		}
		tg = graph.NewTaskGraph(planNodes)
	}

	// 构造 TaskState（兼容现有 Snapshot / 中断恢复）
	taskSteps := graphToTaskSteps(tg)
	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing",
		Steps: taskSteps, Graph: tg,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	// ── Step 3: GraphRuntime 并行执行 ─────────────────────────────────
	gcfg := GraphConfig{
		MaxParallel:   a.cfg.GraphMaxParallel,
		RaceTimeoutMs: a.cfg.GraphRaceTimeoutMs,
		EnableRacing:  a.cfg.GraphEnableRacing,
	}
	rt := NewGraphRuntime(tg, a, gcfg, ts, task, nil)
	graphResult := rt.Execute(ctx)

	// 从 GraphResult 收集 ReAct 步骤
	reactSteps = graphResultToReActSteps(tg, graphResult)

	// ── Step 4: 检查中断 ──────────────────────────────────────────────
	if ctx.Err() != nil || graphResult.Interrupted {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessageFromGraph(tg)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	// ── Step 5: Generator LLM 综合所有观察结果生成最终答案 ────────────────
	task.Phase = "generating"
	answer := a.llmGenerate(ctx, query, graphResult.Observations, memPrefix, histMsgs)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

func (a *UnifiedAgent) runReActStream(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) (string, []ReActStep, *TaskState) {
	var reactSteps []ReActStep

	planNodes := a.llmPlanGraph(ctx, query, ts, memPrefix)

	if len(planNodes) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		reactSteps = append(reactSteps, ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"})
		onEvent(NewStreamEvent("step", ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"}))
		answer := a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
		reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
		if ctx.Err() != nil {
			return "[已中断] 规划完成但生成被中断", reactSteps, nil
		}
		return answer, reactSteps, nil
	}

	tg := graph.NewTaskGraph(planNodes)
	if err := tg.Validate(); err != nil {
		for _, n := range planNodes {
			n.DependsOn = nil
		}
		tg = graph.NewTaskGraph(planNodes)
	}

	taskSteps := graphToTaskSteps(tg)
	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing",
		Steps: taskSteps, Graph: tg,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	// 流式模式：传入 onEvent
	gcfg := GraphConfig{
		MaxParallel:   a.cfg.GraphMaxParallel,
		RaceTimeoutMs: a.cfg.GraphRaceTimeoutMs,
		EnableRacing:  a.cfg.GraphEnableRacing,
	}
	rt := NewGraphRuntime(tg, a, gcfg, ts, task, onEvent)
	graphResult := rt.Execute(ctx)

	reactSteps = graphResultToReActSteps(tg, graphResult)

	if ctx.Err() != nil || graphResult.Interrupted {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessageFromGraph(tg)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	task.Phase = "generating"
	answer := a.llmGenerateStream(ctx, query, graphResult.Observations, memPrefix, histMsgs, onEvent)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

// ─────────────────────────── Graph → 旧结构适配 ───────────────────────────

// graphToTaskSteps 将 TaskGraph 节点映射为 TaskStep 列表（兼容现有 Snapshot / SSE）
func graphToTaskSteps(tg *graph.TaskGraph) []TaskStep {
	levels, _ := tg.TopologicalLevels()
	var steps []TaskStep
	counter := 0
	for _, level := range levels {
		for _, id := range level {
			counter++
			n := tg.Nodes[id]
			steps = append(steps, TaskStep{
				ID: counter, Name: n.Name, ToolName: n.ToolName,
				Params: n.Params, Status: StepPending,
			})
		}
	}
	return steps
}

// graphResultToReActSteps 从图结果中提取 ReAct 步骤
func graphResultToReActSteps(tg *graph.TaskGraph, gr *GraphResult) []ReActStep {
	levels, _ := tg.TopologicalLevels()
	var steps []ReActStep
	for _, level := range levels {
		for _, id := range level {
			n := tg.Nodes[id]
			steps = append(steps, ReActStep{Type: StepThought, Content: n.Name})
			steps = append(steps, ReActStep{
				Type: StepAction, Content: fmt.Sprintf("调用 %s", n.ToolName),
				Tool: n.ToolName, Params: n.Params,
			})
			switch n.Status {
			case graph.StatusDone:
				steps = append(steps, ReActStep{Type: StepObservation, Content: n.Result})
			case graph.StatusFailed:
				steps = append(steps, ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", n.Error)})
			case graph.StatusSkipped:
				steps = append(steps, ReActStep{Type: StepObservation, Content: "[竞速跳过] 其他节点已胜出"})
			case graph.StatusCancelled:
				steps = append(steps, ReActStep{Type: StepObservation, Content: "[已中断]"})
			}
		}
	}
	return steps
}

// buildInterruptMessageFromGraph 根据图节点状态生成中断摘要
func (a *UnifiedAgent) buildInterruptMessageFromGraph(tg *graph.TaskGraph) string {
	doneCount := 0
	var doneDesc []string
	var pendingDesc []string
	for _, n := range tg.Nodes {
		switch n.Status {
		case graph.StatusDone:
			doneCount++
			doneDesc = append(doneDesc, fmt.Sprintf("%s(%s)→%s", n.ID, n.ToolName, truncateStr(n.Result, 30)))
		case graph.StatusPending, graph.StatusRunning, graph.StatusCancelled:
			pendingDesc = append(pendingDesc, fmt.Sprintf("%s(%s)", n.ID, n.ToolName))
		}
	}
	msg := fmt.Sprintf("已完成 %d/%d 步", doneCount, len(tg.Nodes))
	if len(doneDesc) > 0 {
		msg += "：" + strings.Join(doneDesc, "；")
	}
	if len(pendingDesc) > 0 {
		msg += "；未执行：" + strings.Join(pendingDesc, "、")
	}
	return msg
}

// ─────────────────────────── Generator LLM ───────────────────────────────

// llmGenerateStream 流式版本的 Generator LLM，逐 token 回调
func (a *UnifiedAgent) llmGenerateStream(ctx context.Context, query string, observations []string, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) string {
	if len(observations) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		return a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
	}
	if !a.cfg.IsRealLLM() {
		return "综合查询结果：" + strings.Join(observations, "；")
	}

	var obsBuilder strings.Builder
	for i, obs := range observations {
		obsBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, obs))
	}

	genPrompt := fmt.Sprintf(`请根据以下工具执行结果，综合回答用户的问题。回答要自然流畅、重点突出，不要机械罗列原始数据，也不要重复问题本身。

	用户问题：%s

	工具执行结果：
	%s`, query, obsBuilder.String())

	generatorBase := "你是一个善于综合信息的AI助手，能将多个工具的执行结果整合成清晰自然的回答。"
	if memPrefix != "" {
		generatorBase = memPrefix + "\n\n" + generatorBase + "\n结合用户偏好，使回答更个性化。"
	}
	return a.llm.ChatStreamContext(ctx, generatorBase,
		[]llm.Message{{Role: "user", Content: genPrompt}},
		func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
}

// llmGenerate 调用 Generator LLM，将多个工具观察结果合成为自然语言最终答案
func (a *UnifiedAgent) llmGenerate(ctx context.Context, query string, observations []string, memPrefix string, histMsgs []llm.Message) string {
	if len(observations) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		return a.llm.ChatContext(ctx, systemPrompt, histMsgs)
	}
	if !a.cfg.IsRealLLM() {
		return "综合查询结果：" + strings.Join(observations, "；")
	}

	var obsBuilder strings.Builder
	for i, obs := range observations {
		obsBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, obs))
	}

	genPrompt := fmt.Sprintf(`请根据以下工具执行结果，综合回答用户的问题。回答要自然流畅、重点突出，不要机械罗列原始数据，也不要重复问题本身。

	用户问题：%s

	工具执行结果：
	%s`, query, obsBuilder.String())

	generatorBase := "你是一个善于综合信息的AI助手，能将多个工具的执行结果整合成清晰自然的回答。"
	if memPrefix != "" {
		generatorBase = memPrefix + "\n\n" + generatorBase + "\n结合用户偏好，使回答更个性化。"
	}
	return a.llm.ChatContext(ctx, generatorBase,
		[]llm.Message{{Role: "user", Content: genPrompt}})
}

// extractParamsForTool 用 LLM 从 query 中提取工具所需参数；无法调用时用 query 填充首个必填参数
func (a *UnifiedAgent) extractParamsForTool(ctx context.Context, query string, t tool.Tool) map[string]string {
	result := make(map[string]string)
	if len(t.Parameters) == 0 {
		return result
	}
	if !a.cfg.IsRealLLM() {
		for _, p := range t.Parameters {
			if p.Required {
				result[p.Name] = query
				break
			}
		}
		return result
	}
	var lines []string
	for _, p := range t.Parameters {
		req := ""
		if p.Required {
			req = "（必填）"
		}
		lines = append(lines, fmt.Sprintf("- %s (%s)%s: %s", p.Name, p.Type, req, p.Description))
	}
	prompt := fmt.Sprintf(
		"从下面的用户消息中提取工具「%s」所需的参数，以JSON对象格式输出，只输出JSON，不加任何说明。\n\n参数说明：\n%s\n\n用户消息：%s",
		t.Name, strings.Join(lines, "\n"), query,
	)
	raw := a.llm.ChatContext(ctx, "", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		for _, p := range t.Parameters {
			if p.Required {
				result[p.Name] = query
				break
			}
		}
	}
	return result
}

func truncateStr(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}
