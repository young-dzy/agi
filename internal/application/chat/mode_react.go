// mode_react.go — ReAct + Harness 多步推理模式。
//
//   - runReActWithTools / runReActStream  规划 → 逐步执行 → 合成
//   - llmGenerate / llmGenerateStream     基于全部观察合成最终答案
//   - extractParamsForTool                LLM 抽取参数 → 兜底规则
//   - executeStepWithRetryTool            单步执行 + 失败重试
//   - buildInterruptMessage / truncateStr 中断恢复 / 文本截断辅助
package chat

import (
	"agi-assistant/internal/domain/promptctx"
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
	var observations []string

	// ── Step 1: Planner LLM 决定调哪些工具及参数 ──────────────────────────
	planItems := a.llmPlanSteps(ctx, query, ts, memPrefix)

	// 若 Planner 决定不需要任何工具，直接走 LLM 对话
	if len(planItems) == 0 {
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		answer := a.llm.ChatContext(ctx, systemPrompt, histMsgs)
		reactSteps = append(reactSteps, ReActStep{Type: StepThought, Content: "分析后无需调用工具，直接回答"})
		reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
		if ctx.Err() != nil {
			return "[已中断] 规划完成但生成被中断", reactSteps, nil
		}
		return answer, reactSteps, nil
	}

	// 将 planItems 转换为 TaskStep 列表
	var taskSteps []TaskStep
	for i, pi := range planItems {
		taskSteps = append(taskSteps, TaskStep{
			ID: i + 1, Name: pi.Reason, ToolName: pi.Tool,
			Params: pi.Params, Status: StepPending,
		})
	}

	// 把 task 持有为本地变量：ReAct 循环的所有读写都走本地，
	// 多请求并发时彼此互不干扰。setTask 把它发布到 agent 共享状态供
	// PlannerSource / Snapshots 等只读访问。
	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing", Steps: taskSteps,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	// ── Step 2: 按 Planner 计划逐步执行工具 ───────────────────────────────
	for i := range task.Steps {
		// 每步开始前检查 context 是否已取消
		if ctx.Err() != nil {
			task.Phase = "interrupted"
			task.Status = "interrupted"
			task.InterruptedAt = i
			// 将当前步骤标记为中断
			task.Steps[i].Status = StepInterrupted
			// 生成中断摘要
			interruptMsg := a.buildInterruptMessage(task)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg})
			a.saveSnapshot(task)
			return "[已中断] " + interruptMsg, reactSteps, task
		}

		ts2 := &task.Steps[i]
		task.CurrentStep = i
		ts2.Status = StepRunning

		// Thought：展示 Planner 给出的调用理由
		reactSteps = append(reactSteps, ReActStep{
			Type:    StepThought,
			Content: ts2.Name, // Name 即 Planner 生成的 reason
		})
		reactSteps = append(reactSteps, ReActStep{
			Type:    StepAction,
			Content: fmt.Sprintf("调用 %s", ts2.ToolName),
			Tool:    ts2.ToolName,
			Params:  ts2.Params,
		})

		tool, ok := ts[ts2.ToolName]
		if !ok {
			ts2.Status = StepFailed
			ts2.Error = fmt.Sprintf("工具 %s 不在允许列表中", ts2.ToolName)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: ts2.Error})
			a.saveSnapshot(task)
			continue
		}
		if a.executeStepWithRetryTool(ctx, ts2, tool) {
			ts2.Status = StepDone
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: ts2.Result})
			observations = append(observations, fmt.Sprintf("[%s] %s", ts2.ToolName, ts2.Result))
			if a.taskMem != nil {
				a.taskMem.Push(promptctx.StepObservation{
					StepID: ts2.ID, ToolName: ts2.ToolName,
					Result: ts2.Result, Success: true,
				})
			}
			if a.toolTracker != nil {
				a.toolTracker.Record(promptctx.ToolCallTrace{
					ToolName: ts2.ToolName, Success: true, Summary: ts2.Result,
				})
			}
		} else {
			if ctx.Err() != nil {
				ts2.Status = StepInterrupted
				reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断]"})
			} else {
				ts2.Status = StepFailed
				reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", ts2.Error)})
				if a.taskMem != nil {
					a.taskMem.Push(promptctx.StepObservation{
						StepID: ts2.ID, ToolName: ts2.ToolName,
						Error: ts2.Error, Success: false,
					})
				}
				if a.toolTracker != nil {
					a.toolTracker.Record(promptctx.ToolCallTrace{
						ToolName: ts2.ToolName, Success: false, Summary: ts2.Error,
					})
				}
			}
		}
		a.saveSnapshot(task)
	}

	// ── Step 3: Generator LLM 综合所有观察结果生成最终答案 ────────────────
	if ctx.Err() != nil {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessage(task)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	task.Phase = "generating"
	answer := a.llmGenerate(ctx, query, observations, memPrefix, histMsgs)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

func (a *UnifiedAgent) runReActStream(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) (string, []ReActStep, *TaskState) {
	var reactSteps []ReActStep
	var observations []string

	planItems := a.llmPlanSteps(ctx, query, ts, memPrefix)

	if len(planItems) == 0 {
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

	var taskSteps []TaskStep
	for i, pi := range planItems {
		taskSteps = append(taskSteps, TaskStep{
			ID: i + 1, Name: pi.Reason, ToolName: pi.Tool,
			Params: pi.Params, Status: StepPending,
		})
	}

	task := &TaskState{
		TaskID: fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Query:  query, Status: "running", Phase: "executing", Steps: taskSteps,
	}
	a.setTask(task)
	if a.taskMem != nil {
		a.taskMem.Reset()
	}
	a.saveSnapshot(task)

	for i := range task.Steps {
		if ctx.Err() != nil {
			task.Phase = "interrupted"
			task.Status = "interrupted"
			task.InterruptedAt = i
			task.Steps[i].Status = StepInterrupted
			interruptMsg := a.buildInterruptMessage(task)
			reactSteps = append(reactSteps, ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg})
			onEvent(NewStreamEvent("step", ReActStep{Type: StepObservation, Content: "[已中断] " + interruptMsg}))
			a.saveSnapshot(task)
			return "[已中断] " + interruptMsg, reactSteps, task
		}

		ts2 := &task.Steps[i]
		task.CurrentStep = i
		ts2.Status = StepRunning

		thoughtStep := ReActStep{Type: StepThought, Content: ts2.Name}
		actionStep := ReActStep{Type: StepAction, Content: fmt.Sprintf("调用 %s", ts2.ToolName), Tool: ts2.ToolName, Params: ts2.Params}
		reactSteps = append(reactSteps, thoughtStep, actionStep)
		onEvent(NewStreamEvent("step", thoughtStep))
		onEvent(NewStreamEvent("step", actionStep))

		tool, ok := ts[ts2.ToolName]
		if !ok {
			ts2.Status = StepFailed
			ts2.Error = fmt.Sprintf("工具 %s 不在允许列表中", ts2.ToolName)
			obsStep := ReActStep{Type: StepObservation, Content: ts2.Error}
			reactSteps = append(reactSteps, obsStep)
			onEvent(NewStreamEvent("step", obsStep))
			a.saveSnapshot(task)
			continue
		}
		if a.executeStepWithRetryTool(ctx, ts2, tool) {
			ts2.Status = StepDone
			obsStep := ReActStep{Type: StepObservation, Content: ts2.Result}
			reactSteps = append(reactSteps, obsStep)
			onEvent(NewStreamEvent("step", obsStep))
			observations = append(observations, fmt.Sprintf("[%s] %s", ts2.ToolName, ts2.Result))
			if a.taskMem != nil {
				a.taskMem.Push(promptctx.StepObservation{StepID: ts2.ID, ToolName: ts2.ToolName, Result: ts2.Result, Success: true})
			}
			if a.toolTracker != nil {
				a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: ts2.ToolName, Success: true, Summary: ts2.Result})
			}
		} else {
			if ctx.Err() != nil {
				ts2.Status = StepInterrupted
				obsStep := ReActStep{Type: StepObservation, Content: "[已中断]"}
				reactSteps = append(reactSteps, obsStep)
				onEvent(NewStreamEvent("step", obsStep))
			} else {
				ts2.Status = StepFailed
				obsStep := ReActStep{Type: StepObservation, Content: fmt.Sprintf("执行失败: %s", ts2.Error)}
				reactSteps = append(reactSteps, obsStep)
				onEvent(NewStreamEvent("step", obsStep))
				if a.taskMem != nil {
					a.taskMem.Push(promptctx.StepObservation{StepID: ts2.ID, ToolName: ts2.ToolName, Error: ts2.Error, Success: false})
				}
				if a.toolTracker != nil {
					a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: ts2.ToolName, Success: false, Summary: ts2.Error})
				}
			}
		}
		a.saveSnapshot(task)
	}

	if ctx.Err() != nil {
		task.Phase = "interrupted"
		task.Status = "interrupted"
		interruptMsg := a.buildInterruptMessage(task)
		return "[已中断] " + interruptMsg, reactSteps, task
	}

	task.Phase = "generating"
	answer := a.llmGenerateStream(ctx, query, observations, memPrefix, histMsgs, onEvent)
	reactSteps = append(reactSteps, ReActStep{Type: StepFinalAnswer, Content: answer})
	task.Result = answer
	task.Status = "completed"
	task.Phase = "done"
	return answer, reactSteps, task
}

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

// buildInterruptMessage 根据已完成的步骤生成中断摘要
func (a *UnifiedAgent) buildInterruptMessage(task *TaskState) string {
	doneSteps := 0
	var doneDesc []string
	var pendingDesc []string
	for _, s := range task.Steps {
		switch s.Status {
		case StepDone:
			doneSteps++
			doneDesc = append(doneDesc, fmt.Sprintf("%d.%s→%s", s.ID, s.ToolName, truncateStr(s.Result, 30)))
		case StepPending, StepRunning, StepInterrupted:
			pendingDesc = append(pendingDesc, fmt.Sprintf("%d.%s", s.ID, s.ToolName))
		}
	}
	msg := fmt.Sprintf("已完成 %d/%d 步", doneSteps, len(task.Steps))
	if len(doneDesc) > 0 {
		msg += "：" + strings.Join(doneDesc, "；")
	}
	if len(pendingDesc) > 0 {
		msg += "；未执行：" + strings.Join(pendingDesc, "、")
	}
	return msg
}

func truncateStr(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}

// ─────────────────────────── Generator LLM ───────────────────────────────

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
		// LLM 输出无法解析时兜底：用 query 填充首个必填参数
		for _, p := range t.Parameters {
			if p.Required {
				result[p.Name] = query
				break
			}
		}
	}
	return result
}

// ─────────────────────────────── Stage 6：Harness ────────────────────────

// executeStepWithRetryTool 带重试的步骤执行，使用传入的具体工具实例
func (a *UnifiedAgent) executeStepWithRetryTool(ctx context.Context, step *TaskStep, tool tool.Tool) bool {
	params := make(map[string]interface{}, len(step.Params))
	for k, v := range step.Params {
		params[k] = v
	}
	for attempt := 0; attempt < a.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			step.Error = "被用户中断"
			return false
		}
		result, err := tool.Execute(params)
		if err == nil {
			step.Result = result
			return true
		}
		if ctx.Err() != nil {
			step.Error = "被用户中断"
			return false
		}
		step.RetryCount = attempt + 1
		step.Error = err.Error()
		time.Sleep(time.Duration(a.cfg.RetryDelayMs) * time.Millisecond)
	}
	return false
}

// saveSnapshot 对当前 TaskState 做深拷贝快照并持久化到 PG
//
// 接受显式 task 参数（不再读 a.task），以支持多请求并发：每个请求把
// 自己 ReAct 循环的 task 传进来，互不影响。a.snapshots 仍为全局历史，
