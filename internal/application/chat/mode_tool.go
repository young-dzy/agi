// mode_tool.go — 单工具调用模式（"tool"）。
//
//   - runToolFromSet 同步：tool.Decide 选工具 → 执行 → 注入记忆前缀做 LLM 总结
//   - runToolStream  流式：同上，但通过 onEvent 推送中间事件
package chat

import (
	promptctx "agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"context"
	"fmt"
)

func (a *UnifiedAgent) runToolFromSet(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string, histMsgs []llm.Message) (string, *tool.CallResult) {
	tc := tool.Decide(query, ts)
	if tc == nil {
		return "我无法处理这个请求。", nil
	}
	tool, ok := ts[tc.ToolName]
	if !ok {
		return fmt.Sprintf("工具 %s 不存在", tc.ToolName), tc
	}

	// 偏好感知参数自动填充
	a.fillParamsFromPreference(tc)

	result, err := tool.Execute(tc.Params)
	if err != nil {
		if ctx.Err() != nil {
			return "[已中断]", tc
		}
		if a.toolTracker != nil {
			a.toolTracker.Record(promptctx.ToolCallTrace{
				ToolName: tc.ToolName, Success: false, Summary: err.Error(),
			})
		}
		return fmt.Sprintf("工具执行失败: %v", err), tc
	}
	tc.ToolResult = result
	if a.toolTracker != nil {
		a.toolTracker.Record(promptctx.ToolCallTrace{
			ToolName: tc.ToolName, Success: true, Summary: result,
		})
	}

	// 用带记忆的 system prompt 生成自然语言回复
	systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个善于综合信息的AI助手。结合你掌握的用户信息，使回答更个性化。")
	userMsg := fmt.Sprintf("用户问：%s\n工具 %s 返回结果：%s\n请根据结果自然地回答用户。", query, tc.ToolName, result)
	answer := a.llm.ChatContext(ctx, systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
	return answer, tc
}

// ─────────────────────────────── Stage 4：ReAct ──────────────────────────
func (a *UnifiedAgent) runToolStream(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string, histMsgs []llm.Message, onEvent func(StreamEvent)) (string, *tool.CallResult) {
	tc := tool.Decide(query, ts)
	if tc == nil {
		return "我无法处理这个请求。", nil
	}
	tool, ok := ts[tc.ToolName]
	if !ok {
		return fmt.Sprintf("工具 %s 不存在", tc.ToolName), tc
	}

	a.fillParamsFromPreference(tc)

	result, err := tool.Execute(tc.Params)
	if err != nil {
		if ctx.Err() != nil {
			return "[已中断]", tc
		}
		if a.toolTracker != nil {
			a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: tc.ToolName, Success: false, Summary: err.Error()})
		}
		return fmt.Sprintf("工具执行失败: %v", err), tc
	}
	tc.ToolResult = result
	if a.toolTracker != nil {
		a.toolTracker.Record(promptctx.ToolCallTrace{ToolName: tc.ToolName, Success: true, Summary: result})
	}

	onEvent(NewStreamEvent("tool_call", map[string]interface{}{
		"tool_name":   tc.ToolName,
		"params":      tc.Params,
		"tool_result": result,
	}))

	systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个善于综合信息的AI助手。结合你掌握的用户信息，使回答更个性化。")
	userMsg := fmt.Sprintf("用户问：%s\n工具 %s 返回结果：%s\n请根据结果自然地回答用户。", query, tc.ToolName, result)
	answer := a.llm.ChatStreamContext(ctx, systemPrompt, []llm.Message{{Role: "user", Content: userMsg}}, func(token string) {
		onEvent(NewStreamEvent("token", map[string]string{"content": token}))
	})
	return answer, tc
}
