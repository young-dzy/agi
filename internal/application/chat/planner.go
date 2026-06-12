// planner.go — UnifiedAgent 的工具规划器。
//
// 抽自 agent.go 的 "Planner LLM" 区块。在 ReAct 模式下，先由 Planner LLM
// 根据可用工具集和用户问题产出一组 planItem，Harness 再逐项重试执行。
// LLM 不可用或解析失败时降级到 rulePlanItems 关键词规则。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
)

// planItem 是 Planner LLM 输出的单个工具调用计划
type planItem struct {
	Tool   string            `json:"tool"`
	Params map[string]string `json:"params"`
	Reason string            `json:"reason"`
}

// llmPlanSteps 调用 Planner LLM，从允许的工具集中智能选择需要调用的工具及参数。
// 若 LLM 不可用或解析失败，降级为关键词规则。
func (a *UnifiedAgent) llmPlanSteps(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string) []planItem {
	if !a.cfg.IsRealLLM() {
		return a.rulePlanItems(ctx, query, ts, memPrefix)
	}

	// 构造工具描述
	var toolLines []string
	for name, t := range ts {
		var pDescs []string
		for _, p := range t.Parameters {
			req := ""
			if p.Required {
				req = "（必填）"
			}
			pDescs = append(pDescs, fmt.Sprintf("%s(%s)%s", p.Name, p.Type, req))
		}
		params := strings.Join(pDescs, ", ")
		if params == "" {
			params = "无"
		}
		toolLines = append(toolLines, fmt.Sprintf("- %s: %s [参数: %s]", name, t.Description, params))
	}

	planPrompt := fmt.Sprintf(`你是一个任务规划器。
		根据用户问题，从可用工具中选出真正需要调用的工具（不要为了用工具而用工具，按需选择）。
		用户问题：%s
		可用工具：%s
		请以 JSON 数组格式输出执行计划，格式如下：
		[{"tool":"工具名","params":{"参数名":"参数值"},"reason":"一句话说明为什么调用这个工具"}]
		如果无需工具直接回答，输出 []。只输出 JSON，不要其他内容。`, query, strings.Join(toolLines, "\n"))

	plannerBase := "你是一个精准的任务规划器，只在必要时才调用工具，不做无意义的调用。"
	if memPrefix != "" {
		plannerBase = memPrefix + "\n\n" + plannerBase + "\n注意：用户偏好可能影响工具参数选择（如城市、时区等），请在参数中体现。"
	}
	raw := a.llm.ChatContext(ctx, plannerBase,
		[]llm.Message{{Role: "user", Content: planPrompt}})

	if ctx.Err() != nil {
		return a.rulePlanItems(ctx, query, ts, memPrefix)
	}

	// 清洗 LLM 输出（可能包含 markdown 代码块或特殊 function-call 标记）
	raw = strings.TrimSpace(raw)
	// 剥离模型输出的 <|FunctionCallBegin|>...<|FunctionCallEnd|> 包装
	if idx := strings.Index(raw, "<|FunctionCallBegin|>"); idx >= 0 {
		raw = raw[idx+len("<|FunctionCallBegin|>"):]
		if end := strings.Index(raw, "<|FunctionCallEnd|>"); end >= 0 {
			raw = raw[:end]
		}
	}
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	// 尝试解析为 [{"tool":...,"params":...}] 格式
	var items []planItem
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		// 降级：尝试解析为 [{"name":...,"parameters":...}] 格式（部分模型的 function-calling 格式）
		var altItems []struct {
			Name       string                 `json:"name"`
			Parameters map[string]interface{} `json:"parameters"`
		}
		if altErr := json.Unmarshal([]byte(raw), &altItems); altErr == nil {
			for _, ai := range altItems {
				params := make(map[string]string, len(ai.Parameters))
				for k, v := range ai.Parameters {
					params[k] = fmt.Sprint(v)
				}
				items = append(items, planItem{Tool: ai.Name, Params: params, Reason: "LLM 规划调用"})
			}
		} else {
			log.Printf("⚠️  Planner LLM 解析失败 (%v / %v)，降级到规则规划。原始输出: %s", err, altErr, raw)
			return a.rulePlanItems(ctx, query, ts, memPrefix)
		}
	}

	// 过滤：只保留工具集中实际存在的工具
	var valid []planItem
	for _, item := range items {
		if _, ok := ts[item.Tool]; ok {
			if item.Params == nil {
				item.Params = map[string]string{}
			}
			valid = append(valid, item)
		}
	}
	return valid
}

// rulePlanItems 关键词规则降级规划（无真实 LLM 时使用）
func (a *UnifiedAgent) rulePlanItems(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string) []planItem {
	q := strings.ToLower(query)
	var items []planItem

	if _, ok := ts["get_time"]; ok {
		if strings.Contains(q, "时间") || strings.Contains(q, "几点") || strings.Contains(q, "现在") {
			params := map[string]string{}
			if strings.Contains(q, "东京") {
				params["timezone"] = "Asia/Tokyo"
			}
			items = append(items, planItem{Tool: "get_time", Params: params, Reason: "查询当前时间"})
		}
	}
	if _, ok := ts["get_weather"]; ok {
		if strings.Contains(q, "天气") {
			city := "北京"
			for _, c := range []string{"东京", "北京", "上海", "广州", "深圳", "纽约", "伦敦"} {
				if strings.Contains(q, c) {
					city = c
					break
				}
			}
			items = append(items, planItem{Tool: "get_weather", Params: map[string]string{"city": city}, Reason: "查询" + city + "天气"})
		}
	}
	if _, ok := ts["search_web"]; ok {
		if strings.Contains(q, "搜索") || strings.Contains(q, "查询") || strings.Contains(q, "介绍") ||
			strings.Contains(q, "是什么") || strings.Contains(q, "怎么") || strings.Contains(q, "如何") {
			items = append(items, planItem{Tool: "search_web", Params: map[string]string{"query": query}, Reason: "搜索相关信息"})
		}
	}
	if _, ok := ts["exec_command"]; ok {
		if strings.Contains(q, "执行") || strings.Contains(q, "运行") || strings.Contains(q, "命令") ||
			strings.Contains(q, "终端") || strings.Contains(q, "lscpu") || strings.Contains(q, "cpu") ||
			strings.Contains(q, "磁盘") || strings.Contains(q, "内存") || strings.Contains(q, "系统信息") {
			cmd := extractShellCommand(query)
			items = append(items, planItem{Tool: "exec_command", Params: map[string]string{"command": cmd}, Reason: "执行终端命令"})
		}
	}
	if _, ok := ts["rag_search"]; ok {
		items = append(items, planItem{Tool: "rag_search", Params: map[string]string{"query": query}, Reason: "检索个人知识库"})
	}
	// MCP / 自定义工具
	builtins := map[string]bool{"get_time": true, "get_weather": true, "search_web": true, "rag_search": true, "exec_command": true}
	for name, t := range ts {
		if builtins[name] {
			continue
		}
		params := a.extractParamsForTool(ctx, query, t)
		items = append(items, planItem{Tool: name, Params: params, Reason: "调用工具 " + name})
	}
	return items
}
