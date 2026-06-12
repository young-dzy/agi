// Package tool 定义 Agent 可调用的工具抽象与基于规则的工具选择逻辑。
//
// 这个包是纯 domain：
//   - Tool / Param / CallResult 是领域类型
//   - Decide 是基于关键词的工具选择规则
//   - 不持有任何具体工具实现（time/weather/search/exec_command 在 infrastructure/tool）
package tool

import "strings"

// Param 描述工具的单个参数（用于前端展示和 LLM function-calling schema）
type Param struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

// Tool 是可被 Agent 调用的原子能力单元
type Tool struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Parameters  []Param `json:"parameters"`
	IsMCP       bool    `json:"is_mcp,omitempty"` // 是否为外部 MCP 工具
	// Execute 执行工具逻辑，params 对应 Parameters 中声明的参数
	Execute func(params map[string]interface{}) (string, error) `json:"-"`
}

// CallResult 记录一次工具调用的完整上下文（供响应和日志使用）
type CallResult struct {
	ToolName   string                 `json:"tool_name"`
	Params     map[string]interface{} `json:"params"`
	ToolResult string                 `json:"tool_result"`
}

// ─────────────────────────────── 工具选择 ────────────────────────────────

// Decide 基于规则推断应调用的工具及参数。
// 只会返回 ts 中实际存在的工具；若规则匹配到的工具不在 ts 中则返回 nil。
func Decide(query string, ts map[string]Tool) *CallResult {
	q := strings.ToLower(query)

	if strings.Contains(q, "几点") || strings.Contains(q, "时间") {
		if _, ok := ts["get_time"]; ok {
			params := map[string]interface{}{}
			if strings.Contains(q, "东京") {
				params["timezone"] = "Asia/Tokyo"
			}
			return &CallResult{ToolName: "get_time", Params: params}
		}
	}

	if strings.Contains(q, "天气") {
		if _, ok := ts["get_weather"]; ok {
			city := "北京"
			for _, c := range []string{"东京", "北京", "上海", "纽约", "伦敦", "广州", "深圳"} {
				if strings.Contains(q, c) {
					city = c
					break
				}
			}
			return &CallResult{ToolName: "get_weather", Params: map[string]interface{}{"city": city}}
		}
	}

	if strings.Contains(q, "查") || strings.Contains(q, "搜索") || strings.Contains(q, "是什么") {
		if _, ok := ts["search_web"]; ok {
			return &CallResult{ToolName: "search_web", Params: map[string]interface{}{"query": query}}
		}
	}

	if _, ok := ts["exec_command"]; ok {
		return &CallResult{ToolName: "exec_command", Params: map[string]interface{}{"command": query}}
	}

	// 无规则命中或命中工具不在集合中时，取集合中第一个工具兜底
	// 使用工具的首个必填参数名而非硬编码 "query"
	for name, t := range ts {
		paramName := "query"
		for _, p := range t.Parameters {
			if p.Required {
				paramName = p.Name
				break
			}
		}
		return &CallResult{ToolName: name, Params: map[string]interface{}{paramName: query}}
	}
	return nil
}
