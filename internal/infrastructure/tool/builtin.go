// Package toolimpl 提供 domain/tool.Tool 的内置实现：time / weather / search / MCP / tavily / exec_command。
//
// 内置工具被分到 infrastructure 层是因为：
//   - 它们涉及外部 IO（HTTP 调用、时区库、命令执行）
//   - 是 domain.Tool 接口的"插件"，可独立替换 / 扩展
package toolimpl

import (
	"fmt"
	"strings"
	"time"

	"agi-assistant/internal/domain/tool"
)

// GetTime 返回当前时间，支持可选时区参数
func GetTime() tool.Tool {
	return tool.Tool{
		Name:        "get_time",
		Description: "获取当前时间",
		Parameters:  []tool.Param{{Name: "timezone", Type: "string", Description: "时区（如 Asia/Tokyo）", Required: false}},
		Execute: func(p map[string]interface{}) (string, error) {
			loc := time.Local
			if v, ok := p["timezone"].(string); ok && v != "" {
				if l, err := time.LoadLocation(v); err == nil {
					loc = l
				}
			}
			return time.Now().In(loc).Format("2006-01-02 15:04:05"), nil
		},
	}
}

// GetWeather 返回指定城市的模拟天气信息
func GetWeather() tool.Tool {
	db := map[string]string{
		"北京": "晴天 22°C",
		"东京": "多云 18°C 湿度65%",
		"上海": "小雨 20°C",
		"纽约": "晴天 15°C",
		"伦敦": "阴天 12°C",
		"广州": "晴天 28°C",
		"深圳": "晴天 26°C",
	}
	return tool.Tool{
		Name:        "get_weather",
		Description: "获取城市天气信息",
		Parameters:  []tool.Param{{Name: "city", Type: "string", Description: "城市名称", Required: true}},
		Execute: func(p map[string]interface{}) (string, error) {
			city, _ := p["city"].(string)
			if w, ok := db[city]; ok {
				return fmt.Sprintf("%s：%s", city, w), nil
			}
			return fmt.Sprintf("%s：晴天 20°C（模拟）", city), nil
		},
	}
}

// SearchWeb 模拟互联网关键词搜索
func SearchWeb() tool.Tool {
	db := map[string]string{
		"AI应用工程师": "AI 应用工程师是将 AI 技术落地到业务的工程师，需具备 ML 基础、API 开发、Prompt 工程等能力。",
		"Go语言":    "Go 是 Google 开发的开源编程语言，适用于高并发服务端应用。Docker 即用 Go 开发。",
	}
	return tool.Tool{
		Name:        "search_web",
		Description: "搜索互联网获取最新信息",
		Parameters:  []tool.Param{{Name: "query", Type: "string", Description: "搜索关键词", Required: true}},
		Execute: func(p map[string]interface{}) (string, error) {
			q, _ := p["query"].(string)
			for k, v := range db {
				if strings.Contains(q, k) {
					return v, nil
				}
			}
			return fmt.Sprintf("关于「%s」的搜索结果（模拟）", q), nil
		},
	}
}

// DefaultTools 返回所有内置工具的映射表（不含 rag_search，由 agent 动态注入）
func DefaultTools() map[string]tool.Tool {
	list := []tool.Tool{GetTime(), GetWeather(), SearchWeb()}
	m := make(map[string]tool.Tool, len(list))
	for _, t := range list {
		m[t.Name] = t
	}
	return m
}
