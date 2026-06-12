package toolimpl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"agi-assistant/internal/domain/tool"
)

// NewMCPTool 创建一个调用外部 HTTP 端点的 MCP 兼容工具。
// 请求体为 JSON 对象（params），响应体作为工具结果返回。
func NewMCPTool(name, description, endpoint string, params []tool.Param) tool.Tool {
	return tool.Tool{
		Name:        name,
		Description: description,
		Parameters:  params,
		IsMCP:       true,
		Execute: func(p map[string]interface{}) (string, error) {
			body, err := json.Marshal(p)
			if err != nil {
				return "", fmt.Errorf("序列化参数失败: %w", err)
			}
			resp, err := http.Post(endpoint, "application/json", bytes.NewReader(body)) //nolint
			if err != nil {
				return "", fmt.Errorf("MCP 请求失败 [%s]: %w", endpoint, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 400 {
				return "", fmt.Errorf("MCP 返回错误状态 %d [%s]", resp.StatusCode, endpoint)
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", fmt.Errorf("读取 MCP 响应失败: %w", err)
			}
			return string(data), nil
		},
	}
}
