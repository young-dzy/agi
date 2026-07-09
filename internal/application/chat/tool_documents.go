package chat

import (
	"encoding/json"
	"fmt"
	"strings"

	"agi-assistant/internal/domain/document"
	"agi-assistant/internal/domain/tool"
)

func (a *UnifiedAgent) registerDocumentTools() {
	for _, t := range []tool.Tool{
		a.writeDocumentTool(),
		a.listDocumentsTool(),
		a.readDocumentTool(),
		a.ingestDocumentTool(),
	} {
		a.RegisterTool(t)
	}
}

func (a *UnifiedAgent) writeDocumentTool() tool.Tool {
	return tool.Tool{
		Name:        "write_document",
		Description: "将 Markdown 文档写入本地文档库，可选择同步入库 RAG。适合保存报告、总结、研究结果。",
		Parameters: []tool.Param{
			{Name: "title", Type: "string", Description: "文档标题", Required: true},
			{Name: "content_md", Type: "string", Description: "Markdown 正文", Required: true},
			{Name: "doc_type", Type: "string", Description: "文档类型，如 report/note/summary", Required: false},
			{Name: "source", Type: "string", Description: "来源，如 agent_generated", Required: false},
			{Name: "summary", Type: "string", Description: "简短摘要", Required: false},
			{Name: "ingest_to_rag", Type: "boolean", Description: "是否写入后立即进入 RAG 索引", Required: false},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			title := paramString(params, "title")
			content := paramString(params, "content_md")
			if strings.TrimSpace(content) == "" {
				content = paramString(params, "content")
			}
			ingest := paramBool(params, "ingest_to_rag")
			res, err := a.WriteDocument(document.WriteRequest{
				Title:     title,
				DocType:   paramStringDefault(params, "doc_type", "report"),
				Source:    paramStringDefault(params, "source", document.DocumentSourceAgent),
				CreatedBy: "agent",
				ContentMD: content,
				Summary:   paramString(params, "summary"),
				Metadata: map[string]interface{}{
					"tool": "write_document",
				},
			}, ingest)
			if err != nil {
				return "", err
			}
			return jsonString(res), nil
		},
	}
}

func (a *UnifiedAgent) listDocumentsTool() tool.Tool {
	return tool.Tool{
		Name:        "list_documents",
		Description: "列出本地文档库中的文档。",
		Parameters:  nil,
		Execute: func(params map[string]interface{}) (string, error) {
			docs, err := a.ListDocuments()
			if err != nil {
				return "", err
			}
			return jsonString(map[string]interface{}{"documents": docs}), nil
		},
	}
}

func (a *UnifiedAgent) readDocumentTool() tool.Tool {
	return tool.Tool{
		Name:        "read_document",
		Description: "读取本地文档库中的指定文档最新版本。",
		Parameters: []tool.Param{
			{Name: "document_id", Type: "string", Description: "文档 ID", Required: true},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			doc, ver, err := a.GetDocument(paramString(params, "document_id"))
			if err != nil {
				return "", err
			}
			return jsonString(map[string]interface{}{"document": doc, "version": ver}), nil
		},
	}
}

func (a *UnifiedAgent) ingestDocumentTool() tool.Tool {
	return tool.Tool{
		Name:        "ingest_document",
		Description: "将本地文档库中的文档版本切分并写入 RAG 索引。",
		Parameters: []tool.Param{
			{Name: "document_id", Type: "string", Description: "文档 ID", Required: true},
			{Name: "version_id", Type: "string", Description: "版本 ID，不填则使用最新版本", Required: false},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			res, err := a.IngestDocument(paramString(params, "document_id"), paramString(params, "version_id"))
			if err != nil {
				return "", err
			}
			return jsonString(res), nil
		},
	}
}

func paramString(params map[string]interface{}, key string) string {
	if params == nil {
		return ""
	}
	v, ok := params[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func paramStringDefault(params map[string]interface{}, key string, fallback string) string {
	if v := paramString(params, key); v != "" {
		return v
	}
	return fallback
}

func paramBool(params map[string]interface{}, key string) bool {
	if params == nil {
		return false
	}
	switch v := params[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true") || v == "1" || strings.EqualFold(v, "yes")
	default:
		return false
	}
}

func jsonString(v interface{}) string {
	data, _ := json.MarshalIndent(v, "", "  ")
	return string(data)
}
