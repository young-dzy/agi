// Package handler 实现所有 HTTP API 的请求处理逻辑，并注册到 ServeMux。
package handler

import (
	"agi-assistant/config"
	"agi-assistant/internal/application/chat"
	"agi-assistant/internal/domain/document"
	"agi-assistant/internal/domain/tool"
	toolimpl "agi-assistant/internal/infrastructure/tool"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Server 聚合 Agent 引用，挂载所有 HTTP 路由
type Server struct {
	agent *chat.UnifiedAgent
	cfg   *config.APIConfig
}

// New 创建 Server 并注册所有路由到 mux
func New(a *chat.UnifiedAgent, cfg *config.APIConfig) *Server {
	s := &Server{agent: a, cfg: cfg}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	http.HandleFunc("/api/chat", s.chat)
	http.HandleFunc("/api/chat/stream", s.chatStream)
	http.HandleFunc("/api/chat/cancel", s.chatCancel)
	http.HandleFunc("/api/upload", s.upload)
	http.HandleFunc("/api/docs/delete", s.docsDelete)
	http.HandleFunc("/api/documents", s.documents)
	http.HandleFunc("/api/documents/", s.documentByID)
	http.HandleFunc("/api/memory", s.memory)
	http.HandleFunc("/api/tools", s.toolsList)
	http.HandleFunc("/api/tools/mcp", s.registerMCPTool)
	http.HandleFunc("/api/snapshots", s.snapshots)
	http.HandleFunc("/api/status", s.status)
}

// ─────────────────────────────── 路由处理 ────────────────────────────────

// POST /api/chat — 统一对话入口（同步模式，向后兼容）
func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Message       string   `json:"message"`
		UseRAG        bool     `json:"use_rag"`
		SelectedTools []string `json:"selected_tools"`
		Explicit      bool     `json:"explicit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	opts := chat.ChatOptions{
		UseRAG:        req.UseRAG,
		SelectedTools: req.SelectedTools,
		Explicit:      req.Explicit,
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	resp := s.agent.ProcessContext(ctx, req.Message, opts)
	writeJSON(w, resp)
}

// POST /api/chat/stream — SSE 流式对话入口
func (s *Server) chatStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Message       string   `json:"message"`
		UseRAG        bool     `json:"use_rag"`
		SelectedTools []string `json:"selected_tools"`
		Explicit      bool     `json:"explicit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, canFlush := w.(http.Flusher)

	sendSSE := func(event string, data interface{}) {
		jsonData, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, jsonData)
		if canFlush {
			flusher.Flush()
		}
	}

	opts := chat.ChatOptions{
		UseRAG:        req.UseRAG,
		SelectedTools: req.SelectedTools,
		Explicit:      req.Explicit,
	}

	sendSSE("start", map[string]interface{}{"message": req.Message})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	notify := r.Context().Done()
	go func() {
		<-notify
		cancel()
	}()

	s.agent.ProcessStream(ctx, req.Message, opts, func(evt chat.StreamEvent) {
		sendSSE(evt.Type, evt.Data)
	})
}

// POST /api/chat/cancel — 取消当前正在执行的对话任务
func (s *Server) chatCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.agent.Cancel()
	writeJSON(w, map[string]interface{}{"ok": true, "message": "已发送取消信号"})
}

// POST /api/upload — 上传文档到 RAG 知识库
func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<20)

	parsed, err := parseUploadDocument(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if parsed.NeedsOCR {
		writeJSON(w, map[string]interface{}{
			"filename":      parsed.Filename,
			"content_type":  parsed.ContentType,
			"parser":        parsed.Parser,
			"pages":         parsed.Pages,
			"text_chars":    parsed.TextChars,
			"needs_ocr":     true,
			"chunk_count":   0,
			"parent_count":  0,
			"indexed_count": 0,
			"doc_hash":      "",
			"chunks":        nil,
			"message":       "PDF 文本抽取结果过少，可能是扫描件，需要 OCR 后再入库",
		})
		return
	}

	ingested := s.agent.RAG().Ingest(parsed.Content)
	writeJSON(w, map[string]interface{}{
		"filename":      parsed.Filename,
		"content_type":  parsed.ContentType,
		"parser":        parsed.Parser,
		"pages":         parsed.Pages,
		"text_chars":    parsed.TextChars,
		"needs_ocr":     parsed.NeedsOCR,
		"chunk_count":   ingested.ChunkCount,
		"parent_count":  ingested.ParentCount,
		"indexed_count": ingested.IndexedCount,
		"chunk_preview": ingested.ChunkPreview,
		"doc_hash":      ingested.DocHash,
		"chunks":        s.agent.RAG().Chunks(),
	})
}

func parseUploadDocument(r *http.Request) (document.ParseResult, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			return document.ParseResult{}, fmt.Errorf("invalid multipart upload: %w", err)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			return document.ParseResult{}, fmt.Errorf("file is required")
		}
		defer file.Close()
		data, err := io.ReadAll(file)
		if err != nil {
			return document.ParseResult{}, fmt.Errorf("read uploaded file failed: %w", err)
		}
		return document.ParseBytes(header.Filename, header.Header.Get("Content-Type"), data)
	}

	var req struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return document.ParseResult{}, fmt.Errorf("invalid request body")
	}
	return document.ParseBytes("upload.txt", "text/plain", []byte(req.Content))
}

// POST /api/docs/delete — 删除指定文档的所有 chunks
func (s *Server) docsDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DocHash string `json:"doc_hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.DocHash == "" {
		http.Error(w, "doc_hash is required", http.StatusBadRequest)
		return
	}
	if err := s.agent.RAG().Delete(req.DocHash); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "doc_hash": req.DocHash})
}

// GET/POST /api/documents — 列出或写入本地文档库
func (s *Server) documents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		docs, err := s.agent.ListDocuments()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{"documents": docs})
	case http.MethodPost:
		var req struct {
			DocumentID  string                 `json:"document_id"`
			Title       string                 `json:"title"`
			DocType     string                 `json:"doc_type"`
			Source      string                 `json:"source"`
			CreatedBy   string                 `json:"created_by"`
			ContentMD   string                 `json:"content_md"`
			Summary     string                 `json:"summary"`
			Metadata    map[string]interface{} `json:"metadata"`
			IngestToRAG bool                   `json:"ingest_to_rag"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		res, err := s.agent.WriteDocument(document.WriteRequest{
			DocumentID: req.DocumentID,
			Title:      req.Title,
			DocType:    req.DocType,
			Source:     req.Source,
			CreatedBy:  req.CreatedBy,
			ContentMD:  req.ContentMD,
			Summary:    req.Summary,
			Metadata:   req.Metadata,
		}, req.IngestToRAG)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, res)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET /api/documents/{id} 或 POST /api/documents/{id}/ingest
func (s *Server) documentByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/documents/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "document_id is required", http.StatusBadRequest)
		return
	}
	documentID := parts[0]
	if r.Method == http.MethodGet && len(parts) == 1 {
		doc, ver, err := s.agent.GetDocument(documentID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]interface{}{"document": doc, "version": ver})
		return
	}
	if r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "ingest" {
		var req struct {
			VersionID string `json:"version_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		res, err := s.agent.IngestDocument(documentID, req.VersionID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, res)
		return
	}
	http.Error(w, "Not found", http.StatusNotFound)
}

// GET /api/memory — 查看三层记忆状态
func (s *Server) memory(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"short_term": s.agent.ShortTerm().Snapshot(),
		"long_term":  s.agent.LongTerm().Snapshot(),
		"preference": s.agent.Preferences().Snapshot(),
	})
}

// GET /api/tools — 列出所有可用工具
func (s *Server) toolsList(w http.ResponseWriter, r *http.Request) {
	type toolInfo struct {
		Name   string       `json:"name"`
		Desc   string       `json:"description"`
		IsMCP  bool         `json:"is_mcp,omitempty"`
		Params []tool.Param `json:"params,omitempty"`
	}
	var list []toolInfo
	for _, t := range s.agent.Tools() {
		list = append(list, toolInfo{Name: t.Name, Desc: t.Description, IsMCP: t.IsMCP, Params: t.Parameters})
	}
	writeJSON(w, list)
}

// POST /api/tools/mcp — 动态注册一个 MCP 工具
func (s *Server) registerMCPTool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name        string       `json:"name"`
		Description string       `json:"description"`
		Endpoint    string       `json:"endpoint"`
		Params      []tool.Param `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Endpoint == "" {
		http.Error(w, "name and endpoint are required", http.StatusBadRequest)
		return
	}
	t := toolimpl.NewMCPTool(req.Name, req.Description, req.Endpoint, req.Params)
	s.agent.RegisterTool(t)
	writeJSON(w, map[string]interface{}{"ok": true, "name": req.Name})
}

// GET /api/snapshots — 列出任务执行快照摘要
func (s *Server) snapshots(w http.ResponseWriter, r *http.Request) {
	snaps := s.agent.Snapshots()
	infos := make([]map[string]interface{}, 0, len(snaps))
	for i, snap := range snaps {
		infos = append(infos, map[string]interface{}{
			"index":     i,
			"timestamp": snap.Timestamp,
			"steps":     len(snap.State.Steps),
		})
	}
	writeJSON(w, infos)
}

// GET /api/status — 系统状态与配置摘要
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.agent.Status())
}

// ─────────────────────────────── 工具函数 ────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
