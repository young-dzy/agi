// status.go — UnifiedAgent 的状态视图模型聚合。
//
// handler 不再直接读 RAG/memory/Preferences/InfraStatus 等内部组件——
// status 端点通过 Status() 拿到一个聚合 map，结构保持与重构前一致以兼容前端。
package chat

// InfraStatus 暴露平台层连接健康快照（供 status 端点使用）
func (a *UnifiedAgent) InfraStatus() map[string]string {
	// 拷贝避免外部修改
	out := make(map[string]string, len(a.infraStatus))
	for k, v := range a.infraStatus {
		out[k] = v
	}
	return out
}

// Status 构造系统状态视图模型，供 GET /api/status 渲染。
func (a *UnifiedAgent) Status() map[string]interface{} {
	// RAG chunk 预览（最多 60 字符）
	var chunkPreviews []map[string]interface{}
	for _, c := range a.rag.Chunks() {
		preview := c.Content
		if runeCount(preview) > 60 {
			runes := []rune(preview)
			preview = string(runes[:60]) + "..."
		}
		chunkPreviews = append(chunkPreviews, map[string]interface{}{
			"id":      c.ID,
			"content": preview,
		})
	}
	return map[string]interface{}{
		"rag_loaded":       a.rag.Loaded,
		"rag_mode":         a.rag.Mode(),
		"rag_chunks":       chunkPreviews,
		"short_term_count": a.stm.Count(),
		"long_term_count":  a.ltm.Count(),
		"preferences":      a.pref.Snapshot(),
		"tools_count":      len(a.toolsSnapshot()),
		"llm_model":        a.cfg.LLMModel,
		"embedding_model":  a.cfg.EmbeddingModel,
		"is_mock":          !a.cfg.IsRealLLM(),
		"infrastructure":   a.InfraStatus(),
	}
}

// runeCount 返回字符串的 rune 数（unicode 安全的长度）
func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}
