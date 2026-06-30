// restore.go — 启动期从 PostgreSQL 恢复长期记忆 + RAG chunks，
// 以及把 KGStore 与 GraphMemory 串起来。
//
// V2 多租户重构：preference / chat_history 不再启动期 restore——
//   - preference 跨用户全量加载没意义（每用户只需要自己的）
//   - chat_history 改成请求级 lazy load（按 userID + limit 即查即用）
//
// 仅 LTM 仍走启动期全量加载——它是单进程内全用户共享缓存，
// 召回时由 RecallByFilter 按 userID 过滤实现隔离。
package chat

import (
	"log"

	"agi-assistant/internal/domain/knowledge"
	graphmem "agi-assistant/internal/domain/memory/graph"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/infrastructure/llm"
)

// restoreFromDB 启动时从 PostgreSQL 恢复长期记忆。
// preference / chat_history 改为请求级懒加载，本函数不再处理。
func (a *UnifiedAgent) restoreFromDB() {
	// 恢复长期记忆（含所有用户的条目；召回时由 UserID 过滤实现隔离）
	rows := a.repos.ltm.Load()
	for _, row := range rows {
		a.mem.ltm.StoreItem(longterm.Item{
			ID:               row.ID,
			UserID:           row.UserID,
			Content:          row.Content,
			Importance:       row.Importance,
			Embedding:        row.Embedding,
			CreatedAt:        row.CreatedAt,
			LastAccessed:     row.LastAccessed,
			Category:         row.Category,
			Tags:             row.Tags,
			SlotHint:         row.SlotHint,
			Quarantined:      row.Quarantined,
			QuarantineReason: row.QuarantineReason,
			Superseded:       row.Superseded,
			SupersededAt:     row.SupersededAt,
			Supersedes:       row.Supersedes,
		})
	}

	if len(rows) > 0 {
		log.Printf("✅ 长期记忆恢复：%d 条（多用户，按 user_id 过滤召回）", len(rows))
	}
}

// restoreRAGFromDB 从 PostgreSQL 加载持久化的 RAG chunks 到 TF 兜底索引
func (a *UnifiedAgent) restoreRAGFromDB() {
	chunkRows, err := a.repos.ragChunk.LoadAll()
	if err != nil || len(chunkRows) == 0 {
		return
	}
	var chunks []rag.Chunk
	for i, row := range chunkRows {
		chunks = append(chunks, rag.Chunk{ID: i, Content: row.Content})
	}
	a.rag.RestoreChunks(chunks)
	log.Printf("✅ RAG chunks 恢复：%d 条", len(chunks))
}

// initKnowledgeGraph 初始化 Neo4j 知识图谱存储，并注入到 RAG 引擎 + GraphMemory
func (a *UnifiedAgent) initKnowledgeGraph() {
	kg := knowledge.NewKGStore(a.cfg, func(systemPrompt, userMsg string) string {
		return a.llm.Chat(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
	})
	a.kg = kg
	a.rag.SetKGStore(kg)

	// 构建图记忆层（包装现有 ltm）；复用 kg 的 Neo4j 客户端避免双连接
	gm := graphmem.New(a.mem.ltm, kg, kg.Client(), a.cfg.MemoryConsolidationSimilarity)
	gm.SyncPrevID() // 从 DB 恢复后对齐 prevID
	a.mem.attachGraph(gm)

	if kg.Available() {
		log.Printf("🕸️  知识图谱已就绪（Neo4j），RAG 升级为三路混合检索，记忆系统已接入图层")
	} else {
		log.Printf("ℹ️  Neo4j 不可用，RAG 保持双路检索，记忆系统退化为纯向量模式")
	}
}

// KG 暴露知识图谱实例，供 HTTP handler 或记忆模块使用
func (a *UnifiedAgent) KG() *knowledge.KGStore { return a.kg }
