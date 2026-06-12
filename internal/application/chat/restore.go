// restore.go — 启动期从 PostgreSQL 恢复偏好/长期记忆/聊天记录/RAG chunks，
// 以及把 KGStore 与 GraphMemory 串起来。
//
// 这些都是 agent.New 在并发组里调用的"对齐持久化与运行时状态"动作。
package chat

import (
	"log"

	"agi-assistant/internal/domain/knowledge"
	graphmem "agi-assistant/internal/domain/memory/graph"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/infrastructure/llm"
)

// restoreFromDB 启动时从 PostgreSQL 恢复跨会话偏好、长期记忆和聊天记录
func (a *UnifiedAgent) restoreFromDB() {
	// 恢复偏好
	prefs := a.prefRepo.Load("default")
	a.pref.SaveBatch(prefs)

	// 恢复长期记忆
	rows := a.ltmRepo.Load()
	for _, row := range rows {
		a.ltm.StoreItem(longterm.Item{
			ID:           row.ID,
			Content:      row.Content,
			Importance:   row.Importance,
			Embedding:    row.Embedding,
			CreatedAt:    row.CreatedAt,
			LastAccessed: row.LastAccessed,
		})
	}

	// 恢复聊天记录到短期记忆（最近 N 条）
	chatLimit := a.cfg.ShortTermMaxTurns * 2 // 每轮 = user + assistant
	history := a.chatRepo.Load(chatLimit)
	for _, h := range history {
		a.stm.Add(h.Role, h.Content)
	}

	if len(prefs) > 0 || len(rows) > 0 || len(history) > 0 {
		log.Printf("✅ 记忆恢复：%d 条偏好，%d 条长期记忆，%d 条聊天记录", len(prefs), len(rows), len(history))
	}
}

// restoreRAGFromDB 从 PostgreSQL 加载持久化的 RAG chunks 到 TF 兜底索引
func (a *UnifiedAgent) restoreRAGFromDB() {
	chunkRows, err := a.ragChunkRepo.LoadAll()
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
	a.graphMem = graphmem.New(a.ltm, kg, kg.Client(), a.cfg.MemoryConsolidationSimilarity)
	a.graphMem.SyncPrevID() // 从 DB 恢复后对齐 prevID

	if kg.Available() {
		log.Printf("🕸️  知识图谱已就绪（Neo4j），RAG 升级为三路混合检索，记忆系统已接入图层")
	} else {
		log.Printf("ℹ️  Neo4j 不可用，RAG 保持双路检索，记忆系统退化为纯向量模式")
	}
}

// KG 暴露知识图谱实例，供 HTTP handler 或记忆模块使用
func (a *UnifiedAgent) KG() *knowledge.KGStore { return a.kg }
