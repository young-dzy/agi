// Package memory — GraphMemory 在现有 LongTerm 基础上叠加 Neo4j 图层：
//
//   节点类型：(:Memory {mem_id, content, importance})
//   边类型：
//     FOLLOWS      — 时序相邻（上一条对话记忆 → 当前）
//     SIMILAR_TO   — 语义相似度超阈值（Store 时自动连接）
//     CAUSES       — 因果推断（LLM 提取，可选）
//     BELONGS_TO   — 话题归属（暂用 TopK 聚类近似）
//
// 核心能力：
//   Store    — 写入 LTM 的同时在图中创建节点，并建立 FOLLOWS + SIMILAR_TO 边
//   Recall   — 向量召回后沿图扩展，发现不直接相似但关联的历史记忆
//   Consolidate — 结合图中心度保护关键节点，避免高价值记忆被错误淘汰
package memory

import (
	"agi-ai-assitant/internal/graph"
	"log"
	"time"
)

// GraphMemory 是 LongTerm 的图增强包装层
type GraphMemory struct {
	ltm       *LongTerm
	kg        *graph.KGStore
	simThresh float64 // 建立 SIMILAR_TO 边的相似度阈值
	prevID    int     // 上一条存入记忆的 ID（用于 FOLLOWS 边）
}

// NewGraphMemory 创建图记忆层；kg 为 nil 时退化为纯 LongTerm
func NewGraphMemory(ltm *LongTerm, kg *graph.KGStore, simThreshold float64) *GraphMemory {
	if simThreshold <= 0 {
		simThreshold = 0.7
	}
	return &GraphMemory{
		ltm:       ltm,
		kg:        kg,
		simThresh: simThreshold,
		prevID:    -1,
	}
}

// LTM 暴露底层 LongTerm，供 agent 直接读取 Items/NeedConsolidation 等
func (gm *GraphMemory) LTM() *LongTerm { return gm.ltm }

// ─────────────────────────────── Store ───────────────────────────────────────

// Store 将记忆写入 LTM 并在图中建立节点和关联边
// 返回：(newItem bool, itemID int)
//   - newItem=false 表示因去重被跳过
//   - itemID 是写入的条目 ID（新增或已有更新后的 ID）
func (gm *GraphMemory) Store(content string, importance float64, embedding []float64) (bool, int) {
	added := gm.ltm.Store(content, importance, embedding)
	if !added {
		// 去重跳过：返回最相似的已有条目 ID
		return false, gm.findMostSimilarID(embedding)
	}

	// 获取刚加入的条目 ID
	if len(gm.ltm.Items) == 0 {
		return true, -1
	}
	newItem := gm.ltm.Items[len(gm.ltm.Items)-1]
	newID := newItem.ID

	// 图操作（异步，不阻塞主流程）
	if gm.kg != nil && gm.kg.Available() {
		go func() {
			// 1. 创建/更新节点
			gm.kg.UpsertMemoryNode(newID, content, importance)

			// 2. FOLLOWS 边：与上一条记忆建立时序关系
			if gm.prevID >= 0 {
				gm.kg.AddMemoryEdge(gm.prevID, newID, "FOLLOWS", 1.0)
			}

			// 3. SIMILAR_TO 边：遍历最近 N 条，与相似度超阈值的记忆建边
			gm.linkSimilarEdges(newItem, newID)
		}()
	}

	gm.prevID = newID
	return true, newID
}

// linkSimilarEdges 找出与 newItem 语义相近的已有条目，建立 SIMILAR_TO 边
func (gm *GraphMemory) linkSimilarEdges(newItem Item, newID int) {
	// 扫描最近 50 条（避免全量扫描）
	items := gm.ltm.Items
	start := len(items) - 51
	if start < 0 {
		start = 0
	}
	for i := start; i < len(items)-1; i++ {
		old := items[i]
		if old.ID == newID {
			continue
		}
		if len(old.Embedding) == 0 || len(newItem.Embedding) == 0 {
			continue
		}
		sim := cosine(old.Embedding, newItem.Embedding)
		if sim >= gm.simThresh {
			gm.kg.AddMemoryEdge(old.ID, newID, "SIMILAR_TO", sim)
		}
	}
}

// findMostSimilarID 在 LTM 中查找与 embedding 最相似的条目 ID（用于去重返回）
func (gm *GraphMemory) findMostSimilarID(embedding []float64) int {
	if len(embedding) == 0 || len(gm.ltm.Items) == 0 {
		return -1
	}
	bestID, bestSim := -1, 0.0
	for _, item := range gm.ltm.Items {
		if len(item.Embedding) != len(embedding) {
			continue
		}
		if s := cosine(embedding, item.Embedding); s > bestSim {
			bestSim, bestID = s, item.ID
		}
	}
	return bestID
}

// ─────────────────────────────── Recall ──────────────────────────────────────

// Recall 先做向量/TF召回，再用图扩展发现关联但不直接相似的记忆
func (gm *GraphMemory) Recall(query string, topK int, queryEmbedding []float64) []Item {
	// Step 1: 向量 / TF 召回种子
	seedItems := gm.ltm.Recall(query, topK, queryEmbedding)

	if gm.kg == nil || !gm.kg.Available() || len(seedItems) == 0 {
		return seedItems
	}

	// Step 2: 图扩展 — 1跳邻居
	seedIDs := make([]int, len(seedItems))
	for i, item := range seedItems {
		seedIDs[i] = item.ID
	}
	expandedIDs := gm.kg.ExpandMemoryNeighbors(seedIDs, 1)
	if len(expandedIDs) == 0 {
		return seedItems
	}

	// Step 3: 加载扩展节点，去重后合并
	idSet := make(map[int]bool)
	for _, id := range seedIDs {
		idSet[id] = true
	}
	var expanded []Item
	for _, id := range expandedIDs {
		if idSet[id] {
			continue
		}
		for _, item := range gm.ltm.Items {
			if item.ID == id {
				// 图扩展得到的条目给一个基础分（避免 0 分被过滤）
				item.Score = 0.45
				expanded = append(expanded, item)
				idSet[id] = true
				break
			}
		}
	}

	// Step 4: 合并，以 Score 降序保留 topK
	all := append(seedItems, expanded...)
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].Score > all[i].Score {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	if len(all) > topK {
		all = all[:topK]
	}
	return all
}

// ─────────────────────────────── Consolidate ─────────────────────────────────

// GraphAwareConsolidate 在 LTM.Consolidate 基础上：
//   1. 对图中入度高的节点提供保护（不轻易删除核心记忆）
//   2. 删除 LTM 条目时同步删除 Neo4j 节点
func (gm *GraphMemory) GraphAwareConsolidate() ConsolidationResult {
	result := gm.ltm.Consolidate()

	if gm.kg == nil || !gm.kg.Available() {
		return result
	}

	// 保护：图中入度 ≥ 3 的节点不在本次删除
	protected := gm.kg.GetHighCentralityMemoryIDs(result.DeleteFromDB, 3)
	if len(protected) > 0 {
		protSet := make(map[int]bool)
		for _, id := range protected {
			protSet[id] = true
		}
		filtered := result.DeleteFromDB[:0]
		for _, id := range result.DeleteFromDB {
			if !protSet[id] {
				filtered = append(filtered, id)
			}
		}
		log.Printf("🛡️  图中心度保护：%d 条记忆免于删除（入度≥3）", len(result.DeleteFromDB)-len(filtered))
		result.DeleteFromDB = filtered
	}

	// 同步删除 Neo4j 中对应节点
	go func() {
		for _, id := range result.DeleteFromDB {
			gm.kg.DeleteMemoryNode(id)
		}
	}()

	return result
}

// SyncPrevID 在从 DB 恢复记忆后调用，将 prevID 对齐到最新条目
func (gm *GraphMemory) SyncPrevID() {
	if len(gm.ltm.Items) > 0 {
		gm.prevID = gm.ltm.Items[len(gm.ltm.Items)-1].ID
	}
}

// UpdateNodeAfterMerge 记忆合并后更新 Neo4j 节点内容
func (gm *GraphMemory) UpdateNodeAfterMerge(item Item) {
	if gm.kg != nil && gm.kg.Available() {
		go func() {
			gm.kg.UpsertMemoryNode(item.ID, item.Content, item.Importance)
		}()
	}
}

// StoreItem 直接插入（从 DB 恢复），同步图节点
func (gm *GraphMemory) StoreItem(item Item) {
	gm.ltm.StoreItem(item)
	if gm.kg != nil && gm.kg.Available() {
		go gm.kg.UpsertMemoryNode(item.ID, item.Content, item.Importance)
	}
}

// Len 返回当前记忆条目数（等同 LTM）
func (gm *GraphMemory) Len() int { return len(gm.ltm.Items) }

// SetConsolidationConfig 代理到 LTM
func (gm *GraphMemory) SetConsolidationConfig(cfg *ConsolidationConfig) {
	gm.ltm.SetConsolidationConfig(cfg)
}

// NeedConsolidation 代理到 LTM
func (gm *GraphMemory) NeedConsolidation() bool { return gm.ltm.NeedConsolidation() }

// SyncLastItemPGID 代理到 LTM
func (gm *GraphMemory) SyncLastItemPGID(pgID int) {
	gm.ltm.SyncLastItemPGID(pgID)
	// 同步更新 prevID 到最新条目
	if len(gm.ltm.Items) > 0 {
		last := gm.ltm.Items[len(gm.ltm.Items)-1]
		gm.prevID = last.ID
		// 更新 Neo4j 节点 ID（SyncLastItemPGID 会修改最后一条 Item.ID）
		if gm.kg != nil && gm.kg.Available() {
			go func() {
				// 给 Neo4j 一点时间完成之前的异步操作
				time.Sleep(50 * time.Millisecond)
				gm.kg.UpsertMemoryNode(last.ID, last.Content, last.Importance)
			}()
		}
	}
}
