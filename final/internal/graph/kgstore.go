package graph

import (
	"context"
	"final/config"
	"fmt"
	"log"
	"sort"
)

// KGStore 在 Neo4jStore 之上封装 RAG 专用的图操作：
//   - IndexDocument：文档摄入时写入实体节点和关系边
//   - DeleteDocument：删除文档及其关联的孤立节点
//   - Search：根据查询实体做 1~2 跳子图扩展，返回关联的 ChunkID 列表
//   - ExpandMemoryNeighbors：记忆图扩展（供 memory 包复用）
type KGStore struct {
	neo4j     *Neo4jStore
	maxHops   int
	kgWeight  float64
	extractor *Extractor
}

// NewKGStore 创建知识图谱存储
func NewKGStore(cfg *config.APIConfig, llmFn func(systemPrompt, userMsg string) string) *KGStore {
	neo := NewNeo4jStore(cfg)
	return &KGStore{
		neo4j:     neo,
		maxHops:   cfg.KGMaxHops,
		kgWeight:  cfg.KGWeight,
		extractor: NewExtractor(llmFn),
	}
}

// Available 图存储是否可用
func (ks *KGStore) Available() bool { return ks.neo4j.Available() }

// Close 关闭底层连接
func (ks *KGStore) Close() { ks.neo4j.Close() }

// ─────────────────────────────── 文档摄入 ────────────────────────────────────

// IndexDocument 为一批 chunks 抽取实体关系并写入图
// 以异步方式调用，不阻塞主 Ingest 流程
func (ks *KGStore) IndexDocument(docHash string, chunks []ChunkRef) {
	if !ks.neo4j.Available() {
		return
	}
	for _, c := range chunks {
		result := ks.extractor.Extract(c.Content)
		if len(result.Entities) == 0 {
			continue
		}
		ctx := context.Background()
		// 写入实体节点
		for _, ent := range result.Entities {
			ent.DocHash = docHash
			ent.ChunkID = c.ID
			ks.upsertEntity(ctx, ent)
		}
		// 写入关系边
		for _, rel := range result.Relations {
			rel.DocHash = docHash
			rel.ChunkID = c.ID
			ks.upsertRelation(ctx, rel)
		}
	}
	log.Printf("🕸️  知识图谱索引完成：docHash=%s，chunks=%d", docHash, len(chunks))
}

// ChunkRef 是 KGStore 摄入时需要的 chunk 信息（避免直接依赖 rag 包形成循环）
type ChunkRef struct {
	ID      int
	Content string
}

// upsertEntity MERGE 实体节点（幂等）
func (ks *KGStore) upsertEntity(ctx context.Context, ent Entity) {
	sess := ks.neo4j.session()
	defer sess.Close(ctx)
	query := `MERGE (e:Entity {name: $name})
	          SET e.type = $type, e.doc_hash = $doc_hash, e.chunk_id = $chunk_id`
	_, err := sess.Run(ctx, query, map[string]any{
		"name":     ent.Name,
		"type":     string(ent.Type),
		"doc_hash": ent.DocHash,
		"chunk_id": ent.ChunkID,
	})
	if err != nil {
		log.Printf("⚠️  Neo4j upsertEntity 失败 (%s): %v", ent.Name, err)
	}
}

// upsertRelation MERGE 关系边（幂等）
func (ks *KGStore) upsertRelation(ctx context.Context, rel Relation) {
	sess := ks.neo4j.session()
	defer sess.Close(ctx)
	// 动态关系类型无法用参数传递，必须拼入查询字符串
	// 安全性由 isValidRelType 保证（extractor 已过滤非法类型）
	query := `MERGE (a:Entity {name: $from})
	          MERGE (b:Entity {name: $to})
	          MERGE (a)-[r:` + rel.RelType + ` {doc_hash: $doc_hash}]->(b)
	          SET r.chunk_id = $chunk_id`
	_, err := sess.Run(ctx, query, map[string]any{
		"from":     rel.FromName,
		"to":       rel.ToName,
		"doc_hash": rel.DocHash,
		"chunk_id": rel.ChunkID,
	})
	if err != nil {
		log.Printf("⚠️  Neo4j upsertRelation 失败 (%s→%s): %v", rel.FromName, rel.ToName, err)
	}
}

// ─────────────────────────────── 文档删除 ────────────────────────────────────

// DeleteDocument 删除与 docHash 关联的所有关系，并清理孤立实体节点
func (ks *KGStore) DeleteDocument(docHash string) {
	if !ks.neo4j.Available() {
		return
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)

	// 删除所有归属此文档的关系
	_, err := sess.Run(ctx,
		`MATCH ()-[r {doc_hash: $doc_hash}]-() DELETE r`,
		map[string]any{"doc_hash": docHash})
	if err != nil {
		log.Printf("⚠️  Neo4j 删除文档关系失败: %v", err)
	}
	// 清理孤立实体节点
	_, err = sess.Run(ctx,
		`MATCH (e:Entity) WHERE NOT (e)--() AND e.doc_hash = $doc_hash DELETE e`,
		map[string]any{"doc_hash": docHash})
	if err != nil {
		log.Printf("⚠️  Neo4j 清理孤立节点失败: %v", err)
	}
}

// ─────────────────────────────── 图检索 ──────────────────────────────────────

// Search 根据查询文本抽取实体，执行 1~2 跳子图遍历，返回关联的 ChunkID
func (ks *KGStore) Search(queryText string, topK int) []GraphSearchResult {
	if !ks.neo4j.Available() {
		return nil
	}

	// 抽取查询中的实体
	extracted := ks.extractor.Extract(queryText)
	if len(extracted.Entities) == 0 {
		return nil
	}

	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)

	// 构建实体名列表
	names := make([]string, 0, len(extracted.Entities))
	for _, e := range extracted.Entities {
		names = append(names, e.Name)
	}

	// Cypher：从命中节点出发做最多 maxHops 跳遍历，收集相关 chunk_id
	// 每跳权重递减（直接命中 > 1跳 > 2跳）
	hops := ks.maxHops
	if hops <= 0 {
		hops = 2
	}
	query := `
	MATCH (e:Entity) WHERE e.name IN $names
	CALL apoc.path.subgraphNodes(e, {
	  maxLevel: $hops,
	  relationshipFilter: "RELATES_TO|PART_OF|CAUSES|DESCRIBES|MENTIONS|WORKS_FOR|LOCATED_IN"
	})
	YIELD node AS neighbor
	WHERE neighbor:Entity AND neighbor.chunk_id IS NOT NULL
	WITH e.name AS seed, neighbor.name AS nb, neighbor.chunk_id AS cid,
	     toInteger(apoc.node.degree(neighbor)) AS degree
	RETURN cid, collect(DISTINCT seed) AS seeds, collect(DISTINCT nb) AS neighbors, max(degree) AS deg
	ORDER BY size(seeds) DESC, deg DESC
	LIMIT $limit`

	records, err := sess.Run(ctx, query, map[string]any{
		"names": names,
		"hops":  int64(hops),
		"limit": int64(topK * 3),
	})
	if err != nil {
		// APOC 不可用时降级为直接节点匹配
		return ks.searchDirect(ctx, names, topK)
	}

	// 收集结果
	type rawResult struct {
		chunkID  int
		seeds    []string
		neighbors []string
		degree   int64
	}
	var raw []rawResult
	for records.Next(ctx) {
		rec := records.Record()
		cid, _ := rec.Get("cid")
		seeds, _ := rec.Get("seeds")
		nbs, _ := rec.Get("neighbors")
		deg, _ := rec.Get("deg")

		chunkID := toInt(cid)
		if chunkID < 0 {
			continue
		}
		r := rawResult{
			chunkID:  chunkID,
			seeds:    toStringSlice(seeds),
			neighbors: toStringSlice(nbs),
			degree:   toInt64(deg),
		}
		raw = append(raw, r)
	}
	if err := records.Err(); err != nil {
		log.Printf("⚠️  Neo4j 图检索 records 错误: %v", err)
	}

	// 计算分数：命中种子越多 + 图中心度越高 → 分越高
	seen := make(map[int]bool)
	var results []GraphSearchResult
	for _, r := range raw {
		if seen[r.chunkID] {
			continue
		}
		seen[r.chunkID] = true
		score := float64(len(r.seeds))*0.6 + float64(r.degree)*0.01
		score *= ks.kgWeight
		results = append(results, GraphSearchResult{
			ChunkID:  r.chunkID,
			Score:    score,
			Entities: r.seeds,
			HopPath:  r.neighbors,
		})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

// searchDirect APOC 不可用时的降级版本：直接匹配实体所在 chunk
func (ks *KGStore) searchDirect(ctx context.Context, names []string, topK int) []GraphSearchResult {
	sess := ks.neo4j.session()
	defer sess.Close(ctx)

	records, err := sess.Run(ctx,
		`MATCH (e:Entity) WHERE e.name IN $names AND e.chunk_id IS NOT NULL
		 RETURN e.chunk_id AS cid, e.name AS name ORDER BY cid LIMIT $limit`,
		map[string]any{"names": names, "limit": int64(topK)})
	if err != nil {
		return nil
	}

	seen := make(map[int]bool)
	var results []GraphSearchResult
	for records.Next(ctx) {
		rec := records.Record()
		cid := toInt(rec.Values[0])
		name := toString(rec.Values[1])
		if seen[cid] {
			continue
		}
		seen[cid] = true
		results = append(results, GraphSearchResult{
			ChunkID:  cid,
			Score:    ks.kgWeight,
			Entities: []string{name},
		})
	}
	return results
}

// ─────────────────────────────── 记忆图操作 ──────────────────────────────────

// UpsertMemoryNode 插入或更新记忆节点（供 graph_memory 使用）
func (ks *KGStore) UpsertMemoryNode(memID int, content string, importance float64) {
	if !ks.neo4j.Available() {
		return
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)
	_, err := sess.Run(ctx,
		`MERGE (m:Memory {mem_id: $id})
		 SET m.content = $content, m.importance = $importance`,
		map[string]any{"id": int64(memID), "content": content, "importance": importance})
	if err != nil {
		log.Printf("⚠️  Neo4j UpsertMemoryNode 失败 (id=%d): %v", memID, err)
	}
}

// AddMemoryEdge 在两条记忆之间添加关系边（供 graph_memory 使用）
// edgeType: FOLLOWS | SIMILAR_TO | CAUSES | BELONGS_TO
func (ks *KGStore) AddMemoryEdge(fromID, toID int, edgeType string, weight float64) {
	if !ks.neo4j.Available() {
		return
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)
	query := `MATCH (a:Memory {mem_id: $from}), (b:Memory {mem_id: $to})
	          MERGE (a)-[r:` + edgeType + `]->(b)
	          SET r.weight = $weight`
	_, err := sess.Run(ctx, query, map[string]any{
		"from": int64(fromID), "to": int64(toID), "weight": weight,
	})
	if err != nil {
		log.Printf("⚠️  Neo4j AddMemoryEdge 失败 (%d→%d): %v", fromID, toID, err)
	}
}

// ExpandMemoryNeighbors 从种子记忆 ID 出发，1跳扩展相邻记忆 ID
func (ks *KGStore) ExpandMemoryNeighbors(seedIDs []int, hops int) []int {
	if !ks.neo4j.Available() || len(seedIDs) == 0 {
		return nil
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)

	int64Seeds := make([]int64, len(seedIDs))
	for i, id := range seedIDs {
		int64Seeds[i] = int64(id)
	}
	records, err := sess.Run(ctx,
		`MATCH (m:Memory) WHERE m.mem_id IN $ids
		 MATCH (m)-[:FOLLOWS|SIMILAR_TO|CAUSES|BELONGS_TO*1..`+intStr(hops)+`]-(n:Memory)
		 WHERE NOT n.mem_id IN $ids
		 RETURN DISTINCT n.mem_id AS id`,
		map[string]any{"ids": int64Seeds})
	if err != nil {
		return nil
	}

	var result []int
	for records.Next(ctx) {
		rec := records.Record()
		result = append(result, toInt(rec.Values[0]))
	}
	return result
}

// DeleteMemoryNode 删除一条记忆节点及其所有边
func (ks *KGStore) DeleteMemoryNode(memID int) {
	if !ks.neo4j.Available() {
		return
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)
	_, err := sess.Run(ctx,
		`MATCH (m:Memory {mem_id: $id}) DETACH DELETE m`,
		map[string]any{"id": int64(memID)})
	if err != nil {
		log.Printf("⚠️  Neo4j DeleteMemoryNode 失败 (id=%d): %v", memID, err)
	}
}

// GetHighCentralityMemoryIDs 在待删除列表中找出图中入度较高（受保护）的节点
func (ks *KGStore) GetHighCentralityMemoryIDs(candidates []int, threshold int) []int {
	if !ks.neo4j.Available() || len(candidates) == 0 {
		return nil
	}
	ctx := context.Background()
	sess := ks.neo4j.session()
	defer sess.Close(ctx)

	int64IDs := make([]int64, len(candidates))
	for i, id := range candidates {
		int64IDs[i] = int64(id)
	}
	records, err := sess.Run(ctx,
		`MATCH (m:Memory) WHERE m.mem_id IN $ids
		 WITH m, size([(m)<-[]-() | 1]) AS indegree
		 WHERE indegree >= $threshold
		 RETURN m.mem_id AS id`,
		map[string]any{"ids": int64IDs, "threshold": int64(threshold)})
	if err != nil {
		return nil
	}
	var result []int
	for records.Next(ctx) {
		rec := records.Record()
		result = append(result, toInt(rec.Values[0]))
	}
	return result
}

// ─────────────────────────────── 内部工具 ────────────────────────────────────

func toInt(v any) int {
	switch x := v.(type) {
	case int64:
		return int(x)
	case int:
		return x
	case float64:
		return int(x)
	}
	return -1
}

func toInt64(v any) int64 {
	if x, ok := v.(int64); ok {
		return x
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toStringSlice(v any) []string {
	if arr, ok := v.([]any); ok {
		s := make([]string, 0, len(arr))
		for _, a := range arr {
			if str, ok := a.(string); ok {
				s = append(s, str)
			}
		}
		return s
	}
	return nil
}

func intStr(n int) string {
	return fmt.Sprintf("%d", n)
}
