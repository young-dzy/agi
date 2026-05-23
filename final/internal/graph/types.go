// Package graph 实现基于 Neo4j 的知识图谱功能：
//   - Entity / Relation 类型定义
//   - KGStore：文档摄入时建图，查询时三跳子图扩展检索
//   - Extractor：通过 LLM 从文本中抽取实体和关系
//
// 所有操作在 Neo4j 不可用时均优雅降级（返回空结果，不阻塞主流程）。
package graph

// EntityType 实体类型枚举
type EntityType string

const (
	EntityPerson   EntityType = "Person"
	EntityOrg      EntityType = "Organization"
	EntityLocation EntityType = "Location"
	EntityConcept  EntityType = "Concept"
	EntityEvent    EntityType = "Event"
	EntityProduct  EntityType = "Product"
	EntityUnknown  EntityType = "Unknown"
)

// Entity 是知识图谱中的一个节点
type Entity struct {
	Name    string     `json:"name"`
	Type    EntityType `json:"type"`
	DocHash string     `json:"doc_hash,omitempty"`
	ChunkID int        `json:"chunk_id,omitempty"`
}

// Relation 是两个实体之间的有向边
type Relation struct {
	FromName string  `json:"from"`
	ToName   string  `json:"to"`
	RelType  string  `json:"rel_type"` // RELATES_TO / PART_OF / CAUSES / DESCRIBES / MENTIONS
	Weight   float64 `json:"weight,omitempty"`
	DocHash  string  `json:"doc_hash,omitempty"`
	ChunkID  int     `json:"chunk_id,omitempty"`
}

// GraphSearchResult 是一次图检索的单条结果
type GraphSearchResult struct {
	ChunkID  int      `json:"chunk_id"`
	Score    float64  `json:"score"`    // 基于路径跳数和匹配数量的综合分
	Entities []string `json:"entities"` // 命中的实体名称
	HopPath  []string `json:"hop_path"` // 遍历路径（可解释性）
}

// ExtractResult 是 Extractor.Extract 的输出
type ExtractResult struct {
	Entities  []Entity  `json:"entities"`
	Relations []Relation `json:"relations"`
}
