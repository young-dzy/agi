// Package longterm 长期记忆：支持语义向量召回（embedding 优先）或 TF 词袋降级。
//
// 核心特性：
//   - StoreClassified  写入前自动 dedup（cosine ≥ DedupThreshold）
//   - RecallByFilter   按 Schema-driven 过滤条件做语义召回
//   - Consolidate      周期性合并：衰减 → 去重/合并 → 过期淘汰
//
// Item 含 Category / Tags / SlotHint 三个字段，与 promptctx 的 SlotFilter 对齐——
// runtime 装配时按这些字段筛选适合各槽位的记忆，避免 Top-K 召回污染 prompt。
//
// 并发安全：内置 sync.RWMutex 串行化所有读写。Snapshot() 返回只读副本。
package longterm

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Item 是长期记忆的存储单元
type Item struct {
	ID           int       `json:"id"`
	Content      string    `json:"content"`
	Importance   float64   `json:"importance"` // 0~1，越高越重要
	Embedding    []float64 `json:"embedding,omitempty"`
	Score        float64   `json:"score,omitempty"` // 召回时的综合得分（不持久化）
	CreatedAt    time.Time `json:"created_at"`
	LastAccessed time.Time `json:"last_accessed"`
	// Schema-driven 装配字段（promptctx 包按这些字段过滤）
	Category string   `json:"category,omitempty"`  // 主类别：identity / preference / fact / episodic / tool_failure / policy / general
	Tags     []string `json:"tags,omitempty"`      // 自由标签
	SlotHint string   `json:"slot_hint,omitempty"` // 建议归属的 SlotKind 字符串
}

// RecallFilter 控制 RecallByFilter 的语义召回约束（与 promptctx.SlotFilter 同构）
type RecallFilter struct {
	Categories  []string
	RequireTags []string
	MinScore    float64
	TopK        int
	MaxAgeHours int
}

// ConsolidationConfig 记忆合并配置
type ConsolidationConfig struct {
	SimilarityThreshold float64 // 合并相似度阈值 (0~1)，超过此值触发合并
	DedupThreshold      float64 // 去重相似度阈值 (0~1)，超过此值视为重复
	TTLDays             int     // 过期天数 (0=永不过期)
	DecayRate           float64 // 每日衰减系数 (0~1, 如 0.995 表示每天保留 99.5%)
	MinImportance       float64 // 低于此重要性且超 TTL 的条目会被淘汰
	TriggerInterval     int     // 每存入 N 条新记忆后触发合并
}

// DefaultConsolidationConfig 返回默认合并配置
func DefaultConsolidationConfig() *ConsolidationConfig {
	return &ConsolidationConfig{
		SimilarityThreshold: 0.80,
		DedupThreshold:      0.95,
		TTLDays:             30,
		DecayRate:           0.995,
		MinImportance:       0.3,
		TriggerInterval:     5,
	}
}

// ConsolidationResult 记忆合并结果
type ConsolidationResult struct {
	Deduped      int    // 去重删除的条目数
	Merged       int    // 合并的条目数
	Expired      int    // 过期删除的条目数
	DeleteFromDB []int  // 需要从 PG 删除的 ID 列表
	UpdateInDB   []Item // 需要在 PG 更新的条目列表
}

// LongTerm 支持语义向量召回（embedding 优先）或 TF 词袋降级
type LongTerm struct {
	mu               sync.RWMutex
	Items            []Item
	vocabID          map[string]int
	vocab            []string
	nextID           int
	storeCount       int
	consolidationCfg *ConsolidationConfig
}

// New 创建长期记忆
func New() *LongTerm {
	return &LongTerm{vocabID: make(map[string]int)}
}

// SetConsolidationConfig 设置合并配置
func (m *LongTerm) SetConsolidationConfig(cfg *ConsolidationConfig) {
	m.mu.Lock()
	m.consolidationCfg = cfg
	m.mu.Unlock()
}

// Snapshot 返回 Items 的只读副本，调用方可安全遍历不被并发写入打断
func (m *LongTerm) Snapshot() []Item {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]Item, len(m.Items))
	copy(cp, m.Items)
	return cp
}

// Count 返回当前条目数（持锁读）
func (m *LongTerm) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Items)
}

// LastID 返回最后一条记忆的 ID；空时返回 -1
func (m *LongTerm) LastID() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.Items) == 0 {
		return -1
	}
	return m.Items[len(m.Items)-1].ID
}

// LastItem 返回最后一条记忆的副本和 ok 标记
func (m *LongTerm) LastItem() (Item, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.Items) == 0 {
		return Item{}, false
	}
	return m.Items[len(m.Items)-1], true
}

// FindByID 按 ID 查找记忆，返回副本和 ok 标记（持读锁）
func (m *LongTerm) FindByID(id int) (Item, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, it := range m.Items {
		if it.ID == id {
			return it, true
		}
	}
	return Item{}, false
}

func (m *LongTerm) buildVocab(text string) {
	for _, t := range Tokenize(text) {
		if _, ok := m.vocabID[t]; !ok {
			m.vocabID[t] = len(m.vocab)
			m.vocab = append(m.vocab, t)
		}
	}
}

func (m *LongTerm) textToVector(text string) []float64 {
	vec := make([]float64, len(m.vocabID))
	for _, t := range Tokenize(text) {
		if idx, ok := m.vocabID[t]; ok {
			vec[idx]++
		}
	}
	return vec
}

// Store 将内容存入长期记忆（embedding 可选，传 nil 则使用 TF 降级）
// 返回 true 表示新增成功，false 表示因去重而跳过
func (m *LongTerm) Store(content string, importance float64, embedding []float64) bool {
	return m.StoreClassified(content, importance, embedding, "general", nil, "")
}

// StoreClassified 与 Store 行为相同，但额外记录 category/tags/slot_hint
// 用于 Schema-driven 槽位装配过滤
func (m *LongTerm) StoreClassified(content string, importance float64, embedding []float64,
	category string, tags []string, slotHint string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 去重检测：与已有条目相似度过高时跳过，但更新已有条目的访问时间和重要性
	if m.consolidationCfg != nil && len(m.Items) > 0 && len(embedding) > 0 {
		for i := range m.Items {
			if len(m.Items[i].Embedding) == len(embedding) {
				sim := Cosine(embedding, m.Items[i].Embedding)
				if sim >= m.consolidationCfg.DedupThreshold {
					if importance > m.Items[i].Importance {
						m.Items[i].Importance = importance
					}
					m.Items[i].LastAccessed = time.Now()
					// 命中已有条目：若新分类更具体则覆盖（general 视为最弱）
					if category != "" && (m.Items[i].Category == "" || m.Items[i].Category == "general") {
						m.Items[i].Category = category
					}
					if slotHint != "" && m.Items[i].SlotHint == "" {
						m.Items[i].SlotHint = slotHint
					}
					if len(tags) > 0 {
						m.Items[i].Tags = mergeTags(m.Items[i].Tags, tags)
					}
					return false
				}
			}
		}
	}

	m.buildVocab(content)
	now := time.Now()
	if category == "" {
		category = "general"
	}
	m.Items = append(m.Items, Item{
		ID:           m.nextID,
		Content:      content,
		Importance:   importance,
		Embedding:    embedding,
		CreatedAt:    now,
		LastAccessed: now,
		Category:     category,
		Tags:         tags,
		SlotHint:     slotHint,
	})
	m.nextID++
	m.storeCount++
	return true
}

// mergeTags 合并两个标签列表去重，保持顺序
func mergeTags(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, t := range append(a, b...) {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

// StoreItem 直接插入已有 Item（用于从 DB 恢复数据）
func (m *LongTerm) StoreItem(item Item) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buildVocab(item.Content)
	if item.ID >= m.nextID {
		m.nextID = item.ID + 1
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	if item.LastAccessed.IsZero() {
		item.LastAccessed = item.CreatedAt
	}
	m.Items = append(m.Items, item)
}

// SyncLastItemPGID 将最后一条记忆的 ID 同步为 PG 自增 ID
// 用于解决内存 ID 与 PG ID 不一致的问题
func (m *LongTerm) SyncLastItemPGID(pgID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Items) > 0 && pgID > 0 {
		m.Items[len(m.Items)-1].ID = pgID
		if pgID >= m.nextID {
			m.nextID = pgID + 1
		}
	}
}

// NeedConsolidation 检查是否需要触发记忆合并
func (m *LongTerm) NeedConsolidation() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.consolidationCfg != nil &&
		m.consolidationCfg.TriggerInterval > 0 &&
		m.storeCount >= m.consolidationCfg.TriggerInterval
}

// Recall 从长期记忆中召回与 query 最相关的 topK 条
// 优先使用 embedding 余弦相似度，若无 embedding 则退回 TF
// 只返回综合得分超过 threshold 的条目，避免注入噪声
func (m *LongTerm) Recall(query string, topK int, queryEmbedding []float64) []Item {
	return m.RecallByFilter(query, queryEmbedding, RecallFilter{TopK: topK, MinScore: 0.4})
}

// RecallByFilter 按 Schema-driven 过滤条件做语义召回
//   - filter.Categories 非空时，只返回 Category 命中其一的条目
//   - filter.RequireTags 非空时，条目必须包含全部标签
//   - filter.MaxAgeHours > 0 时，超龄条目被过滤
//   - filter.MinScore 控制综合分阈值（默认 0.4）
//   - filter.TopK 控制返回数量（0 表示不截断）
func (m *LongTerm) RecallByFilter(query string, queryEmbedding []float64, filter RecallFilter) []Item {
	// 写路径：召回过程中会更新 LastAccessed + 在 TF 兜底时增量 buildVocab
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.Items) == 0 {
		return nil
	}
	threshold := filter.MinScore
	if threshold <= 0 {
		threshold = 0.4
	}
	now := time.Now()

	// 仅在确实走 TF 兜底时（query 没 embedding 或维度不匹配）才需要扩词表
	useTF := len(queryEmbedding) == 0
	if useTF {
		m.buildVocab(query)
	}

	type scored struct {
		item Item
		s    float64
	}
	var items []scored
	for i := range m.Items {
		// 类别过滤
		if len(filter.Categories) > 0 && !containsString(filter.Categories, m.Items[i].Category) {
			continue
		}
		// 标签过滤
		if len(filter.RequireTags) > 0 && !containsAllTags(m.Items[i].Tags, filter.RequireTags) {
			continue
		}
		// 年龄过滤
		if filter.MaxAgeHours > 0 {
			if now.Sub(m.Items[i].CreatedAt).Hours() > float64(filter.MaxAgeHours) {
				continue
			}
		}

		var sim float64
		if !useTF && len(m.Items[i].Embedding) == len(queryEmbedding) {
			sim = Cosine(queryEmbedding, m.Items[i].Embedding)
		} else {
			qv := m.textToVector(query)
			iv := m.textToVector(m.Items[i].Content)
			if len(qv) < len(iv) {
				qv = append(qv, make([]float64, len(iv)-len(qv))...)
			} else if len(iv) < len(qv) {
				iv = append(iv, make([]float64, len(qv)-len(iv))...)
			}
			sim = Cosine(qv, iv)
		}
		s := sim*0.7 + m.Items[i].Importance*0.3
		if s >= threshold {
			m.Items[i].LastAccessed = now
			items = append(items, scored{item: m.Items[i], s: s})
		}
	}
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].s > items[j].s })
	topK := filter.TopK
	if topK > 0 && topK < len(items) {
		items = items[:topK]
	}
	result := make([]Item, len(items))
	for i := range result {
		result[i] = items[i].item
		result[i].Score = items[i].s
	}
	return result
}

// containsString 简单 slice 包含检查
func containsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

// ContainsString 暴露给外部子包（如 graph）使用
func ContainsString(slice []string, target string) bool { return containsString(slice, target) }

// containsAllTags 检查 item 是否包含所有要求的标签
func containsAllTags(itemTags, required []string) bool {
	for _, r := range required {
		if !containsString(itemTags, r) {
			return false
		}
	}
	return true
}

// FilterByCategory 直接返回属于指定 category 的全部条目（不做语义召回）
// 用于 Profile 等结构化槽位的稳定枚举
func (m *LongTerm) FilterByCategory(categories []string, limit int) []Item {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.Items) == 0 || len(categories) == 0 {
		return nil
	}
	var result []Item
	for i := range m.Items {
		if containsString(categories, m.Items[i].Category) {
			result = append(result, m.Items[i])
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// Consolidate 执行记忆合并：衰减 → 去重+合并 → 过期淘汰
// 返回合并结果，调用方需根据结果同步 PG
func (m *LongTerm) Consolidate() ConsolidationResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := ConsolidationResult{}
	if m.consolidationCfg == nil || len(m.Items) <= 1 {
		return result
	}
	m.storeCount = 0
	removed := make(map[int]bool)

	// Phase 1: 重要性衰减 — 重要性随时间指数递减
	for i := range m.Items {
		days := time.Since(m.Items[i].CreatedAt).Hours() / 24
		m.Items[i].Importance *= math.Pow(m.consolidationCfg.DecayRate, days)
	}

	// Phase 2: 去重 + 合并 — 两两比较相似度
	for i := 0; i < len(m.Items); i++ {
		if removed[i] {
			continue
		}
		for j := i + 1; j < len(m.Items); j++ {
			if removed[j] {
				continue
			}
			sim := m.itemSimilarity(m.Items[i], m.Items[j])

			if sim >= m.consolidationCfg.DedupThreshold {
				// 去重：保留重要性更高的，删除另一个
				if m.Items[j].Importance >= m.Items[i].Importance {
					removed[i] = true
					result.Deduped++
					result.DeleteFromDB = append(result.DeleteFromDB, m.Items[i].ID)
				} else {
					removed[j] = true
					result.Deduped++
					result.DeleteFromDB = append(result.DeleteFromDB, m.Items[j].ID)
				}
			} else if sim >= m.consolidationCfg.SimilarityThreshold {
				// 合并：语义相近但非完全重复，合并为一条
				merged := m.mergeItems(m.Items[i], m.Items[j])
				m.Items[i] = merged
				removed[j] = true
				result.Merged++
				result.DeleteFromDB = append(result.DeleteFromDB, m.Items[j].ID)
				result.UpdateInDB = append(result.UpdateInDB, merged)
			}
		}
	}

	// Phase 3: 过期淘汰 — 低重要性 + 超过 TTL 的条目自动删除
	for i := range m.Items {
		if removed[i] {
			continue
		}
		days := time.Since(m.Items[i].CreatedAt).Hours() / 24
		if m.consolidationCfg.TTLDays > 0 &&
			days > float64(m.consolidationCfg.TTLDays) &&
			m.Items[i].Importance < m.consolidationCfg.MinImportance {
			removed[i] = true
			result.Expired++
			result.DeleteFromDB = append(result.DeleteFromDB, m.Items[i].ID)
		}
	}

	// 重建列表和词表
	var newItems []Item
	for i, item := range m.Items {
		if !removed[i] {
			newItems = append(newItems, item)
		}
	}
	m.Items = newItems
	m.rebuildVocab()

	return result
}

// itemSimilarity 计算两条记忆之间的相似度
func (m *LongTerm) itemSimilarity(a, b Item) float64 {
	if len(a.Embedding) > 0 && len(b.Embedding) > 0 && len(a.Embedding) == len(b.Embedding) {
		return Cosine(a.Embedding, b.Embedding)
	}
	// TF 词袋降级
	m.buildVocab(a.Content)
	m.buildVocab(b.Content)
	av := m.textToVector(a.Content)
	bv := m.textToVector(b.Content)
	if len(av) < len(bv) {
		av = append(av, make([]float64, len(bv)-len(av))...)
	} else if len(bv) < len(av) {
		bv = append(bv, make([]float64, len(av)-len(bv))...)
	}
	return Cosine(av, bv)
}

// mergeItems 合并两条相似记忆，保留重要性更高的作为主体
func (m *LongTerm) mergeItems(a, b Item) Item {
	// 以重要性更高的条目为主体
	base, other := a, b
	if b.Importance > a.Importance {
		base, other = b, a
	}

	merged := Item{
		ID:           base.ID,
		Importance:   math.Max(base.Importance, other.Importance),
		Embedding:    base.Embedding,
		CreatedAt:    base.CreatedAt,
		LastAccessed: time.Now(),
	}

	// 内容合并：非子串关系时用分号拼接，否则保留较长的
	if !strings.Contains(base.Content, other.Content) && !strings.Contains(other.Content, base.Content) {
		merged.Content = base.Content + "；" + other.Content
	} else if len(other.Content) > len(base.Content) {
		merged.Content = other.Content
	} else {
		merged.Content = base.Content
	}

	// Embedding 按重要性加权平均
	if len(base.Embedding) > 0 && len(other.Embedding) > 0 && len(base.Embedding) == len(other.Embedding) {
		wA, wB := base.Importance, other.Importance
		total := wA + wB
		if total > 0 {
			merged.Embedding = make([]float64, len(base.Embedding))
			for i := range base.Embedding {
				merged.Embedding[i] = (base.Embedding[i]*wA + other.Embedding[i]*wB) / total
			}
		}
	}

	return merged
}

// rebuildVocab 重建全局词表（合并/删除后调用）
func (m *LongTerm) rebuildVocab() {
	m.vocabID = make(map[string]int)
	m.vocab = nil
	for _, item := range m.Items {
		m.buildVocab(item.Content)
	}
}

// ─────────────────────────────── 公共工具 ──────────────────────────────

// Tokenize 将文本切成词元（中文逐字，英文按单词）
// 暴露给外部子包（如 graph）使用
func Tokenize(text string) []string {
	var tokens []string
	word := ""
	for _, r := range text {
		if r >= 0x4E00 && r <= 0x9FFF {
			if word != "" {
				tokens = append(tokens, strings.ToLower(word))
				word = ""
			}
			tokens = append(tokens, string(r))
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			word += string(r)
		} else {
			if word != "" {
				tokens = append(tokens, strings.ToLower(word))
				word = ""
			}
		}
	}
	if word != "" {
		tokens = append(tokens, strings.ToLower(word))
	}
	return tokens
}

// Cosine 计算两个向量的余弦相似度
// 暴露给外部子包（如 graph）使用
func Cosine(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
