// mem_stack.go — 三层记忆 + 偏好的聚合容器。
//
// 把 stm（短期）/ ltm（长期）/ graphMem（图增强）/ pref（偏好）四个独立组件
// 装进一个 struct，UnifiedAgent 上不再铺 4 个字段；调用点通过 a.mem.stm 等
// 访问意图更直白。
//
// graphMem 是延后注入的：initKnowledgeGraph 在 New 末尾才创建，期间 mem.graphMem 为 nil。
// 调用方需要 nil 检查（与重构前一致）。
package chat

import (
	"agi-assistant/config"
	graphmem "agi-assistant/internal/domain/memory/graph"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/memory/shortterm"
)

// memoryStack 聚合三层记忆 + 用户偏好
type memoryStack struct {
	stm      *shortterm.ShortTerm
	ltm      *longterm.LongTerm
	graphMem *graphmem.GraphMemory // 启动期为 nil，initKnowledgeGraph 中赋值
	pref     *preference.Preference
}

// newMemoryStack 创建并配置记忆栈。graphMem 在初始化阶段不就位，由
// initKnowledgeGraph 通过 attachGraph 延后注入。
func newMemoryStack(cfg *config.APIConfig) *memoryStack {
	ltm := longterm.New()
	ltm.SetConsolidationConfig(&longterm.ConsolidationConfig{
		SimilarityThreshold: cfg.MemoryConsolidationSimilarity,
		DedupThreshold:      cfg.MemoryConsolidationDedup,
		TTLDays:             cfg.MemoryConsolidationTTLDays,
		DecayRate:           cfg.MemoryConsolidationDecayRate,
		MinImportance:       cfg.MemoryConsolidationMinImport,
		TriggerInterval:     cfg.MemoryConsolidationTrigger,
	})
	return &memoryStack{
		stm:  shortterm.New(cfg.ShortTermMaxTurns),
		ltm:  ltm,
		pref: preference.New(),
	}
}

// attachGraph 在 KGStore 就绪后注入图增强记忆层
func (m *memoryStack) attachGraph(g *graphmem.GraphMemory) {
	m.graphMem = g
}
