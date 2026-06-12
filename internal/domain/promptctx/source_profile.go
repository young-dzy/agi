package promptctx

import (
	"context"
	"fmt"
	"sort"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
)

// ProfileSource 装填 Long-term Profile 槽位
// 数据来源：preference.Preference（高优先级，稳定身份信息）+ LTM 中 category=identity|preference 的条目
type ProfileSource struct {
	pref *preference.Preference
	ltm  *longterm.LongTerm
}

// NewProfileSource 创建 Profile source
func NewProfileSource(pref *preference.Preference, ltm *longterm.LongTerm) *ProfileSource {
	return &ProfileSource{pref: pref, ltm: ltm}
}

// ID 返回 source 标识
func (s *ProfileSource) ID() string { return "profile" }

// Supports 仅声明支持 Profile 槽位
func (s *ProfileSource) Supports(kind SlotKind) bool { return kind == SlotProfile }

// Fetch 输出按字母序稳定的偏好键值对 + LTM 身份/偏好类条目
func (s *ProfileSource) Fetch(ctx context.Context, slot Slot, q Query) ([]ContextItem, error) {
	var items []ContextItem

	if s.pref != nil {
		// 拿一次性快照，避免遍历期间被并发写入打断
		data := s.pref.Snapshot()
		if len(data) > 0 {
			keys := make([]string, 0, len(data))
			for k := range data {
				keys = append(keys, k)
			}
			sort.Strings(keys) // 稳定顺序，避免每轮 prompt 抖动
			for _, k := range keys {
				items = append(items, ContextItem{
					Text:   fmt.Sprintf("%s: %s", k, data[k]),
					Score:  1.0, // 偏好是确定性事实
					Source: s.ID(),
				})
			}
		}
	}

	if s.ltm != nil && len(slot.Filter.Categories) > 0 {
		limit := slot.Filter.TopK
		if limit <= 0 {
			limit = 10
		}
		for _, item := range s.ltm.FilterByCategory(slot.Filter.Categories, limit) {
			items = append(items, ContextItem{
				Text:   item.Content,
				Score:  item.Importance,
				Source: s.ID(),
				Meta:   map[string]string{"category": item.Category},
			})
		}
	}

	return items, nil
}
