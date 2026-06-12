// memory_writer.go — 从 LLM 回复里提取记忆，分类后写入长期记忆 + PG。
//
// 抽自 agent.go 的"Stage 5：Memory（基础层，注入所有模式）"区块。包含：
//
//   - extractMemoryFromReply：从 assistant 回复抽取 k-v 事实
//   - classifyMemoryContent：基于关键词的快速分类规则
//   - llmClassifyMemory：LLM 兜底分类
//   - syncConsolidationToDB：把记忆合并结果同步回 PG
package chat

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/infrastructure/llm"
)

// extractMemoryFromReply 从 assistant 回复中提取值得记忆的信息并存入长期记忆。
// 写入前用规则层 + LLM 兜底对内容分类（category/tags/slot_hint），
// 使 Schema-driven 装配机制能按槽位过滤召回。
func (a *UnifiedAgent) extractMemoryFromReply(answer string) {
	if answer == "" || !a.cfg.IsRealLLM() {
		return
	}
	// 用 LLM 提取 k-v 事实
	prompt := `从下面这段AI回复中，提取值得长期记住的客观事实或用户偏好信息。
只提取明确的、非临时性的信息，忽略对话上下文和临时细节。
输出 JSON 对象（key为中文名称，value为具体值），如果没有值得记忆的信息则输出 {}。
只输出 JSON，不要有其他内容。

回复：` + answer
	raw := a.llm.Chat("", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	var kvs map[string]string
	if err := json.Unmarshal([]byte(raw), &kvs); err != nil || len(kvs) == 0 {
		return
	}
	for k, v := range kvs {
		if k == "" || v == "" {
			continue
		}
		a.pref.Save(k, v)
		a.prefRepo.Save("default", k, v)
		content := fmt.Sprintf("用户%s: %s", k, v)

		// ── 分类管线：规则优先，LLM 兜底 ──
		category, tags, slotHint := classifyMemoryContent(k, v)
		if category == "" {
			category, tags, slotHint = a.llmClassifyMemory(content)
		}

		emb, _ := a.llm.Embed(content)
		if a.graphMem != nil {
			if added, _ := a.graphMem.StoreClassified(content, 0.7, emb, category, tags, slotHint); added {
				embJSON, _ := json.Marshal(emb)
				pgID := a.ltmRepo.SaveClassified(content, 0.7, embJSON, category, tags, slotHint)
				a.graphMem.SyncLastItemPGID(pgID)
			}
		} else if a.ltm.StoreClassified(content, 0.7, emb, category, tags, slotHint) {
			embJSON, _ := json.Marshal(emb)
			pgID := a.ltmRepo.SaveClassified(content, 0.7, embJSON, category, tags, slotHint)
			a.ltm.SyncLastItemPGID(pgID)
		}
		log.Printf("🧠 从回复中提取记忆：%s = %s（类别=%s）", k, v, category)
	}
}

// classifyMemoryContent 用正则规则快速分类；返回空字符串表示规则未命中，由 LLM 兜底
func classifyMemoryContent(key, value string) (category string, tags []string, slotHint string) {
	combined := key + value
	switch {
	case containsAny(combined, "叫", "名字", "姓名", "是我", "我是"):
		return "identity", []string{"name"}, "profile"
	case containsAny(combined, "喜欢", "偏好", "习惯", "爱好", "讨厌", "不喜欢"):
		return "preference", []string{"preference"}, "profile"
	case containsAny(combined, "工具", "失败", "错误", "报错", "异常"):
		return "tool_failure", []string{"tool", "error"}, "tool_state"
	case containsAny(combined, "禁止", "不要", "不能", "必须", "强制"):
		return "policy", []string{"constraint"}, "constraints"
	default:
		return "", nil, ""
	}
}

// containsAny 检查 s 是否包含 subs 中任意子串
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// llmClassifyMemory 调用一次 LLM 对记忆内容做 JSON 分类，
// 返回 category / tags / slotHint；失败时回退到 "general"
func (a *UnifiedAgent) llmClassifyMemory(content string) (category string, tags []string, slotHint string) {
	if !a.cfg.IsRealLLM() {
		return "general", nil, ""
	}
	prompt := `请对以下记忆内容进行分类，只输出 JSON，格式如下：
{"category":"identity|preference|fact|episodic|tool_failure|policy|general","tags":["tag1"],"slot_hint":"profile|planner|task_memory|tool_state|constraints|recall_memory"}

记忆内容：` + content
	raw := a.llm.Chat("", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(raw), "```json"), "```"))
	var result struct {
		Category string   `json:"category"`
		Tags     []string `json:"tags"`
		SlotHint string   `json:"slot_hint"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil || result.Category == "" {
		return "general", nil, ""
	}
	return result.Category, result.Tags, result.SlotHint
}

// syncConsolidationToDB 将记忆合并结果同步到 PostgreSQL
func (a *UnifiedAgent) syncConsolidationToDB(result longterm.ConsolidationResult) {
	if len(result.DeleteFromDB) > 0 {
		a.ltmRepo.Delete(result.DeleteFromDB)
		log.Printf("🧹 记忆合并：删除 %d 条（去重=%d, 合并=%d, 过期=%d）",
			result.Deduped+result.Merged+result.Expired, result.Deduped, result.Merged, result.Expired)
	}
	for _, item := range result.UpdateInDB {
		embJSON, _ := json.Marshal(item.Embedding)
		a.ltmRepo.Update(item.ID, item.Content, item.Importance, embJSON)
		log.Printf("🔗 记忆合并：更新 id=%d", item.ID)
	}
}
