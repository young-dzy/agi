// accessor.go — UnifiedAgent 的字段访问器、工具注册、快照保存、参数补全。
//
// 这些方法本身没有业务编排逻辑，是 agent struct 对外暴露状态 / 内部辅助操作的薄包装。
package chat

import (
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/memory/shortterm"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/tool"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func (a *UnifiedAgent) RegisterTool(t tool.Tool) {
	a.toolsMu.Lock()
	a.tools[t.Name] = t
	a.toolsMu.Unlock()
}

// RAG 暴露 RAG 引擎，供 HTTP handler 直接调用 Ingest
func (a *UnifiedAgent) RAG() *rag.Engine { return a.rag }

// Tools 暴露工具集（持锁拷贝），供 HTTP handler 列出工具信息。
// 调用方拿到的是快照，可无锁安全使用，且修改不影响 agent 内部 map。
func (a *UnifiedAgent) Tools() map[string]tool.Tool {
	return a.toolsSnapshot()
}

// toolsSnapshot 持锁返回工具 map 的浅拷贝（Tool 内部字段不可变，浅拷贝足够）
// 路由层（runReAct*/runTool*/Decide）调用一次后即可无锁使用，且能保证整次
// 调用看到一致的工具集（不被 in-flight RegisterTool 干扰）。
func (a *UnifiedAgent) toolsSnapshot() map[string]tool.Tool {
	a.toolsMu.RLock()
	defer a.toolsMu.RUnlock()
	cp := make(map[string]tool.Tool, len(a.tools))
	for k, v := range a.tools {
		cp[k] = v
	}
	return cp
}

// ShortTerm 暴露短期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) ShortTerm() *shortterm.ShortTerm { return a.stm }

// LongTerm 暴露长期记忆，供 HTTP handler 查询
func (a *UnifiedAgent) LongTerm() *longterm.LongTerm { return a.ltm }

// Preferences 暴露用户偏好，供 HTTP handler 查询
func (a *UnifiedAgent) Preferences() *preference.Preference { return a.pref }

// Snapshots 返回历史快照列表（持锁拷贝）
func (a *UnifiedAgent) Snapshots() []Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]Snapshot, len(a.snapshots))
	copy(cp, a.snapshots)
	return cp
}
func (a *UnifiedAgent) saveSnapshot(task *TaskState) {
	if task == nil {
		return
	}
	var stateCopy TaskState
	data, _ := json.Marshal(task)
	if err := json.Unmarshal(data, &stateCopy); err != nil {
		// 不应该发生（自序列化），但避免吃掉错误
		log.Printf("⚠️  saveSnapshot 反序列化失败: %v", err)
		return
	}
	snap := Snapshot{State: stateCopy, Timestamp: time.Now().Format("15:04:05")}
	a.mu.Lock()
	a.snapshots = append(a.snapshots, snap)
	a.mu.Unlock()
	a.snapRepo.Save(task.TaskID, data)
}

// ─────────────────────────────── Stage 5：Memory（基础层，注入所有模式）────────
//
// 旧的 buildMemorySystemPrefix / buildMemorySystemPrefixWithCtx 已删除，
// 由 buildContextPrefix → promptctx.ContextAssembler 取代（Schema-driven 装配）。

// fillParamsFromPreference 用用户偏好自动补全工具调用参数中缺失的值
func (a *UnifiedAgent) fillParamsFromPreference(tc *tool.CallResult) {
	if tc == nil {
		return
	}
	prefs := a.pref.Snapshot() // 一次性快照，下方可无锁访问
	if len(prefs) == 0 {
		return
	}
	// 偏好 key → 工具参数名的映射
	prefToParam := map[string][]string{
		"城市": {"city", "location", "location_name"},
		"时区": {"timezone", "tz", "time_zone"},
		"姓名": {"name", "username", "user_name"},
		"语言": {"language", "lang"},
		"国家": {"country", "nation"},
	}
	for prefKey, paramNames := range prefToParam {
		prefVal, ok := prefs[prefKey]
		if !ok || prefVal == "" {
			continue
		}
		for _, paramName := range paramNames {
			if v, exists := tc.Params[paramName]; !exists || v == nil || fmt.Sprint(v) == "" {
				tc.Params[paramName] = prefVal
			}
		}
	}
}
