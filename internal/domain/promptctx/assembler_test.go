package promptctx

import (
	"context"
	"strings"
	"testing"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
)

func buildAssembler() *ContextAssembler {
	pref := preference.New()
	pref.Save("城市", "北京")
	pref.Save("语言", "中文")

	ltm := longterm.New()
	ltm.StoreClassified("用户叫张三", 0.9, nil, "identity", []string{"name"}, "profile")
	ltm.StoreClassified("用户喜欢喝咖啡", 0.85, nil, "preference", nil, "profile")
	ltm.StoreClassified("上次问过天气API", 0.7, nil, "episodic", nil, "recall_memory")
	ltm.StoreClassified("python 是一门动态语言", 0.6, nil, "fact", nil, "recall_memory")

	reg := NewSourceRegistry()
	reg.Register(NewProfileSource(pref, ltm))
	reg.Register(NewConstraintsSource(sandbox.PolicySnapshot()))
	reg.Register(NewRecallSource(ltm))
	reg.Register(NewToolStateSource(
		func() map[string]tool.Tool {
			return map[string]tool.Tool{
				"get_time": {Name: "get_time", Description: "获取当前时间"},
			}
		},
		NewToolStateTracker(5),
	))
	reg.Register(NewTaskMemSource(NewTaskMemBuffer(5)))
	reg.Register(NewPlannerSource(func() *PlannerSnapshot { return nil }))

	return NewAssembler(DefaultSchemas(), reg)
}

func TestAssembler_ChatMode_HasFewSlots(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), Query{Mode: "chat", Text: "你好"})
	if rc == nil {
		t.Fatal("nil RuntimeContext")
	}

	rendered := rc.Render()

	// Chat 模式不应包含 Planner / TaskMem / ToolState
	if strings.Contains(rendered, "【任务规划】") {
		t.Error("chat mode should not render Planner slot")
	}
	if strings.Contains(rendered, "【任务记忆】") {
		t.Error("chat mode should not render TaskMem slot")
	}
	if strings.Contains(rendered, "【可用工具】") {
		t.Error("chat mode should not render ToolState slot")
	}
}

func TestAssembler_ReactMode_RendersPlannerAndTools(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), Query{Mode: "react", Text: "查天气并写诗"})

	rendered := rc.Render()

	// ReAct 模式应至少渲染 Constraints + Tools（Planner 当前 task=nil 返回空，可被跳过）
	if !strings.Contains(rendered, "【硬性约束】") {
		t.Error("react mode must include Constraints slot")
	}
	if !strings.Contains(rendered, "【可用工具】") {
		t.Error("react mode must include ToolState slot")
	}
}

func TestAssembler_UnknownModeFallsBackToChat(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), Query{Mode: "nonexistent", Text: "hi"})

	if rc.Schema.Mode != "chat" {
		t.Errorf("expected fallback to chat mode, got %s", rc.Schema.Mode)
	}
}

func TestAssembler_GlobalBudgetTruncation(t *testing.T) {
	asm := buildAssembler()
	asm.globalLimit = 200 // 极小预算，强制裁剪

	rc := asm.Assemble(context.Background(), Query{Mode: "react", Text: "test"})
	rendered := rc.Render()

	if len(rendered) > 400 {
		// 允许一定 overhead（slot 标题、换行等），但远低于默认 2400
		t.Errorf("expected truncated output, got %d chars", len(rendered))
	}

	// Constraints 优先级最高，在小预算下应仍然保留
	if !strings.Contains(rendered, "【硬性约束】") {
		t.Error("Constraints slot must survive global budget truncation (highest priority)")
	}
}

func TestAssembler_RenderProducesValidPrefix(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), Query{Mode: "chat", Text: "你好"})
	rendered := rc.Render()

	if rendered == "" {
		t.Fatal("expected non-empty render")
	}
	// 偏好 + 至少一个其他 slot
	if !strings.Contains(rendered, "城市") {
		t.Error("expected preference 城市 in render")
	}
}
