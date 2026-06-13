// process.go — 主入口（Process / ProcessWithOptions / ProcessContext / ProcessStream）
// + 内部统一执行流 runOnce + 三段拆分（prepare / dispatch / finalize）。
//
// 设计：流式与非流式合并为同一执行流，靠 onEvent 是否为 nil 区分：
//
//	Process / ProcessContext / ProcessWithOptions    → runOnce(..., nil)
//	ProcessStream                                    → runOnce(..., onEvent)
//
// runOnce 编排：
//
//	prepare（STM 写入 + 偏好提取 + 路由决策 + 上下文装配 + 历史构建）
//	  ↓
//	dispatch（按 mode 分发到 chat / tool / rag / react，单一 mode handler 同时支持流/非流）
//	  ↓
//	finalize（assistant STM 写入 + 异步记忆抽取 + 异步合并 + 事件发布 + 计数填充）
package chat

import (
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"context"
	"encoding/json"
	"fmt"
)

// ─────────────────────────── 公开入口 ───────────────────────────

func (a *UnifiedAgent) Process(query string) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, ChatOptions{Explicit: false}, nil)
}

// ProcessWithOptions 带显式选项的入口，供前端精确控制路由
func (a *UnifiedAgent) ProcessWithOptions(query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, opts, nil)
}

// ProcessContext 带 context 的入口，支持 SSE 流式和取消
func (a *UnifiedAgent) ProcessContext(ctx context.Context, query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, opts, nil)
}

// ProcessStream 流式处理入口，在关键节点通过 onEvent 回调推送 SSE 事件。
// 返回完整的 Response（与 Process 一致），同时通过回调实时推送中间事件。
func (a *UnifiedAgent) ProcessStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	if onEvent == nil {
		// 上层若错误地传 nil，仍走非流式路径，保证 Response 完整
		return a.runOnce(ctx, query, opts, nil)
	}
	return a.runOnce(ctx, query, opts, onEvent)
}

// ─────────────────────────── 内部执行流 ───────────────────────────

// preparedRequest 是路由决策完成后、模式分发开始前的中间产物
type preparedRequest struct {
	query      string
	mode       string               // chat / tool / rag / react
	routeTools map[string]tool.Tool // 仅在 mode == tool / react 时非空
	memPrefix  string
	histMsgs   []llm.Message
	extracted  string // 同步规则提取的偏好回显（可能为空）
}

// runOnce 是 process / processStream 的统一编排：
// onEvent == nil → 非流式（不推送任何事件，但内部仍走完同一段逻辑）
// onEvent != nil → 流式（在 route / step / token / tool_call / rag_result / done 等节点推送）
func (a *UnifiedAgent) runOnce(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	pr := a.prepare(ctx, query, opts)

	resp := &Response{
		Query:         query,
		Mode:          pr.mode,
		ExtractedInfo: pr.extracted,
	}

	if pr.extracted != "" {
		emit(onEvent, "memory", map[string]string{"extracted_info": pr.extracted})
	}
	emit(onEvent, "route", map[string]string{"mode": pr.mode})

	// 检查 context 是否已取消（在分发前）
	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		emit(onEvent, "done", resp)
		return resp
	}

	a.dispatch(ctx, pr, resp, onEvent)

	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.finalize(query, resp)

	emit(onEvent, "done", resp)
	return resp
}

// prepare 完成 STM 写入 / 偏好提取 / 路由 / 上下文装配 / 历史构建。
// 不推任何事件——事件由 runOnce 统一推送，便于 finalize 控制顺序。
func (a *UnifiedAgent) prepare(ctx context.Context, query string, opts ChatOptions) preparedRequest {
	// 更新短期记忆 + 持久化
	a.mem.stm.Add("user", query)
	a.repos.chat.Save("user", query)

	// 偏好提取：优先 LLM（异步）+ 同步规则兜底（立即生效，回显给前端）
	a.goSafe("process.preference-extract", func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) == 0 {
			return
		}
		a.mem.pref.SaveBatch(kvs)
		for k, v := range kvs {
			a.repos.pref.Save("default", k, v)
			content := fmt.Sprintf("用户%s: %s", k, v)
			emb, _ := a.llm.Embed(content)
			if added, _ := a.mem.graphMem.Store(content, 0.8, emb); added {
				embJSON, _ := json.Marshal(emb)
				pgID := a.repos.ltm.Save(content, 0.8, embJSON)
				a.mem.graphMem.SyncLastItemPGID(pgID)
			}
		}
	})

	var extracted string
	if key, value, ok := a.mem.pref.ExtractAndSave(query); ok {
		extracted = fmt.Sprintf("已记住：%s = %s", key, value)
	}

	// 路由决策
	mode, routeTools := a.routeDecide(query, opts)

	// 装配 Schema-driven 上下文前缀 + 对话历史
	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(query)

	return preparedRequest{
		query:      query,
		mode:       mode,
		routeTools: routeTools,
		memPrefix:  memPrefix,
		histMsgs:   histMsgs,
		extracted:  extracted,
	}
}

// routeDecide 把"显式 vs 自动路由"的判断从 process 主流程中抽出。
// 显式优先（Explicit=true 时按 SelectedTools / UseRAG 决定），否则按关键词启发式判定。
func (a *UnifiedAgent) routeDecide(query string, opts ChatOptions) (mode string, routeTools map[string]tool.Tool) {
	if opts.Explicit {
		switch {
		case len(opts.SelectedTools) > 0:
			routeTools = a.filterTools(opts.SelectedTools)
			if a.needReActFromTools(query, routeTools) {
				mode = "react"
			} else {
				mode = "tool"
			}
		case opts.UseRAG && a.rag.Loaded:
			mode = "rag"
		default:
			mode = "chat"
		}
		return
	}

	switch {
	case a.needReAct(query):
		mode = "react"
		routeTools = a.toolsSnapshot()
	case a.needTool(query):
		mode = "tool"
		routeTools = a.toolsSnapshot()
	case a.needRAG(query):
		mode = "rag"
	default:
		mode = "chat"
	}
	return
}

// dispatch 按 mode 调对应 handler，把结果填回 resp。
// 流式与非流式共用同一组 handler，由 onEvent 区分。
func (a *UnifiedAgent) dispatch(ctx context.Context, pr preparedRequest, resp *Response, onEvent func(StreamEvent)) {
	switch pr.mode {
	case "react":
		answer, steps, task := a.runReAct(ctx, pr.query, pr.routeTools, pr.memPrefix, pr.histMsgs, onEvent)
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runTool(ctx, pr.query, pr.routeTools, pr.memPrefix, pr.histMsgs, onEvent)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		answer, results := a.rag.QueryWithHistory(pr.query, a.recentHistoryForRAG())
		resp.Answer, resp.SearchResults = answer, results
		// RAG 暂不流式合成，但也通过事件回放结果以保持 SSE 输出形状一致
		emit(onEvent, "rag_result", map[string]interface{}{"search_results": results})
		emit(onEvent, "token", map[string]string{"content": answer})
	default: // chat
		systemPrompt := a.buildSystemPrompt(pr.memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.chatLLM(ctx, systemPrompt, pr.histMsgs, onEvent)
	}
}

// finalize 完成 assistant 写回、异步记忆抽取、异步合并、事件发布、计数填充。
// 所有"无论流式或非流式都要做"的副作用集中在这里。
func (a *UnifiedAgent) finalize(query string, resp *Response) {
	a.mem.stm.Add("assistant", resp.Answer)
	a.repos.chat.Save("assistant", resp.Answer)

	// 从 assistant 回答中提取可记忆信息
	a.goSafe("process.memory-extract", func() { a.extractMemoryFromReply(resp.Answer) })

	// 异步触发记忆合并（去重+合并+衰减+过期；有图层时使用图感知合并以保护高中心度节点）
	a.goSafe("process.consolidate", func() {
		if a.mem.ltm.NeedConsolidation() {
			var result longterm.ConsolidationResult
			if a.mem.graphMem != nil {
				result = a.mem.graphMem.GraphAwareConsolidate()
			} else {
				result = a.mem.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.repos.events.Publish("agent.chat", string(eventData))

	resp.ShortTermCount = a.mem.stm.Count()
	resp.LongTermCount = a.mem.ltm.Count()
	resp.Preferences = a.mem.pref.Snapshot()
}
