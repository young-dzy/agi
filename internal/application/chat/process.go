// process.go — 四种主入口（Process / ProcessWithOptions / ProcessContext / ProcessStream）
// + 内部主流程 process / processStream。
//
// process 串起每轮对话的完整链路：
//
//	更新 STM → 异步偏好提取 → 路由决策（mode）→ 装配 prompt 上下文
//	→ 按 mode 分发到 chat / tool / rag / react 处理器
//	→ 写回 STM + 异步记忆抽取 + 异步合并
//
// processStream 与 process 同结构，只是在关键节点通过 onEvent 推 SSE 事件。
package chat

import (
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/tool"
	"context"
	"encoding/json"
	"fmt"
)

func (a *UnifiedAgent) Process(query string) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, ChatOptions{Explicit: false})
}

// ProcessWithOptions 带显式选项的入口，供前端精确控制路由
func (a *UnifiedAgent) ProcessWithOptions(query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, opts)
}

// ProcessContext 带 context 的入口，支持 SSE 流式和取消
func (a *UnifiedAgent) ProcessContext(ctx context.Context, query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.process(ctx, query, opts)
}

// ProcessStream 流式处理入口，在关键节点通过 onEvent 回调推送 SSE 事件。
// 返回完整的 Response（与 Process 一致），同时通过回调实时推送中间事件。
func (a *UnifiedAgent) ProcessStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.processStream(ctx, query, opts, onEvent)
}

// Cancel 取消所有当前正在执行的任务（每个 in-flight 请求都会收到取消信号）
func (a *UnifiedAgent) process(ctx context.Context, query string, opts ChatOptions) *Response {
	resp := &Response{Query: query, Mode: "chat"}

	// 更新短期记忆
	a.stm.Add("user", query)

	// 持久化用户消息到 PG
	a.chatRepo.Save("user", query)

	// 偏好提取：优先 LLM，降级规则
	a.goSafe("process.preference-extract", func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) > 0 {
			a.pref.SaveBatch(kvs)
			for k, v := range kvs {
				a.prefRepo.Save("default", k, v)
				content := fmt.Sprintf("用户%s: %s", k, v)
				emb, _ := a.llm.Embed(content)
				if added, _ := a.graphMem.Store(content, 0.8, emb); added {
					embJSON, _ := json.Marshal(emb)
					pgID := a.ltmRepo.Save(content, 0.8, embJSON)
					a.graphMem.SyncLastItemPGID(pgID)
				}
			}
		}
	})

	// 同步规则提取（用于立即展示 ExtractedInfo）
	if key, value, ok := a.pref.ExtractAndSave(query); ok {
		resp.ExtractedInfo = fmt.Sprintf("已记住：%s = %s", key, value)
	}

	// ── 路由决策（mode 在装配前确定，让 schema 选取正确）──
	var mode string
	var routeTools map[string]tool.Tool
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
	} else {
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
	}
	resp.Mode = mode

	// ── 装配 Schema-driven 上下文前缀 ──
	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(query)

	// 检查 context 是否已取消
	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		return resp
	}

	// ── 分发执行（mode 已确定）──
	switch mode {
	case "react":
		answer, steps, task := a.runReActWithTools(ctx, query, routeTools, memPrefix, histMsgs)
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runToolFromSet(ctx, query, routeTools, memPrefix, histMsgs)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		answer, results := a.rag.QueryWithHistory(query, a.recentHistoryForRAG())
		resp.Answer, resp.SearchResults = answer, results
	default:
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.llm.ChatContext(ctx, systemPrompt, histMsgs)
	}

	// 检查是否被中断
	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.stm.Add("assistant", resp.Answer)
	a.chatRepo.Save("assistant", resp.Answer)

	// 从 assistant 回答中提取可记忆信息
	a.goSafe("process.memory-extract", func() { a.extractMemoryFromReply(resp.Answer) })

	// 异步触发记忆合并（去重+合并+衰减+过期；有图层时使用图感知合并以保护高中心度节点）
	a.goSafe("process.consolidate", func() {
		if a.ltm.NeedConsolidation() {
			var result longterm.ConsolidationResult
			if a.graphMem != nil {
				result = a.graphMem.GraphAwareConsolidate()
			} else {
				result = a.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.events.Publish("agent.chat", string(eventData))

	resp.ShortTermCount = a.stm.Count()
	resp.LongTermCount = a.ltm.Count()
	resp.Preferences = a.pref.Snapshot()
	return resp
}

// processStream 与 process 逻辑一致，但在关键节点通过 onEvent 推送 SSE 事件
func (a *UnifiedAgent) processStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	if onEvent == nil {
		return a.process(ctx, query, opts)
	}

	resp := &Response{Query: query, Mode: "chat"}

	a.stm.Add("user", query)
	a.chatRepo.Save("user", query)

	// 偏好提取（异步，与 process 一致）
	a.goSafe("processStream.preference-extract", func() {
		kvs := a.llm.ExtractPreferences(query)
		if len(kvs) > 0 {
			a.pref.SaveBatch(kvs)
			for k, v := range kvs {
				a.prefRepo.Save("default", k, v)
				content := fmt.Sprintf("用户%s: %s", k, v)
				emb, _ := a.llm.Embed(content)
				if added, _ := a.graphMem.Store(content, 0.8, emb); added {
					embJSON, _ := json.Marshal(emb)
					pgID := a.ltmRepo.Save(content, 0.8, embJSON)
					a.graphMem.SyncLastItemPGID(pgID)
				}
			}
		}
	})

	// 同步规则提取
	if key, value, ok := a.pref.ExtractAndSave(query); ok {
		resp.ExtractedInfo = fmt.Sprintf("已记住：%s = %s", key, value)
		onEvent(NewStreamEvent("memory", map[string]string{"extracted_info": resp.ExtractedInfo}))
	}

	// ── 路由决策 ──
	var mode string
	var routeTools map[string]tool.Tool
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
	} else {
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
	}
	resp.Mode = mode
	onEvent(NewStreamEvent("route", map[string]string{"mode": mode}))

	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(query)

	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		onEvent(NewStreamEvent("done", resp))
		return resp
	}

	// ── 分发执行（流式版） ──
	switch mode {
	case "react":
		answer, steps, task := a.runReActStream(ctx, query, routeTools, memPrefix, histMsgs, onEvent)
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runToolStream(ctx, query, routeTools, memPrefix, histMsgs, onEvent)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		answer, results := a.rag.QueryWithHistory(query, a.recentHistoryForRAG())
		resp.Answer, resp.SearchResults = answer, results
		onEvent(NewStreamEvent("rag_result", map[string]interface{}{"search_results": results}))
		onEvent(NewStreamEvent("token", map[string]string{"content": answer}))
	default:
		systemPrompt := a.buildSystemPrompt(memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.llm.ChatStreamContext(ctx, systemPrompt, histMsgs, func(token string) {
			onEvent(NewStreamEvent("token", map[string]string{"content": token}))
		})
	}

	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.stm.Add("assistant", resp.Answer)
	a.chatRepo.Save("assistant", resp.Answer)

	a.goSafe("processStream.memory-extract", func() { a.extractMemoryFromReply(resp.Answer) })

	a.goSafe("processStream.consolidate", func() {
		if a.ltm.NeedConsolidation() {
			var result longterm.ConsolidationResult
			if a.graphMem != nil {
				result = a.graphMem.GraphAwareConsolidate()
			} else {
				result = a.ltm.Consolidate()
			}
			a.syncConsolidationToDB(result)
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.events.Publish("agent.chat", string(eventData))

	resp.ShortTermCount = a.stm.Count()
	resp.LongTermCount = a.ltm.Count()
	resp.Preferences = a.pref.Snapshot()
	onEvent(NewStreamEvent("done", resp))
	return resp
}
