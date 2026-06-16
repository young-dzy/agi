// planner.go — UnifiedAgent 的图任务规划器。
//
// 抽自 agent.go 的 "Planner LLM" 区块。在 ReAct 模式下，先由 Planner LLM
// 根据可用工具集和用户问题产出一组 planNode（带依赖和竞速组），GraphRuntime 再并行调度执行。
// LLM 不可用或解析失败时降级到 rulePlanNodes 关键词规则。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"agi-assistant/internal/domain/graph"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
)

// planNode 是 Planner LLM 输出的图节点（带依赖和竞速组）
type planNode struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Tool      string            `json:"tool"`
	Agent     string            `json:"agent"`
	Goal      string            `json:"goal"`
	Params    map[string]string `json:"params"`
	Reason    string            `json:"reason"`
	DependsOn []string          `json:"depends_on"`
	RaceGroup string            `json:"race_group"`
}

// toGraphNode 将 planNode 转为 graph.Node
func (pn planNode) toGraphNode() *graph.Node {
	nodeType := graph.NodeTypeTool
	if pn.Type == string(graph.NodeTypeSubAgent) || pn.Agent != "" {
		nodeType = graph.NodeTypeSubAgent
	}
	name := pn.Reason
	if name == "" {
		name = pn.Goal
	}
	return &graph.Node{
		ID:        graph.NodeID(pn.ID),
		Type:      nodeType,
		Name:      name,
		ToolName:  pn.Tool,
		AgentName: pn.Agent,
		Goal:      pn.Goal,
		Params:    pn.Params,
		DependsOn: toNodeIDs(pn.DependsOn),
		RaceGroup: pn.RaceGroup,
	}
}

func toNodeIDs(ss []string) []graph.NodeID {
	ids := make([]graph.NodeID, len(ss))
	for i, s := range ss {
		ids[i] = graph.NodeID(s)
	}
	return ids
}

// llmPlanGraph 调用 Planner LLM，输出带依赖关系的 planNode 列表。
// 若 LLM 不可用或解析失败，降级为关键词规则。
func (a *UnifiedAgent) llmPlanGraph(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string) []*graph.Node {
	if a.needsSubAgentPlan(strings.ToLower(query)) {
		return a.rulePlanNodes(ctx, query, ts, memPrefix)
	}
	if !a.cfg.IsRealLLM() {
		return a.rulePlanNodes(ctx, query, ts, memPrefix)
	}

	// 构造工具描述
	var toolLines []string
	for name, t := range ts {
		var pDescs []string
		for _, p := range t.Parameters {
			req := ""
			if p.Required {
				req = "（必填）"
			}
			pDescs = append(pDescs, fmt.Sprintf("%s(%s)%s", p.Name, p.Type, req))
		}
		params := strings.Join(pDescs, ", ")
		if params == "" {
			params = "无"
		}
		toolLines = append(toolLines, fmt.Sprintf("- %s: %s [参数: %s]", name, t.Description, params))
	}
	var agentLines []string
	for name, sa := range a.subagents.snapshot() {
		agentLines = append(agentLines, fmt.Sprintf("- %s: %s", name, sa.Description()))
	}

	planPrompt := fmt.Sprintf(`你是一个任务规划器。根据用户问题，从可用工具和可用子 Agent 中选出需要调用的节点，并标注它们之间的依赖关系。

规则：
- 给每个节点分配一个唯一 id（如 n1, n2, n3...）
- type 只能是 "tool" 或 "sub_agent"
- 工具节点填写 tool 和 params；子 Agent 节点填写 agent 和 goal
- 如果节点 B 需要节点 A 的输出，则 B 的 depends_on 包含 A 的 id
- 如果两个工具功能类似（如多个搜索源），设相同的 race_group，系统会并行执行谁先返回用谁
- 无依赖关系的节点不要互相等待，depends_on 设为 []
- 需要研究、总结、报告、写文档时，优先使用 research_agent / writer_agent / review_agent / doc_agent 组合
- doc_agent 负责把上游内容保存到本地文档库并写入 RAG

用户问题：%s
可用工具：
%s

可用子 Agent：
%s

请以 JSON 数组格式输出执行计划：
[{"id":"n1","type":"sub_agent","agent":"research_agent","goal":"研究目标","params":{},"reason":"一句话说明为什么调用","depends_on":[],"race_group":""}]
如果无需工具直接回答，输出 []。只输出 JSON，不要其他内容。`, query, strings.Join(toolLines, "\n"), strings.Join(agentLines, "\n"))

	plannerBase := "你是一个精准的任务规划器，只在必要时才调用工具，不做无意义的调用。能识别工具间的依赖关系和可竞速的同类工具。"
	if memPrefix != "" {
		plannerBase = memPrefix + "\n\n" + plannerBase + "\n注意：用户偏好可能影响工具参数选择（如城市、时区等），请在参数中体现。"
	}
	raw := a.llm.ChatContext(ctx, plannerBase,
		[]llm.Message{{Role: "user", Content: planPrompt}})

	if ctx.Err() != nil {
		return a.rulePlanNodes(ctx, query, ts, memPrefix)
	}

	// 清洗 LLM 输出
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "<|FunctionCallBegin|>"); idx >= 0 {
		raw = raw[idx+len("<|FunctionCallBegin|>"):]
		if end := strings.Index(raw, "<|FunctionCallEnd|>"); end >= 0 {
			raw = raw[:end]
		}
	}
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	// 尝试解析为图节点格式
	var nodes []planNode
	if err := json.Unmarshal([]byte(raw), &nodes); err != nil {
		// 降级：尝试解析为旧格式 [{"tool":...,"params":...,"reason":...}]
		var legacyItems []struct {
			Tool   string            `json:"tool"`
			Params map[string]string `json:"params"`
			Reason string            `json:"reason"`
		}
		if legacyErr := json.Unmarshal([]byte(raw), &legacyItems); legacyErr == nil {
			for i, li := range legacyItems {
				id := fmt.Sprintf("n%d", i+1)
				nodes = append(nodes, planNode{
					ID: id, Tool: li.Tool, Params: li.Params,
					Reason: li.Reason, DependsOn: []string{}, RaceGroup: "",
				})
			}
		} else {
			// 再降级：尝试 function-calling 格式 [{"name":...,"parameters":...}]
			var altItems []struct {
				Name       string                 `json:"name"`
				Parameters map[string]interface{} `json:"parameters"`
			}
			if altErr := json.Unmarshal([]byte(raw), &altItems); altErr == nil {
				for i, ai := range altItems {
					params := make(map[string]string, len(ai.Parameters))
					for k, v := range ai.Parameters {
						params[k] = fmt.Sprint(v)
					}
					id := fmt.Sprintf("n%d", i+1)
					nodes = append(nodes, planNode{
						ID: id, Tool: ai.Name, Params: params,
						Reason: "LLM 规划调用", DependsOn: []string{}, RaceGroup: "",
					})
				}
			} else {
				log.Printf("⚠️  Planner LLM 解析失败 (%v / %v / %v)，降级到规则规划。原始输出: %s", err, legacyErr, altErr, raw)
				return a.rulePlanNodes(ctx, query, ts, memPrefix)
			}
		}
	}

	// 过滤：只保留工具集中实际存在的工具，自动补全 id/depends_on/race_group
	var valid []*graph.Node
	for i, n := range nodes {
		if n.Type == "" {
			if n.Agent != "" {
				n.Type = string(graph.NodeTypeSubAgent)
			} else {
				n.Type = string(graph.NodeTypeTool)
			}
		}
		if n.Type == string(graph.NodeTypeSubAgent) {
			if _, ok := a.subagents.get(n.Agent); !ok {
				continue
			}
		} else if _, ok := ts[n.Tool]; !ok {
			continue
		}
		if n.ID == "" {
			n.ID = fmt.Sprintf("n%d", i+1)
		}
		if n.DependsOn == nil {
			n.DependsOn = []string{}
		}
		if n.Params == nil {
			n.Params = map[string]string{}
		}
		if n.Goal == "" {
			n.Goal = n.Reason
		}
		valid = append(valid, n.toGraphNode())
	}
	return valid
}

// rulePlanNodes 关键词规则降级规划（无真实 LLM 时使用），所有节点 depends_on=[] 无依赖
func (a *UnifiedAgent) rulePlanNodes(ctx context.Context, query string, ts map[string]tool.Tool, memPrefix string) []*graph.Node {
	q := strings.ToLower(query)
	var nodes []*graph.Node
	counter := 0
	nextID := func() string {
		counter++
		return fmt.Sprintf("n%d", counter)
	}

	if a.needsSubAgentPlan(q) {
		researchID := nextID()
		writerID := nextID()
		reviewID := nextID()
		nodes = append(nodes,
			&graph.Node{
				ID: graph.NodeID(researchID), Type: graph.NodeTypeSubAgent,
				AgentName: "research_agent", Goal: "围绕用户任务进行多轮检索和证据整理",
				Name: "子 Agent 研究与证据收集", DependsOn: []graph.NodeID{},
			},
			&graph.Node{
				ID: graph.NodeID(writerID), Type: graph.NodeTypeSubAgent,
				AgentName: "writer_agent", Goal: "基于研究结果生成 Markdown 报告",
				Name: "子 Agent 生成报告", DependsOn: []graph.NodeID{graph.NodeID(researchID)},
			},
			&graph.Node{
				ID: graph.NodeID(reviewID), Type: graph.NodeTypeSubAgent,
				AgentName: "review_agent", Goal: "检查报告质量、风险和证据缺口",
				Name: "子 Agent 审查报告", DependsOn: []graph.NodeID{graph.NodeID(writerID)},
			},
		)
		if wantsDocumentWrite(q) {
			nodes = append(nodes, &graph.Node{
				ID: graph.NodeID(nextID()), Type: graph.NodeTypeSubAgent,
				AgentName: "doc_agent", Goal: "保存报告到本地文档库并写入 RAG",
				Name: "子 Agent 保存文档", DependsOn: []graph.NodeID{graph.NodeID(writerID), graph.NodeID(reviewID)},
			})
		}
		return nodes
	}

	if _, ok := ts["get_time"]; ok {
		if strings.Contains(q, "时间") || strings.Contains(q, "几点") || strings.Contains(q, "现在") {
			params := map[string]string{}
			if strings.Contains(q, "东京") {
				params["timezone"] = "Asia/Tokyo"
			}
			nodes = append(nodes, &graph.Node{
				ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
				ToolName: "get_time", Params: params, Name: "查询当前时间",
				DependsOn: []graph.NodeID{}, RaceGroup: "",
			})
		}
	}
	if _, ok := ts["get_weather"]; ok {
		if strings.Contains(q, "天气") {
			city := "北京"
			for _, c := range []string{"东京", "北京", "上海", "广州", "深圳", "纽约", "伦敦"} {
				if strings.Contains(q, c) {
					city = c
					break
				}
			}
			nodes = append(nodes, &graph.Node{
				ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
				ToolName: "get_weather", Params: map[string]string{"city": city},
				Name: "查询" + city + "天气", DependsOn: []graph.NodeID{}, RaceGroup: "",
			})
		}
	}
	if _, ok := ts["search_web"]; ok {
		if strings.Contains(q, "搜索") || strings.Contains(q, "查询") || strings.Contains(q, "介绍") ||
			strings.Contains(q, "是什么") || strings.Contains(q, "怎么") || strings.Contains(q, "如何") {
			nodes = append(nodes, &graph.Node{
				ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
				ToolName: "search_web", Params: map[string]string{"query": query},
				Name: "搜索相关信息", DependsOn: []graph.NodeID{}, RaceGroup: "search",
			})
		}
	}
	if _, ok := ts["exec_command"]; ok {
		if strings.Contains(q, "执行") || strings.Contains(q, "运行") || strings.Contains(q, "命令") ||
			strings.Contains(q, "终端") || strings.Contains(q, "lscpu") || strings.Contains(q, "cpu") ||
			strings.Contains(q, "磁盘") || strings.Contains(q, "内存") || strings.Contains(q, "系统信息") {
			cmd := extractShellCommand(query)
			nodes = append(nodes, &graph.Node{
				ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
				ToolName: "exec_command", Params: map[string]string{"command": cmd},
				Name: "执行终端命令", DependsOn: []graph.NodeID{}, RaceGroup: "",
			})
		}
	}
	if _, ok := ts["rag_search"]; ok {
		nodes = append(nodes, &graph.Node{
			ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
			ToolName: "rag_search", Params: map[string]string{"query": query},
			Name: "检索个人知识库", DependsOn: []graph.NodeID{}, RaceGroup: "search",
		})
	}
	// MCP / 自定义工具
	builtins := map[string]bool{"get_time": true, "get_weather": true, "search_web": true, "rag_search": true, "exec_command": true}
	for name, t := range ts {
		if builtins[name] {
			continue
		}
		params := a.extractParamsForTool(ctx, query, t)
		nodes = append(nodes, &graph.Node{
			ID: graph.NodeID(nextID()), Type: graph.NodeTypeTool,
			ToolName: name, Params: params, Name: "调用工具 " + name,
			DependsOn: []graph.NodeID{}, RaceGroup: "",
		})
	}
	return nodes
}

func (a *UnifiedAgent) needsSubAgentPlan(q string) bool {
	return strings.Contains(q, "研究") || strings.Contains(q, "调研") ||
		strings.Contains(q, "总结") || strings.Contains(q, "报告") ||
		strings.Contains(q, "文档") || strings.Contains(q, "方案") ||
		strings.Contains(q, "分析")
}

func wantsDocumentWrite(q string) bool {
	return strings.Contains(q, "保存") || strings.Contains(q, "落库") ||
		strings.Contains(q, "写入") || strings.Contains(q, "文档库") ||
		strings.Contains(q, "知识库") || strings.Contains(q, "生成报告") ||
		strings.Contains(q, "报告")
}
