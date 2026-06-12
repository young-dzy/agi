// Package chat 是聊天会话的应用服务层（用例编排）。
//
// 文件分组：
//
//	agent.go            UnifiedAgent struct 定义 + Deps 依赖容器 + New 装配
//	types.go            请求 / 响应 / 任务 / SSE 事件类型
//	process.go          Process / ProcessStream 主入口 + process / processStream 主流程
//	context_builder.go  装配 prompt 上下文（schema-driven） + 历史消息构建
//	mode_chat.go        默认对话模式（直接调 LLM）—— 在 process.go 的 default 分支
//	mode_tool.go        单工具调用模式（runToolFromSet / runToolStream）
//	mode_react.go       多步推理模式（runReActWithTools / runReActStream + Harness）
//	router.go           路由决策（needRAG / needTool / needReAct）
//	planner.go          ReAct 工具规划 LLM
//	memory_writer.go    异步从回复抽取记忆 + 写入 LTM
//	cancel.go           per-request cancel token 管理
//	restore.go          启动期从 PG 恢复偏好 / 长期记忆 / 聊天记录 / RAG chunks
//	init_sandbox.go     沙箱初始化 + exec_command 工具注册
//	accessor.go         字段访问器 + 工具注册 + 快照保存 + 参数补全
//	status.go           系统状态 / 配置摘要（HTTP /api/status 用）
//
// 学习入口建议：
//  1. agent.go 看 UnifiedAgent 持有哪些组件
//  2. process.go 跟着 process() 走完一轮对话的完整链路
//  3. 按需挑 mode_*.go 深入某种模式
package chat
