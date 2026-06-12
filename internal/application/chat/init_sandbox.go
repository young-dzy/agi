// init_sandbox.go — 沙箱初始化与 shell 命令解析。
package chat

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"agi-assistant/internal/domain/sandbox"
	sandboximpl "agi-assistant/internal/infrastructure/sandbox"
	toolimpl "agi-assistant/internal/infrastructure/tool"
)

// initSandbox 初始化命令执行沙箱并注册 exec_command 工具
func (a *UnifiedAgent) initSandbox() {
	if !a.cfg.SandboxEnabled {
		log.Printf("ℹ️  沙箱未启用（config.sandbox.enabled=false），跳过 exec_command 工具")
		return
	}

	sbCfg := sandbox.SandboxConfig{
		Image:           a.cfg.SandboxImage,
		Timeout:         time.Duration(a.cfg.SandboxTimeoutMs) * time.Millisecond,
		MaxOutputBytes:  a.cfg.SandboxMaxOutput,
		MemoryLimitMB:   a.cfg.SandboxMemoryMB,
		CPUPercent:      a.cfg.SandboxCPUPercent,
		MaxPIDs:         a.cfg.SandboxMaxPIDs,
		NetworkDisabled: a.cfg.SandboxNetDisabled,
		ReadOnlyRootfs:  a.cfg.SandboxReadOnly,
	}
	secCfg := sandbox.SecurityConfig{
		MaxCommandLength: a.cfg.SecMaxCmdLength,
		AllowlistMode:    a.cfg.SecAllowlistMode,
		Allowlist:        a.cfg.SecAllowlist,
	}

	sb := sandboximpl.NewSandbox(a.cfg.SandboxBackend, sbCfg, secCfg)

	// 注入审计回调：将每条命令执行结果发送到 Kafka
	sb.SetAuditFn(func(r sandbox.ExecResult) {
		event, _ := json.Marshal(map[string]interface{}{
			"command":     r.Command,
			"level":       string(r.Validation.Level),
			"exit_code":   r.ExitCode,
			"duration_ms": r.Duration.Milliseconds(),
			"backend":     r.Backend,
			"killed":      r.Killed,
			"truncated":   r.Truncated,
			"reason":      r.Validation.Reason,
			"violations":  r.Validation.Violations,
		})
		a.events.Publish("sandbox.exec", string(event))
	})

	a.sandbox = sb
	// 走 RegisterTool 持锁写入：initSandbox 在 New 中以 goroutine 形式运行，
	// 与同期的 RAG/search_web 注册存在并发，必须串行化。
	a.RegisterTool(toolimpl.ExecCommandTool(sb))
	log.Printf("🛡️  沙箱已就绪，后端=%s，exec_command 工具已注册", sb.Backend())
}

// Sandbox 暴露沙箱实例，供 HTTP handler 或前端查询状态
func (a *UnifiedAgent) Sandbox() *sandbox.Sandbox { return a.sandbox }

// extractShellCommand 从用户自然语言查询中提取实际的 shell 命令
func extractShellCommand(query string) string {
	// 简单提取：去掉常见中文前缀后，取第一个词作为命令
	q := query
	for _, prefix := range []string{"执行", "运行", "请执行", "请运行", "帮我执行", "帮我运行"} {
		if strings.HasPrefix(q, prefix) {
			q = strings.TrimPrefix(q, prefix)
			break
		}
	}
	// 去掉常见中文后缀
	for _, suffix := range []string{"命令", "查看CPU信息", "查看内存信息", "查看磁盘信息", "查看系统信息", "查看信息"} {
		if strings.HasSuffix(q, suffix) {
			q = strings.TrimSuffix(q, suffix)
			break
		}
	}
	q = strings.TrimSpace(q)
	if q != "" {
		return q
	}
	return query
}
