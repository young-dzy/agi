package sandbox

import (
	"context"
	"errors"
	"fmt"
	"log"
)

// Executor 是沙箱执行器的统一接口
type Executor interface {
	Exec(ctx context.Context, req ExecRequest) ExecResult
	Backend() string
	Available() bool
}

// Sandbox 封装 Validator + Executor + 审计回调
type Sandbox struct {
	validator *Validator
	executor  Executor
	auditFn   func(ExecResult) // 审计回调（异步），由 agent 注入 Kafka 推送
}

// NewSandbox 根据后端名称构造 Sandbox。后端不可用时降级到 MockSandbox。
func NewSandbox(backend string, sandboxCfg SandboxConfig, secCfg SecurityConfig) *Sandbox {
	validator := NewValidator(secCfg)

	var exec Executor
	switch backend {
	case "docker":
		ds := NewDockerSandbox(sandboxCfg)
		if ds.Available() {
			exec = ds
		} else {
			log.Printf("⚠️  Docker 不可用，沙箱降级到 mock 模式")
			exec = NewMockSandbox()
		}
	case "local":
		exec = NewLocalSandbox(sandboxCfg)
	case "mock":
		exec = NewMockSandbox()
	default:
		log.Printf("⚠️  未知沙箱后端 %q，使用 mock", backend)
		exec = NewMockSandbox()
	}

	return &Sandbox{
		validator: validator,
		executor:  exec,
	}
}

// SetAuditFn 注入审计回调（在 Exec 完成后异步触发）
func (s *Sandbox) SetAuditFn(fn func(ExecResult)) {
	s.auditFn = fn
}

// Backend 返回当前底层执行后端名称
func (s *Sandbox) Backend() string {
	return s.executor.Backend()
}

// Validator 暴露 Validator 供工具层做预检
func (s *Sandbox) Validator() *Validator {
	return s.validator
}

// Exec 主入口：先校验，再执行，最后审计
func (s *Sandbox) Exec(ctx context.Context, req ExecRequest) ExecResult {
	// 1. 安全校验
	validation := s.validator.Validate(req.Command)

	result := ExecResult{
		Command:    req.Command,
		Validation: validation,
		Backend:    s.executor.Backend(),
	}

	// 2. Block 级直接拒绝，不进入执行
	if validation.Level == RiskBlock {
		result.ExitCode = -1
		result.Stderr = "[拒绝执行] " + validation.Reason
		s.audit(result)
		return result
	}

	// 3. Warn 级要求 confirm
	if validation.Level == RiskWarn && !req.Confirm {
		result.ExitCode = -2
		result.Stderr = fmt.Sprintf("[需要确认] 该命令触发以下规则：%v；请重新调用并设置 confirm=true", validation.Violations)
		s.audit(result)
		return result
	}

	// 4. 进入沙箱执行
	execResult := s.executor.Exec(ctx, req)
	execResult.Command = req.Command
	execResult.Validation = validation
	execResult.Backend = s.executor.Backend()

	s.audit(execResult)
	return execResult
}

func (s *Sandbox) audit(r ExecResult) {
	if s.auditFn != nil {
		go s.auditFn(r)
	}
}

// ErrSandboxUnavailable 表示底层沙箱后端不可用
var ErrSandboxUnavailable = errors.New("sandbox backend unavailable")
