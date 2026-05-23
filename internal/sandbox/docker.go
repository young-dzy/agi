package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// DockerSandbox 通过 docker CLI 在容器内执行命令
//
// 关键安全约束（作为 docker run 参数传入）:
//   --rm                  执行完自动清理容器
//   --network none        禁用网络
//   --read-only           根文件系统只读
//   --tmpfs /tmp:size=...允许 /tmp 临时写入
//   --memory / --cpus / --pids-limit  cgroup 资源硬限制
//   --security-opt no-new-privileges  禁止权限提升
//   --cap-drop ALL        放弃所有 Linux capabilities
type DockerSandbox struct {
	cfg       SandboxConfig
	available bool
}

// NewDockerSandbox 创建 DockerSandbox，并通过 docker version 检测可用性
func NewDockerSandbox(cfg SandboxConfig) *DockerSandbox {
	d := &DockerSandbox{cfg: cfg}
	d.available = d.probe()
	return d
}

// Backend 返回后端名称
func (d *DockerSandbox) Backend() string { return "docker" }

// Available 报告 docker daemon 是否可用
func (d *DockerSandbox) Available() bool { return d.available }

// probe 通过 docker version 命令检测 daemon 是否就绪
func (d *DockerSandbox) probe() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// Exec 在隔离容器中运行命令
func (d *DockerSandbox) Exec(ctx context.Context, req ExecRequest) ExecResult {
	start := time.Now()
	result := ExecResult{Command: req.Command, Backend: "docker"}

	if !d.available {
		result.ExitCode = -3
		result.Stderr = "Docker 后端不可用"
		return result
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = d.cfg.Timeout
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := d.buildDockerArgs(req.Command)

	cmd := exec.CommandContext(execCtx, "docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = limitWriter(&stdout, d.cfg.MaxOutputBytes)
	cmd.Stderr = limitWriter(&stderr, d.cfg.MaxOutputBytes)

	err := cmd.Run()
	result.Duration = time.Since(start)
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		result.Killed = true
		result.ExitCode = -4
		if !strings.Contains(result.Stderr, "超时") {
			result.Stderr += fmt.Sprintf("\n[超时] 执行超过 %v 被强制终止", timeout)
		}
		return result
	}

	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
		} else {
			result.ExitCode = -5
			result.Stderr += "\n[沙箱内部错误] " + err.Error()
		}
	}

	// 输出截断标记
	if d.cfg.MaxOutputBytes > 0 &&
		(stdout.Len() >= d.cfg.MaxOutputBytes || stderr.Len() >= d.cfg.MaxOutputBytes) {
		result.Truncated = true
	}

	return result
}

// buildDockerArgs 构造 docker run 的完整参数列表
func (d *DockerSandbox) buildDockerArgs(command string) []string {
	args := []string{
		"run",
		"--rm",
		"-i",
		"--security-opt", "no-new-privileges",
		"--cap-drop", "ALL",
	}

	if d.cfg.NetworkDisabled {
		args = append(args, "--network", "none")
	}
	if d.cfg.ReadOnlyRootfs {
		args = append(args, "--read-only", "--tmpfs", "/tmp:rw,size=64m")
	}
	if d.cfg.MemoryLimitMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", d.cfg.MemoryLimitMB))
	}
	if d.cfg.CPUPercent > 0 {
		// docker 的 --cpus 接受小数核心数；50% → 0.5
		args = append(args, "--cpus", fmt.Sprintf("%.2f", float64(d.cfg.CPUPercent)/100.0))
	}
	if d.cfg.MaxPIDs > 0 {
		args = append(args, "--pids-limit", fmt.Sprintf("%d", d.cfg.MaxPIDs))
	}

	image := d.cfg.Image
	if image == "" {
		image = "alpine:3.19"
	}
	args = append(args, image, "sh", "-c", command)
	return args
}

// limitWriter 返回一个带字节上限的 io.Writer
func limitWriter(buf *bytes.Buffer, max int) io.Writer {
	if max <= 0 {
		return buf
	}
	return &limitedWriter{w: buf, remaining: max}
}

type limitedWriter struct {
	w         *bytes.Buffer
	remaining int
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.remaining <= 0 {
		return len(p), nil // 丢弃但不报错，保证命令本身能正常结束
	}
	if len(p) > lw.remaining {
		lw.w.Write(p[:lw.remaining])
		lw.remaining = 0
		return len(p), nil
	}
	n, err := lw.w.Write(p)
	lw.remaining -= n
	return n, err
}
