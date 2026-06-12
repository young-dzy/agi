// cancel.go — UnifiedAgent 的并发执行 helper：
//
//   - cancelFns map：每个 in-flight 请求一个 token，Cancel() 触发全部
//   - currentTask / setTask：持锁访问当前任务状态
//   - goSafe：带 panic recover 的后台 goroutine 启动器
//
// 这些是从 agent.go 内的 "内部并发 helper" 区段抽出，与主流程解耦。
package chat

import (
	"context"
	"log"
	"runtime/debug"
)

// registerCancel 把本次请求的 cancel 函数挂到 agent 上，返回反注册的函数。
// 多请求并发时每个请求拿到独立 token；Cancel() 触发全部 in-flight 中断。
func (a *UnifiedAgent) registerCancel(cancel context.CancelFunc) func() {
	a.mu.Lock()
	if a.cancelFns == nil {
		a.cancelFns = make(map[int64]context.CancelFunc)
	}
	a.nextCancelID++
	id := a.nextCancelID
	a.cancelFns[id] = cancel
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		delete(a.cancelFns, id)
		a.mu.Unlock()
		cancel() // 幂等：context.CancelFunc 自带 once 保护
	}
}

// currentTask 持锁返回当前 task 引用（可能为 nil）
func (a *UnifiedAgent) currentTask() *TaskState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.task
}

// setTask 持锁设置当前 task，并清空 snapshots（新任务开始）
func (a *UnifiedAgent) setTask(t *TaskState) {
	a.mu.Lock()
	a.task = t
	a.snapshots = nil
	a.mu.Unlock()
}

// Cancel 触发所有 in-flight 请求的 ctx 取消（用于 /api/chat/cancel）
func (a *UnifiedAgent) Cancel() {
	a.mu.Lock()
	fns := make([]context.CancelFunc, 0, len(a.cancelFns))
	for _, fn := range a.cancelFns {
		fns = append(fns, fn)
	}
	a.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// goSafe 启动一个带 panic recover 的后台 goroutine。
//
// agent 有大量 fire-and-forget 异步任务（偏好提取、记忆挖掘、记忆合并、
// Neo4j 异步写、KG 索引等）。任意一处 panic（比如 Neo4j 突然断连后某处空指针）
// 在裸 go func() 下会让整个进程崩溃，影响其他正常请求。
//
// 这个 helper 给所有异步任务统一兜底：
//   - 捕获 panic 并打印 stack trace（便于事后排查）
//   - name 标记任务来源，方便日志检索
//   - 不影响业务返回值（任务失败时静默丢弃）
func (a *UnifiedAgent) goSafe(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("⚠️  goroutine panic [%s]: %v\n%s", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}
