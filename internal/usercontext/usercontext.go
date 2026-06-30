// Package usercontext 是请求级用户身份在 context 中的统一读写入口。
//
// 为什么单独建包而不放在 httpmw 或 chat：
//   - httpmw（interfaces 层）和 chat（application 层）都需要读写它
//   - 互相 import 会产生循环依赖
//   - 中性包不依赖任何业务，谁都可以引用
//
// 写入侧：HTTP 中间件解 JWT 后调 With；CLI / 测试入口直接调 With。
// 读取侧：application/domain 层调 UserIDFromContext / UsernameFromContext。
package usercontext

import "context"

// 私有 key 类型——禁止外部以字符串猜测 key 值碰撞。
type userIDKey struct{}
type usernameKey struct{}

// With 把 userID + username 写入 ctx 一并返回新 ctx。
// userID 空字符串视为"未登录"——下游业务层应据此拒绝写记忆 / 召回。
func With(ctx context.Context, userID, username string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, userIDKey{}, userID)
	ctx = context.WithValue(ctx, usernameKey{}, username)
	return ctx
}

// UserIDFromContext 取 userID。未设置时返回空——调用方决定如何处理。
func UserIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(userIDKey{}).(string); ok {
		return v
	}
	return ""
}

// UsernameFromContext 取用户名（仅日志/审计用，权限判定看 UserID）。
func UsernameFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(usernameKey{}).(string); ok {
		return v
	}
	return ""
}
