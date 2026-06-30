// Package httpmw 提供 HTTP 横切中间件：requestID / accessLog / panicRecover / cors。
//
// 设计目标：
//   - 0 第三方依赖（除 chi 本身）
//   - 中间件之间松耦合，可独立启用 / 禁用
//   - 通过 context 传递 requestID，让深层调用（LLM 客户端、PG 查询）也能拿到
//
// 使用顺序（典型）：
//
//	r := chi.NewRouter()
//	r.Use(httpmw.RequestID)        // 必须最早——后续中间件都要靠它
//	r.Use(httpmw.PanicRecover)     // 紧随其后——给后面所有 handler 兜底
//	r.Use(httpmw.AccessLog)        // 出口前打访问日志
//	r.Use(httpmw.CORS(cfg))        // 跨域
package httpmw

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

// ─────────────────────────────── RequestID ────────────────────────────────

// requestIDKey 是 context 中存放 requestID 的私有键，避免与外部键冲突
type requestIDKey struct{}

// RequestIDHeader 是客户端 / 服务端约定的 header 名
const RequestIDHeader = "X-Request-Id"

// RequestID 中间件：
//   - 从入站 header 读取（让客户端可以指定 ID 用于跨服务追踪）
//   - 没有则生成 16 字节十六进制（128 位，碰撞概率可忽略）
//   - 写入 ctx + response header
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFromContext 从 ctx 取出 requestID。空时返回 "-" 便于日志拼接。
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey{}).(string); ok && v != "" {
		return v
	}
	return "-"
}

// newRequestID 16 字节随机 → 32 位十六进制
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 不应失败；万一失败用时间戳兜底（仍可见，便于发现）
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// ─────────────────────────────── AccessLog ────────────────────────────────

// responseRecorder 包 http.ResponseWriter 抓 status / body size。
// 为支持 SSE 路由，必须实现 http.Flusher（chi 不会代理 Flush）。
type responseRecorder struct {
	http.ResponseWriter
	status     int
	bytesWrote int
	wroteHead  bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHead {
		return
	}
	r.status = code
	r.wroteHead = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHead {
		r.status = http.StatusOK
		r.wroteHead = true
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytesWrote += n
	return n, err
}

// Flush 透传 http.Flusher——SSE 路由依赖此能力。
// 如果底层 ResponseWriter 不支持 Flusher，调用本方法不报错（no-op）。
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// AccessLog 中间件：在 handler 退出后打一条访问日志。
//
// 字段顺序（便于 grep 和后续 zap/slog 平滑迁移）：
//
//	[ACCESS] req=<id> method=<X> path=<X> status=<X> bytes=<X> dur=<Xms> remote=<X>
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		dur := time.Since(start)

		// SSE 路径或长请求只打"完成"那一行，避免双倍日志
		log.Printf("[ACCESS] req=%s method=%s path=%s status=%d bytes=%d dur=%dms remote=%s",
			RequestIDFromContext(r.Context()),
			r.Method, r.URL.Path,
			statusOr200(rec.status), rec.bytesWrote,
			dur.Milliseconds(),
			clientIP(r),
		)
	})
}

func statusOr200(s int) int {
	if s == 0 {
		return http.StatusOK
	}
	return s
}

// clientIP 从 X-Forwarded-For / X-Real-Ip / RemoteAddr 取首个可用 IP。
// 反向代理后部署时 RemoteAddr 是网关地址，必须看 XFF。
func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		// XFF 是逗号分隔的链路，第一个是真实客户端
		if idx := strings.Index(v, ","); idx >= 0 {
			return strings.TrimSpace(v[:idx])
		}
		return strings.TrimSpace(v)
	}
	if v := r.Header.Get("X-Real-Ip"); v != "" {
		return v
	}
	return r.RemoteAddr
}

// ─────────────────────────────── PanicRecover ─────────────────────────────

// PanicRecover 中间件：捕获 handler 内的 panic，避免一个崩溃的 handler 拖死整个进程。
// 返回 500 + request_id，让用户可反馈给运维定位日志。
func PanicRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				reqID := RequestIDFromContext(r.Context())
				// 完整 stack 打到日志，但不返给客户端（避免泄漏内部路径）
				log.Printf("[PANIC] req=%s path=%s panic=%v\n%s",
					reqID, r.URL.Path, rec, debug.Stack())
				// 客户端只看到通用 500 + request_id
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"error":"internal server error","request_id":%q}`, reqID)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ─────────────────────────────── CORS ─────────────────────────────────────

// CORSConfig 跨域配置。空切片表示"允许任何来源"——仅用于本机开发。
// 生产环境必须显式列出 AllowedOrigins。
type CORSConfig struct {
	AllowedOrigins []string // 空 → "*"（仅开发用）
	AllowedMethods []string
	AllowedHeaders []string
	MaxAgeSeconds  int
}

// DefaultCORSConfig 返回宽松的默认配置（适合本地开发；生产请覆盖）。
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins: nil, // → "*"
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization", RequestIDHeader},
		MaxAgeSeconds:  600,
	}
}

// CORS 返回 CORS 中间件。OPTIONS 预检直接 204 短路。
func CORS(cfg CORSConfig) func(http.Handler) http.Handler {
	allowOrigin := "*"
	originSet := map[string]bool{}
	for _, o := range cfg.AllowedOrigins {
		if o == "*" {
			allowOrigin = "*"
			break
		}
		originSet[o] = true
	}
	methods := strings.Join(cfg.AllowedMethods, ", ")
	headers := strings.Join(cfg.AllowedHeaders, ", ")

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				if allowOrigin == "*" && len(originSet) == 0 {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else if originSet[origin] {
					// 显式回显请求 Origin（而非 "*"）才能配合 Allow-Credentials
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", methods)
			w.Header().Set("Access-Control-Allow-Headers", headers)
			if cfg.MaxAgeSeconds > 0 {
				w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", cfg.MaxAgeSeconds))
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
