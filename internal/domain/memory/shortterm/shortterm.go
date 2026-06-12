// Package shortterm 短期记忆：固定窗口的对话历史。
//
// ShortTerm 维护最近 MaxTurns 轮（每轮 user + assistant 两条）的对话上下文，
// 超出窗口时自动丢弃最早记录。不持久化——进程消亡即清空。
//
// 并发安全：所有公共方法持锁。直接读 .Messages 字段（旧代码 / JSON 序列化）请用 Snapshot()。
package shortterm

import (
	"sync"
	"time"
)

// ConversationMessage 是单条对话记录
type ConversationMessage struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
}

// ShortTerm 维护最近 MaxTurns 轮的对话上下文
type ShortTerm struct {
	mu       sync.RWMutex
	Messages []ConversationMessage `json:"messages"`
	MaxTurns int                   `json:"max_turns"`
}

// New 创建短期记忆，maxTurns 为保留的最大对话轮数
func New(maxTurns int) *ShortTerm {
	return &ShortTerm{MaxTurns: maxTurns}
}

// Add 追加一条消息，超出窗口时自动丢弃最早记录
func (m *ShortTerm) Add(role, content string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = append(m.Messages, ConversationMessage{
		Role:      role,
		Content:   content,
		Timestamp: time.Now().Format("15:04:05"),
	})
	max := m.MaxTurns * 2 // 每轮 = user + assistant 两条
	if len(m.Messages) > max {
		m.Messages = m.Messages[len(m.Messages)-max:]
	}
}

// Snapshot 返回当前 Messages 的副本（持读锁）
func (m *ShortTerm) Snapshot() []ConversationMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]ConversationMessage, len(m.Messages))
	copy(cp, m.Messages)
	return cp
}

// Count 返回当前消息数（持读锁）
func (m *ShortTerm) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Messages)
}
