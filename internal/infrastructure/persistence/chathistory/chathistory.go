// Package chathistory 是聊天记录仓储。
//
// PG 实现：write to chat_history (role, content, created_at).
package chathistory

import (
	"database/sql"
	"log"
)

// Entry 是一条聊天记录的领域模型
type Entry struct {
	Role      string
	Content   string
	CreatedAt string // 'HH:MM:SS' 形式（用于回显）
}

// Repo 聊天记录仓储接口
type Repo interface {
	Save(role, content string)
	Load(limit int) []Entry
}

// PGRepo 是 Postgres 实现
type PGRepo struct {
	db *sql.DB
}

// NewPGRepo 创建 PG 实现；db 为 nil 时返回的实例所有方法都是无操作降级。
func NewPGRepo(db *sql.DB) *PGRepo { return &PGRepo{db: db} }

// Save 持久化一条聊天记录
func (r *PGRepo) Save(role, content string) {
	if r.db == nil {
		return
	}
	_, err := r.db.Exec(
		`INSERT INTO chat_history (role, content) VALUES ($1, $2)`,
		role, content,
	)
	if err != nil {
		log.Printf("⚠️  聊天记录保存到 PG 失败: %v", err)
	}
}

// Load 加载最近 N 条聊天记录（按时间正序返回）
func (r *PGRepo) Load(limit int) []Entry {
	if r.db == nil {
		return nil
	}
	rows, err := r.db.Query(
		`SELECT role, content, TO_CHAR(created_at, 'HH24:MI:SS') FROM chat_history ORDER BY id DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		log.Printf("⚠️  加载聊天记录失败: %v", err)
		return nil
	}
	defer rows.Close()
	var result []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Role, &e.Content, &e.CreatedAt); err == nil {
			result = append(result, e)
		}
	}
	// 反转为时间正序
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}
