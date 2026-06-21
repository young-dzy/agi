// Package longterm 是长期记忆条目的仓储。
package longterm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Row 是长期记忆条目的领域模型
type Row struct {
	ID           int
	Content      string
	Importance   float64
	Embedding    []float64
	CreatedAt    time.Time
	LastAccessed time.Time
	Category     string
	Tags         []string
	SlotHint     string
}

// Repo 长期记忆仓储接口
type Repo interface {
	Save(content string, importance float64, embeddingJSON []byte) int
	SaveClassified(content string, importance float64, embeddingJSON []byte,
		category string, tags []string, slotHint string) int
	Load() []Row
	Update(id int, content string, importance float64, embeddingJSON []byte)
	UpdateImportanceBatch(items []ImportanceUpdate)
	Delete(ids []int)
}

// ImportanceUpdate 批量衰减时的最小变更单元
type ImportanceUpdate struct {
	ID         int
	Importance float64
}

// PGRepo 是 Postgres 实现
type PGRepo struct {
	db *sql.DB
}

func NewPGRepo(db *sql.DB) *PGRepo { return &PGRepo{db: db} }

// Save 默认分类 "general" 写入
func (r *PGRepo) Save(content string, importance float64, embeddingJSON []byte) int {
	return r.SaveClassified(content, importance, embeddingJSON, "general", nil, "")
}

// SaveClassified 带分类信息写入
func (r *PGRepo) SaveClassified(content string, importance float64, embeddingJSON []byte,
	category string, tags []string, slotHint string) int {
	if r.db == nil {
		return -1
	}
	if category == "" {
		category = "general"
	}
	if tags == nil {
		tags = []string{}
	}
	var id int
	err := r.db.QueryRow(
		`INSERT INTO long_term_memory (content, importance, embedding, category, tags, slot_hint)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, '')) RETURNING id`,
		content, importance, embeddingJSON, category, pq.Array(tags), slotHint,
	).Scan(&id)
	if err != nil {
		log.Printf("⚠️  长期记忆保存失败: %v", err)
		return -1
	}
	return id
}

// Load 加载全部长期记忆条目
func (r *PGRepo) Load() []Row {
	if r.db == nil {
		return nil
	}
	rows, err := r.db.Query(`SELECT id, content, importance, embedding,
		COALESCE(created_at, NOW()), COALESCE(last_accessed, NOW()),
		COALESCE(category, 'general'), COALESCE(tags, '{}'::TEXT[]), COALESCE(slot_hint, '')
		FROM long_term_memory ORDER BY id`)
	if err != nil {
		log.Printf("⚠️  加载长期记忆失败: %v", err)
		return nil
	}
	defer rows.Close()
	var items []Row
	for rows.Next() {
		var row Row
		var embJSON []byte
		var tags pq.StringArray
		if err := rows.Scan(&row.ID, &row.Content, &row.Importance, &embJSON,
			&row.CreatedAt, &row.LastAccessed, &row.Category, &tags, &row.SlotHint); err != nil {
			continue
		}
		if len(embJSON) > 0 {
			json.Unmarshal(embJSON, &row.Embedding)
		}
		row.Tags = []string(tags)
		items = append(items, row)
	}
	return items
}

// Update 修改一条长期记忆
func (r *PGRepo) Update(id int, content string, importance float64, embeddingJSON []byte) {
	if r.db == nil {
		return
	}
	_, err := r.db.Exec(
		`UPDATE long_term_memory SET content = $1, importance = $2, embedding = $3, last_accessed = NOW() WHERE id = $4`,
		content, importance, embeddingJSON, id,
	)
	if err != nil {
		log.Printf("⚠️  长期记忆更新失败 (id=%d): %v", id, err)
	}
}

// Delete 批量删除
func (r *PGRepo) Delete(ids []int) {
	if r.db == nil || len(ids) == 0 {
		return
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf("DELETE FROM long_term_memory WHERE id IN (%s)", strings.Join(placeholders, ","))
	if _, err := r.db.Exec(query, args...); err != nil {
		log.Printf("⚠️  长期记忆批量删除失败: %v", err)
	}
}

// UpdateImportanceBatch 用单条 SQL（VALUES + UPDATE FROM）批量刷新 importance。
// 衰减阶段每次 Consolidate 都会触达全表，单条 UPDATE 会产生 N 次往返；
// 这里用 unnest 把 (id, importance) 数组下推一次完成。
func (r *PGRepo) UpdateImportanceBatch(items []ImportanceUpdate) {
	if r.db == nil || len(items) == 0 {
		return
	}
	ids := make([]int64, len(items))
	imps := make([]float64, len(items))
	for i, it := range items {
		ids[i] = int64(it.ID)
		imps[i] = it.Importance
	}
	_, err := r.db.Exec(
		`UPDATE long_term_memory AS m
		 SET importance = v.importance
		 FROM (SELECT unnest($1::BIGINT[]) AS id, unnest($2::DOUBLE PRECISION[]) AS importance) AS v
		 WHERE m.id = v.id`,
		pq.Array(ids), pq.Array(imps),
	)
	if err != nil {
		log.Printf("⚠️  长期记忆批量衰减更新失败 (n=%d): %v", len(items), err)
	}
}
