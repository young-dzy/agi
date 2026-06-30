// Package postgres 提供 PostgreSQL 连接的薄封装与启动期 schema bootstrap。
package postgres

import (
	"database/sql"
	"log"
	"time"

	"agi-assistant/config"

	_ "github.com/lib/pq" // 驱动注册
)

// Connect 打开 PG 连接、Ping 验证并应用合理的连接池参数。
// 失败时返回 (nil, "disconnected")，调用方决定是否降级。
func Connect(cfg config.PostgresConfig) (*sql.DB, string) {
	pg, err := sql.Open("postgres", cfg.PGDSN())
	if err != nil {
		log.Printf("⚠️  PostgreSQL 打开失败: %v", err)
		return nil, "disconnected"
	}
	// 连接池调优：默认 unlimited 在并发量大时会打爆 PG max_connections。
	//   - MaxOpenConns 25：单实例上限，留余量给其他客户端
	//   - MaxIdleConns 5：维持最小空闲，避免每次冷连接
	//   - ConnMaxLifetime 30min：定期回收，防止云数据库 idle timeout 后用到失效连接
	pg.SetMaxOpenConns(25)
	pg.SetMaxIdleConns(5)
	pg.SetConnMaxLifetime(30 * time.Minute)
	pg.SetConnMaxIdleTime(5 * time.Minute)

	if err := pg.Ping(); err != nil {
		log.Printf("⚠️  PostgreSQL Ping 失败: %v", err)
		return nil, "disconnected"
	}
	log.Println("✅ PostgreSQL 已连接:", cfg.PGDSN())
	return pg, "connected"
}

// BootstrapSchema 幂等地创建/升级所有业务表。
// 业务表的 DDL 集中在此处便于 schema review；运行时只在启动阶段调用一次。
func BootstrapSchema(pg *sql.DB) {
	if pg == nil {
		return
	}
	ddls := []string{
		// 用户表：username 唯一，bcrypt 密码哈希。
		// 单体应用阶段用 SERIAL 自增；未来若要分布式可改 UUID。
		`CREATE TABLE IF NOT EXISTS users (
			id            SERIAL PRIMARY KEY,
			username      TEXT NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			created_at    TIMESTAMP DEFAULT NOW(),
			last_login_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)`,
		`CREATE TABLE IF NOT EXISTS user_preferences (
			user_id    TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      TEXT NOT NULL,
			updated_at TIMESTAMP DEFAULT NOW(),
			PRIMARY KEY (user_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS task_snapshots (
			task_id    TEXT PRIMARY KEY,
			state      JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS chat_history (
			id         SERIAL PRIMARY KEY,
			role       TEXT NOT NULL,
			content    TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS long_term_memory (
			id            SERIAL PRIMARY KEY,
			content       TEXT NOT NULL,
			importance    FLOAT NOT NULL DEFAULT 0.5,
			embedding     JSONB,
			created_at    TIMESTAMP DEFAULT NOW(),
			last_accessed TIMESTAMP DEFAULT NOW()
		)`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS last_accessed TIMESTAMP DEFAULT NOW()`,
		// Schema-driven 装配支持：分类 / 标签 / 槽位提示
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS category  TEXT NOT NULL DEFAULT 'general'`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS tags      TEXT[] NOT NULL DEFAULT '{}'`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS slot_hint TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_lti_category ON long_term_memory(category)`,
		`CREATE INDEX IF NOT EXISTS idx_lti_tags     ON long_term_memory USING GIN(tags)`,
		// 投毒防御：被 poison detector / 人工标记隔离的条目，不召回但保留证据
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS quarantined        BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS quarantine_reason  TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_lti_quarantined ON long_term_memory(quarantined) WHERE quarantined`,
		// 矛盾治理：被新条目取代的历史记忆，不召回但保留以便审计回滚
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS superseded     BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS superseded_at  TIMESTAMP`,
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS supersedes     INT[] NOT NULL DEFAULT '{}'`,
		`CREATE INDEX IF NOT EXISTS idx_lti_superseded ON long_term_memory(superseded) WHERE superseded`,
		// 多租户：所有"用户私有"数据加 user_id 列。
		// 老数据（迁移前的 user_id IS NULL 或 'default'）统一打 'legacy' 标签——
		// 防止新用户登录后看到他人记忆，又不丢历史数据便于审计。
		`ALTER TABLE long_term_memory ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT 'legacy'`,
		`CREATE INDEX IF NOT EXISTS idx_lti_user ON long_term_memory(user_id)`,
		`ALTER TABLE chat_history     ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT 'legacy'`,
		`CREATE INDEX IF NOT EXISTS idx_chat_user ON chat_history(user_id, id DESC)`,
		`ALTER TABLE task_snapshots   ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT 'legacy'`,
		// user_preferences 表的 user_id 之前是值 'default'——批量改名到 'legacy' 与上面对齐
		`UPDATE user_preferences SET user_id = 'legacy' WHERE user_id = 'default'`,
		`CREATE TABLE IF NOT EXISTS rag_chunks (
			id          BIGSERIAL PRIMARY KEY,
			doc_hash    TEXT NOT NULL,
			chunk_idx   INT NOT NULL,
			content     TEXT NOT NULL,
			embedding   JSONB,
			created_at  TIMESTAMP DEFAULT NOW(),
			UNIQUE(doc_hash, chunk_idx)
		)`,
		// 父子块（small-to-big）：检索用小块（精准），返回大块给 LLM（上下文完整）
		// 老行的 parent_content 为 NULL，HybridStore 会回退到 content 自身，向后兼容
		`ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS parent_content TEXT`,
		`CREATE TABLE IF NOT EXISTS documents (
			id         TEXT PRIMARY KEY,
			title      TEXT NOT NULL,
			doc_type   TEXT NOT NULL,
			source     TEXT NOT NULL,
			status     TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS document_versions (
			id          TEXT PRIMARY KEY,
			document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
			version     INT NOT NULL,
			content_md  TEXT NOT NULL,
			summary     TEXT,
			metadata    JSONB,
			created_at  TIMESTAMP DEFAULT NOW(),
			UNIQUE(document_id, version)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_document_versions_document ON document_versions(document_id, version DESC)`,
		`ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS document_id TEXT`,
		`ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS version_id TEXT`,
		`ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS section TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_rag_chunks_document ON rag_chunks(document_id, version_id)`,
	}
	for _, ddl := range ddls {
		if _, err := pg.Exec(ddl); err != nil {
			log.Printf("⚠️  PG 建表失败: %v", err)
		}
	}
	log.Println("✅ PostgreSQL 表结构已初始化")
}
