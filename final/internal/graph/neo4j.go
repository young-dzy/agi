package graph

import (
	"context"
	"final/config"
	"log"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jStore 持有 Neo4j 驱动连接
type Neo4jStore struct {
	driver    neo4j.DriverWithContext
	available bool
}

// NewNeo4jStore 创建连接；连接失败时返回不可用实例，不阻塞启动
func NewNeo4jStore(cfg *config.APIConfig) *Neo4jStore {
	if !cfg.KGEnabled || cfg.Neo4jURI == "" {
		log.Printf("ℹ️  Neo4j 未启用（KGEnabled=%v, URI=%q）", cfg.KGEnabled, cfg.Neo4jURI)
		return &Neo4jStore{available: false}
	}

	driver, err := neo4j.NewDriverWithContext(
		cfg.Neo4jURI,
		neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, ""),
	)
	if err != nil {
		log.Printf("⚠️  Neo4j 驱动初始化失败: %v（知识图谱将降级跳过）", err)
		return &Neo4jStore{available: false}
	}

	// 连通性验证（超时 5s）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		log.Printf("⚠️  Neo4j 连通性验证失败: %v（知识图谱将降级跳过）", err)
		_ = driver.Close(context.Background())
		return &Neo4jStore{available: false}
	}

	store := &Neo4jStore{driver: driver, available: true}
	store.ensureConstraints()
	log.Printf("✅ Neo4j 已连接: %s", cfg.Neo4jURI)
	return store
}

// Available 报告 Neo4j 是否可用
func (s *Neo4jStore) Available() bool { return s.available }

// Close 关闭驱动
func (s *Neo4jStore) Close() {
	if s.driver != nil {
		s.driver.Close(context.Background())
	}
}

// session 返回一个写入 session；调用方需 defer session.Close
func (s *Neo4jStore) session() neo4j.SessionWithContext {
	return s.driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
}

// ensureConstraints 确保 Neo4j 中存在唯一约束（幂等）
func (s *Neo4jStore) ensureConstraints() {
	ctx := context.Background()
	sess := s.session()
	defer sess.Close(ctx)

	queries := []string{
		`CREATE CONSTRAINT entity_name IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS UNIQUE`,
		`CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.type)`,
		`CREATE INDEX memory_node_id IF NOT EXISTS FOR (m:Memory) ON (m.mem_id)`,
	}
	for _, q := range queries {
		if _, err := sess.Run(ctx, q, nil); err != nil {
			// 约束已存在或版本不支持时忽略
			log.Printf("ℹ️  Neo4j constraint/index: %v", err)
		}
	}
}
