// Package es 提供 Elasticsearch 连接的薄封装。
package es

import (
	"log"

	"agi-assistant/config"

	es "github.com/elastic/go-elasticsearch/v8"
)

// Connect 尝试连接 ES 并 Info()-ping 验证。
// 失败时返回 (nil, "disconnected")。
func Connect(cfg config.ESConfig) (*es.Client, string) {
	esCfg := es.Config{
		Addresses: cfg.ESAddresses,
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
	}
	client, err := es.NewClient(esCfg)
	if err != nil {
		log.Printf("⚠️  Elasticsearch 连接失败: %v", err)
		return nil, "disconnected"
	}
	res, err := client.Info()
	if err != nil {
		log.Printf("⚠️  Elasticsearch Ping 失败: %v", err)
		return nil, "disconnected"
	}
	res.Body.Close()
	log.Println("✅ Elasticsearch 已连接:", cfg.ESAddresses)
	return client, "connected"
}
