// Package milvus 提供 Milvus 向量数据库连接的薄封装。
// 业务级集合管理与搜索方法不放这里——保持平台层无业务语义。
package milvus

import (
	"context"
	"log"

	"agi-assistant/config"

	milvusClient "github.com/milvus-io/milvus-sdk-go/v2/client"
)

// Connect 尝试连接 Milvus；失败时返回 (nil, "disconnected")，调用方决定是否降级。
func Connect(cfg config.MilvusConfig) (milvusClient.Client, string) {
	mc, err := milvusClient.NewClient(context.Background(), milvusClient.Config{
		Address: cfg.MilvusAddr(),
	})
	if err != nil {
		log.Printf("⚠️  Milvus 连接失败: %v (将使用内存向量库)", err)
		return nil, "disconnected"
	}
	log.Println("✅ Milvus 已连接:", cfg.MilvusAddr())
	return mc, "connected"
}
