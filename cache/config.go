package cache

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// Config 定义缓存配置
type Config struct {
	// MongoDB 客户端
	Client *mongo.Client
	// 数据库名称
	Database string
	// 集合名称
	Collection string
	// 要同步的字段，为空表示同步所有字段
	Fields []string
	// 缓存过期时间，0表示永不过期
	TTL time.Duration
	// 缓存大小限制，0表示无限制
	MaxSize int
	// 是否启用反向更新
	EnableReverseUpdate bool
}

// NewConfig 创建默认配置
func NewConfig(client *mongo.Client, database, collection string) *Config {
	return &Config{
		Client:              client,
		Database:            database,
		Collection:          collection,
		Fields:              []string{},
		TTL:                 0,
		MaxSize:             0,
		EnableReverseUpdate: true,
	}
}
