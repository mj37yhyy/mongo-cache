package cache

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
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
	// 是否启用查询缓存
	EnableQueryCache bool
	// 查询缓存的过期时间
	QueryTTL time.Duration
	// 查询匹配器函数，用于自定义判断文档是否匹配查询条件
	QueryMatcher func(doc bson.M, query bson.M) bool
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
		EnableQueryCache:    true,
		QueryTTL:            time.Minute * 5,
		QueryMatcher:        DefaultQueryMatcher,
	}
}

// DefaultQueryMatcher 提供基本的查询匹配逻辑
func DefaultQueryMatcher(doc bson.M, query bson.M) bool {
	// 简单实现：只处理基本的相等比较
	for k, v := range query {
		// 跳过复杂查询操作符
		if k[0] == '$' {
			continue
		}

		// 如果是子查询（如 {field: {$gt: 10}}）
		if _, ok := v.(bson.M); ok {
			// 对于复杂查询，默认匹配器不处理，返回false
			// 用户应该提供自己的匹配器来处理复杂查询
			return false
		} else {
			// 简单的相等比较
			docValue, exists := doc[k]
			if !exists || docValue != v {
				return false
			}
		}
	}
	return true
}
