package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mj37yhyy/mongocache/cache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 连接到MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("连接MongoDB失败: %v", err)
	}
	defer client.Disconnect(ctx)

	// 创建缓存配置
	config := cache.NewConfig(client, "testdb", "users")
	config.Fields = []string{"name", "email", "age"} // 只同步这些字段
	config.TTL = 30 * time.Minute                    // 缓存30分钟
	config.MaxSize = 1000                            // 最多缓存1000条记录
	config.EnableReverseUpdate = true                // 启用反向更新

	// 自定义查询匹配器
	config.QueryMatcher = func(doc bson.M, query bson.M) bool {
		// 示例：自定义匹配逻辑
		// 这里实现一个简单的匹配器，只检查年龄字段
		if ageQuery, ok := query["age"].(bson.M); ok {
			if gtValue, ok := ageQuery["$gt"].(float64); ok {
				if docAge, ok := doc["age"].(float64); ok {
					return docAge > gtValue
				}
			}
		}
		// 如果没有匹配到特定条件，使用默认匹配器
		return cache.DefaultQueryMatcher(doc, query)
	}

	// 创建缓存实例
	mongoCache, err := cache.New(config)
	if err != nil {
		log.Fatalf("创建缓存失败: %v", err)
	}

	// 使用查询条件启动缓存
	query := bson.M{"age": bson.M{"$gt": 25}}
	projection := bson.M{"name": 1, "age": 1}
	if err := mongoCache.StartWithQuery("older_users", query, projection); err != nil {
		log.Fatalf("使用查询条件启动缓存失败: %v", err)
	}
	defer mongoCache.Stop()

	// 从查询缓存获取数据
	if result, found := mongoCache.GetWithQuery("older_users", query); found {
		fmt.Println("查询缓存中的数据:")
		if data, ok := result.([]bson.M); ok {
			for _, item := range data {
				fmt.Printf("  - %v\n", item)
			}
		}
	}

	// 使用缓存
	// 从缓存获取文档
	if doc, found := mongoCache.Get("60d5ec2d82c3a12345678901"); found {
		fmt.Printf("从缓存获取到文档: %v\n", doc)
	}

	// 获取所有缓存的文档
	allDocs := mongoCache.GetAll()
	fmt.Printf("缓存中共有 %d 个文档\n", len(allDocs))

	// 通过缓存更新文档（会同步到数据库）
	err = mongoCache.Update("60d5ec2d82c3a12345678901", bson.M{
		"name": "新名字",
		"age":  30,
	})
	if err != nil {
		log.Printf("更新失败: %v", err)
	} else {
		fmt.Println("文档已更新")
	}

	// 创建新文档
	newDoc := bson.M{
		"name":  "张三",
		"email": "zhangsan@example.com",
		"age":   25,
	}

	newID, err := mongoCache.Insert(newDoc)
	if err != nil {
		log.Printf("创建文档失败: %v", err)
	} else {
		fmt.Printf("文档已创建，ID: %s\n", newID)

		// 从缓存获取新创建的文档
		if doc, found := mongoCache.Get(newID); found {
			fmt.Printf("新创建的文档: %v\n", doc)
		}
	}

	// 删除文档
	if newID != "" {
		time.Sleep(1 * time.Second) // 等待变更流更新缓存

		err = mongoCache.Delete(newID)
		if err != nil {
			log.Printf("删除文档失败: %v", err)
		} else {
			fmt.Println("文档已删除")

			// 验证文档已从缓存中删除
			if _, found := mongoCache.Get(newID); !found {
				fmt.Println("文档已从缓存中删除")
			}
		}
	}

	// 示例：使用查询条件缓存
	demoQueryCache(mongoCache)

	// 保持程序运行，以便观察变更流
	fmt.Println("缓存已启动，按Ctrl+C退出...")
	select {}
}

// 新增：演示查询条件缓存的使用
func demoQueryCache(cache *cache.MongoCache) {
	fmt.Println("演示查询条件缓存:")

	// 定义一些查询条件
	query1 := map[string]interface{}{
		"status": "active",
		"limit":  10,
	}

	query2 := map[string]interface{}{
		"status": "inactive",
		"limit":  20,
	}

	// 模拟数据库查询结果
	result1 := []string{"item1", "item2", "item3"}
	result2 := []string{"item4", "item5"}

	// 缓存不同查询条件的结果
	cache.SetWithQuery("users", query1, result1)
	cache.SetWithQuery("users", query2, result2)

	// 从缓存获取结果
	if cachedResult1, found := cache.GetWithQuery("users", query1); found {
		fmt.Printf("查询条件1的缓存结果: %v\n", cachedResult1)
	}

	if cachedResult2, found := cache.GetWithQuery("users", query2); found {
		fmt.Printf("查询条件2的缓存结果: %v\n", cachedResult2)
	}

	// 清除特定键的所有查询缓存
	cache.ClearQueryCache("users")
	fmt.Println("已清除users的所有查询缓存")

	// 验证缓存已清除
	if _, found := cache.GetWithQuery("users", query1); !found {
		fmt.Println("查询条件1的缓存已被清除")
	}
}
