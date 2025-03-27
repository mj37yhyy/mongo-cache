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

	// 创建缓存实例
	mongoCache, err := cache.New(config)
	if err != nil {
		log.Fatalf("创建缓存失败: %v", err)
	}

	// 启动缓存
	if err := mongoCache.Start(); err != nil {
		log.Fatalf("启动缓存失败: %v", err)
	}
	defer mongoCache.Stop()

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

	// 保持程序运行，以便观察变更流
	fmt.Println("缓存已启动，按Ctrl+C退出...")
	select {}
}
