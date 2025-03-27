package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CacheEntry 表示缓存中的一个条目
type CacheEntry struct {
	Data       bson.M
	LastAccess time.Time
	ExpireAt   time.Time
}

// MongoCache 实现MongoDB集合的本地缓存
type MongoCache struct {
	config     *Config
	collection *mongo.Collection
	cache      map[string]*CacheEntry
	mutex      sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
}

// New 创建一个新的MongoCache实例
func New(config *Config) (*MongoCache, error) {
	if config.Client == nil {
		return nil, errors.New("MongoDB客户端不能为空")
	}

	if config.Database == "" || config.Collection == "" {
		return nil, errors.New("数据库名和集合名不能为空")
	}

	ctx, cancel := context.WithCancel(context.Background())

	cache := &MongoCache{
		config:     config,
		collection: config.Client.Database(config.Database).Collection(config.Collection),
		cache:      make(map[string]*CacheEntry),
		ctx:        ctx,
		cancel:     cancel,
	}

	return cache, nil
}

// Start 开始监听变更并初始化缓存
func (c *MongoCache) Start() error {
	// 首先加载现有数据
	if err := c.loadInitialData(); err != nil {
		return fmt.Errorf("加载初始数据失败: %w", err)
	}

	// 启动变更流监听
	go c.startChangeStream()

	// 如果设置了TTL，启动过期清理
	if c.config.TTL > 0 {
		go c.startExpirationChecker()
	}

	return nil
}

// Stop 停止缓存服务
func (c *MongoCache) Stop() {
	c.cancel()
}

// Get 从缓存获取文档
func (c *MongoCache) Get(id string) (bson.M, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	entry, exists := c.cache[id]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if !entry.ExpireAt.IsZero() && time.Now().After(entry.ExpireAt) {
		return nil, false
	}

	// 更新最后访问时间
	entry.LastAccess = time.Now()

	return entry.Data, true
}

// GetAll 获取缓存中的所有文档
func (c *MongoCache) GetAll() map[string]bson.M {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string]bson.M)
	now := time.Now()

	for id, entry := range c.cache {
		// 跳过过期的条目
		if !entry.ExpireAt.IsZero() && now.After(entry.ExpireAt) {
			continue
		}

		// 更新最后访问时间
		entry.LastAccess = now
		result[id] = entry.Data
	}

	return result
}

// Update 更新缓存中的文档并同步到数据库（如果启用）
func (c *MongoCache) Update(id string, update bson.M) error {
	if !c.config.EnableReverseUpdate {
		return errors.New("反向更新功能未启用")
	}

	// 检查ID是否有效
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("无效的ObjectID: %w", err)
	}

	// 更新数据库
	filter := bson.M{"_id": objectID}
	updateDoc := bson.M{"$set": update}

	_, err = c.collection.UpdateOne(context.Background(), filter, updateDoc)
	if err != nil {
		return fmt.Errorf("更新数据库失败: %w", err)
	}

	// 数据库更新成功后，变更流会自动更新缓存
	return nil
}

// Insert 在数据库中插入新文档并通过变更流更新缓存
func (c *MongoCache) Insert(document bson.M) (string, error) {
	if !c.config.EnableReverseUpdate {
		return "", errors.New("反向更新功能未启用")
	}

	// 确保文档有 _id 字段，如果没有则自动生成
	if _, hasID := document["_id"]; !hasID {
		document["_id"] = primitive.NewObjectID()
	}

	// 插入文档到数据库
	_, err := c.collection.InsertOne(context.Background(), document)
	if err != nil {
		return "", fmt.Errorf("插入文档失败: %w", err)
	}

	// 获取文档ID
	id, ok := document["_id"].(primitive.ObjectID)
	if !ok {
		return "", errors.New("无法获取插入文档的ID")
	}

	// 数据库插入成功后，变更流会自动更新缓存
	return id.Hex(), nil
}

// Delete 从数据库中删除文档并通过变更流更新缓存
func (c *MongoCache) Delete(id string) error {
	if !c.config.EnableReverseUpdate {
		return errors.New("反向更新功能未启用")
	}

	// 检查ID是否有效
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("无效的ObjectID: %w", err)
	}

	// 从数据库删除文档
	filter := bson.M{"_id": objectID}
	result, err := c.collection.DeleteOne(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("删除文档失败: %w", err)
	}

	// 检查是否有文档被删除
	if result.DeletedCount == 0 {
		return errors.New("未找到要删除的文档")
	}

	// 数据库删除成功后，变更流会自动更新缓存
	return nil
}

// loadInitialData 加载集合中的初始数据到缓存
func (c *MongoCache) loadInitialData() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 创建查询选项
	findOptions := options.Find()

	// 如果指定了字段，只获取这些字段
	if len(c.config.Fields) > 0 {
		projection := bson.M{}
		for _, field := range c.config.Fields {
			projection[field] = 1
		}
		findOptions.SetProjection(projection)
	}

	// 查询所有文档
	cursor, err := c.collection.Find(context.Background(), bson.M{}, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	// 遍历结果并添加到缓存
	for cursor.Next(context.Background()) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return err
		}

		id, ok := doc["_id"].(primitive.ObjectID)
		if !ok {
			continue
		}

		var expireAt time.Time
		if c.config.TTL > 0 {
			expireAt = time.Now().Add(c.config.TTL)
		}

		c.cache[id.Hex()] = &CacheEntry{
			Data:       doc,
			LastAccess: time.Now(),
			ExpireAt:   expireAt,
		}
	}

	return cursor.Err()
}

// startChangeStream 启动MongoDB变更流监听
func (c *MongoCache) startChangeStream() {
	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// 创建变更流
	stream, err := c.collection.Watch(c.ctx, pipeline, opts)
	if err != nil {
		fmt.Printf("创建变更流失败: %v\n", err)
		return
	}
	defer stream.Close(c.ctx)

	// 监听变更事件
	for stream.Next(c.ctx) {
		var event struct {
			OperationType string `bson:"operationType"`
			DocumentKey   struct {
				ID primitive.ObjectID `bson:"_id"`
			} `bson:"documentKey"`
			FullDocument bson.M `bson:"fullDocument"`
		}

		if err := stream.Decode(&event); err != nil {
			fmt.Printf("解码变更事件失败: %v\n", err)
			continue
		}

		c.handleChangeEvent(event.OperationType, event.DocumentKey.ID.Hex(), event.FullDocument)
	}

	if err := stream.Err(); err != nil && err != context.Canceled {
		fmt.Printf("变更流错误: %v\n", err)
	}
}

// handleChangeEvent 处理变更事件
func (c *MongoCache) handleChangeEvent(operationType, id string, document bson.M) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch operationType {
	case "insert", "update", "replace":
		// 如果指定了字段，只保留这些字段
		if len(c.config.Fields) > 0 {
			filteredDoc := bson.M{"_id": document["_id"]}
			for _, field := range c.config.Fields {
				if value, exists := document[field]; exists {
					filteredDoc[field] = value
				}
			}
			document = filteredDoc
		}

		var expireAt time.Time
		if c.config.TTL > 0 {
			expireAt = time.Now().Add(c.config.TTL)
		}

		c.cache[id] = &CacheEntry{
			Data:       document,
			LastAccess: time.Now(),
			ExpireAt:   expireAt,
		}

	case "delete":
		delete(c.cache, id)
	}

	// 检查缓存大小限制
	if c.config.MaxSize > 0 && len(c.cache) > c.config.MaxSize {
		c.evictLeastRecentlyUsed()
	}
}

// evictLeastRecentlyUsed 移除最近最少使用的缓存条目
func (c *MongoCache) evictLeastRecentlyUsed() {
	var oldestID string
	var oldestTime time.Time

	// 找到最旧的条目
	first := true
	for id, entry := range c.cache {
		if first || entry.LastAccess.Before(oldestTime) {
			oldestID = id
			oldestTime = entry.LastAccess
			first = false
		}
	}

	// 移除最旧的条目
	if oldestID != "" {
		delete(c.cache, oldestID)
	}
}

// startExpirationChecker 启动过期检查器
func (c *MongoCache) startExpirationChecker() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanExpiredEntries()
		case <-c.ctx.Done():
			return
		}
	}
}

// cleanExpiredEntries 清理过期的缓存条目
func (c *MongoCache) cleanExpiredEntries() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	for id, entry := range c.cache {
		if !entry.ExpireAt.IsZero() && now.After(entry.ExpireAt) {
			delete(c.cache, id)
		}
	}
}
