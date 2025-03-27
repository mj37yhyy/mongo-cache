package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	queryCache map[string]interface{}
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
		queryCache: make(map[string]interface{}),
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

// 修改：使用查询条件启动缓存
func (c *MongoCache) StartWithQuery(key string, query bson.M, projection bson.M) error {
	// 执行查询并缓存结果
	if err := c.loadDataWithQuery(key, query, projection); err != nil {
		return fmt.Errorf("加载查询数据失败: %w", err)
	}

	// 启动变更流监听
	go c.startChangeStreamWithQuery(key, query, projection)

	// 如果设置了查询TTL，启动查询缓存过期清理
	if c.config.QueryTTL > 0 {
		go c.startQueryExpirationChecker()
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

// GetWithQuery 根据查询条件获取缓存数据
func (c *MongoCache) GetWithQuery(key string, query interface{}) (interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// 生成查询条件的唯一键
	queryKey := c.generateQueryKey(key, query)

	value, exists := c.queryCache[queryKey]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if entry, ok := value.(map[string]interface{}); ok {
		if expireAt, ok := entry["expireAt"].(time.Time); ok && !expireAt.IsZero() && time.Now().After(expireAt) {
			return nil, false
		}
		return entry["data"], true
	}

	return value, true
}

// SetWithQuery 根据查询条件设置缓存数据
func (c *MongoCache) SetWithQuery(key string, query interface{}, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 生成查询条件的唯一键
	queryKey := c.generateQueryKey(key, query)

	// 设置过期时间
	var expireAt time.Time
	if c.config.QueryTTL > 0 {
		expireAt = time.Now().Add(c.config.QueryTTL)
	}

	// 存储数据和过期时间
	c.queryCache[queryKey] = map[string]interface{}{
		"data":     value,
		"expireAt": expireAt,
	}
}

// generateQueryKey 生成查询条件的唯一键
func (c *MongoCache) generateQueryKey(key string, query interface{}) string {
	queryBytes, err := json.Marshal(query)
	if err != nil {
		// 如果无法序列化查询条件，则使用字符串表示
		return fmt.Sprintf("%s:%v", key, query)
	}
	return fmt.Sprintf("%s:%s", key, string(queryBytes))
}

// ClearQueryCache 清除特定键的所有查询缓存
func (c *MongoCache) ClearQueryCache(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	prefix := key + ":"
	for k := range c.queryCache {
		if strings.HasPrefix(k, prefix) {
			delete(c.queryCache, k)
		}
	}
}

// Clear 清除所有缓存数据
func (c *MongoCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache = make(map[string]*CacheEntry)
	c.queryCache = make(map[string]interface{})
}

// 新增：根据查询条件加载数据
func (c *MongoCache) loadDataWithQuery(key string, query bson.M, projection bson.M) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 创建查询选项
	findOptions := options.Find()
	if projection != nil {
		findOptions.SetProjection(projection)
	}

	// 执行查询
	cursor, err := c.collection.Find(context.Background(), query, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	// 收集查询结果
	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		return err
	}

	// 缓存查询结果
	var expireAt time.Time
	if c.config.QueryTTL > 0 {
		expireAt = time.Now().Add(c.config.QueryTTL)
	}

	// 将查询结果存储到查询缓存中
	c.queryCache[c.generateQueryKey(key, query)] = map[string]interface{}{
		"data":     results,
		"expireAt": expireAt,
	}

	return nil
}

// 新增：启动查询缓存过期检查器
func (c *MongoCache) startQueryExpirationChecker() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanExpiredQueryEntries()
		case <-c.ctx.Done():
			return
		}
	}
}

// 新增：清理过期的查询缓存条目
func (c *MongoCache) cleanExpiredQueryEntries() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	for key, value := range c.queryCache {
		if entry, ok := value.(map[string]interface{}); ok {
			if expireAt, ok := entry["expireAt"].(time.Time); ok && !expireAt.IsZero() && now.After(expireAt) {
				delete(c.queryCache, key)
			}
		}
	}
}

// 新增：启动针对特定查询的变更流监听
func (c *MongoCache) startChangeStreamWithQuery(key string, query bson.M, projection bson.M) {
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

		// 检查变更的文档是否符合查询条件
		if c.config.QueryMatcher(event.FullDocument, query) {
			// 如果有投影，应用投影
			document := event.FullDocument
			if projection != nil && len(projection) > 0 {
				document = c.applyProjection(document, projection)
			}

			// 更新查询缓存
			c.updateQueryCache(key, query, event.OperationType, event.DocumentKey.ID, document)
		}
	}

	if err := stream.Err(); err != nil && err != context.Canceled {
		fmt.Printf("变更流错误: %v\n", err)
	}
}

// 新增：应用投影到文档
func (c *MongoCache) applyProjection(doc bson.M, projection bson.M) bson.M {
	result := bson.M{}

	// 始终保留 _id 字段，除非明确排除
	if idExcluded, ok := projection["_id"]; !ok || idExcluded != 0 {
		if id, ok := doc["_id"]; ok {
			result["_id"] = id
		}
	}

	// 应用投影
	for field, include := range projection {
		if field == "_id" {
			continue // 已经处理过了
		}

		if include != 0 {
			if value, ok := doc[field]; ok {
				result[field] = value
			}
		}
	}

	return result
}

// 新增：更新查询缓存
func (c *MongoCache) updateQueryCache(key string, query bson.M, operationType string, docID primitive.ObjectID, document bson.M) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	queryKey := c.generateQueryKey(key, query)
	cacheEntry, exists := c.queryCache[queryKey]

	if !exists {
		// 如果缓存不存在，对于插入操作创建新缓存
		if operationType == "insert" {
			c.queryCache[queryKey] = map[string]interface{}{
				"data":     []bson.M{document},
				"expireAt": time.Now().Add(c.config.QueryTTL),
			}
		}
		return
	}

	// 获取缓存的数据
	entry, ok := cacheEntry.(map[string]interface{})
	if !ok {
		return
	}

	data, ok := entry["data"].([]bson.M)
	if !ok {
		return
	}

	// 根据操作类型更新缓存
	switch operationType {
	case "insert":
		// 添加新文档到结果集
		entry["data"] = append(data, document)

	case "update", "replace":
		// 更新现有文档
		found := false
		for i, doc := range data {
			if docID, ok := doc["_id"].(primitive.ObjectID); ok && docID == docID {
				data[i] = document
				found = true
				break
			}
		}
		if !found {
			// 如果文档不在结果集中，但现在符合查询条件，添加它
			entry["data"] = append(data, document)
		}

	case "delete":
		// 从结果集中删除文档
		newData := make([]bson.M, 0, len(data))
		for _, doc := range data {
			if id, ok := doc["_id"].(primitive.ObjectID); !ok || id != docID {
				newData = append(newData, doc)
			}
		}
		entry["data"] = newData
	}

	// 更新过期时间
	entry["expireAt"] = time.Now().Add(c.config.QueryTTL)
}
