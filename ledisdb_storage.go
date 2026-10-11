package ledisdb_storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-infrastructure/go-iterator"
	"github.com/redis/go-redis/v9"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// LedisDB 兼容 Redis 协议，支持 Lua 脚本原子执行。
// 以下三段 Lua 脚本走原子 CAS 路线，Hash 结构 {version, json}。
// 命令按真实 LedisDB 服务端的能力挑选，而不是按 Redis 的：
//   - EXISTS 在 LedisDB 上对已存在的 key 也返回 0，存在性判断改用 HEXISTS；
//   - Lua 里的 HSET 只接受单个 field/value，多字段写入用 HMSET；
//   - PUBLISH 与 SCAN 都不存在，Watch 由调用方降级为轮询，遍历用 XSCAN。
const (
	// createScript 原子创建：version 字段不存在时写入，返回 1；已存在返回 0
	createScript = `
if redis.call('HEXISTS', KEYS[1], 'version') == 0 then
    redis.call('HMSET', KEYS[1], 'version', ARGV[1], 'json', ARGV[2])
    return 1
else
    return 0
end
`

	// updateScript 原子更新：当前 version 匹配时写入新 version 和 json，返回 1；不匹配返回 0
	updateScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('HMSET', KEYS[1], 'version', ARGV[2], 'json', ARGV[3])
    return 1
else
    return 0
end
`

	// deleteScript 原子删除：当前 version 匹配时清空 hash，返回 1；不匹配返回 0。
	// LedisDB 的 DEL 对 HASH 类型不生效（返回 1 但数据还在），删除字段必须用 HDEL。
	deleteScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('HDEL', KEYS[1], 'version', 'json')
    return 1
else
    return 0
end
`
)

// LedisDBStorage 基于 LedisDB 的存储引擎实现
// LedisDB 兼容 Redis 协议：Lua CAS + SCAN List + Pub/Sub Watch。
// 与 redis-storage 的差异：LedisDB 不支持 TIME 命令，GetTime 降级为本地时间，
// 故 Capabilities 不声明 CapabilityReliableTime；keyspace notification 可能不支持，
// Watch 首选 publish 通道，失败降级为轮询。
type LedisDBStorage struct {
	options *LedisDBStorageOptions
	client  *redis.Client
}

var _ storage.Storage = &LedisDBStorage{}

// 命令结果是具体类型且内部状态不可构造。默认函数执行真实命令，测试替换后覆盖分支。
var (
	ledisEval = func(client *redis.Client, ctx context.Context, script string, keys []string, args ...any) (int64, error) {
		return client.Eval(ctx, script, keys, args...).Int64()
	}
	ledisHGet = func(client *redis.Client, ctx context.Context, key, field string) (string, error) {
		return client.HGet(ctx, key, field).Result()
	}
	// ledisScan 用 LedisDB 的 XSCAN 遍历 HASH key。LedisDB 没有 SCAN。
	// XSCAN 的游标是上一页最后一个 key 的字符串，而不是数字；数字游标只作为
	// "是否还有下一页"的信号（0 表示结束），真实游标由闭包持有，所以并发的 List 不会串页。
	ledisScan    = newLedisScanner()
	ledisHGetAll = func(client *redis.Client, ctx context.Context, key string) (map[string]string, error) {
		return client.HGetAll(ctx, key).Result()
	}
)

func NewLedisDBStorage(ctx context.Context, options *LedisDBStorageOptions) (*LedisDBStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &LedisDBStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const StorageName = "ledisdb-storage"

func (x *LedisDBStorage) GetName() string { return StorageName }

// Capabilities 声明 LedisDB 存储支持的能力
// Lua CAS + 轮询 Watch（LedisDB 没有 PUBLISH/SUBSCRIBE）；无服务端时间（TIME 不支持），
// ReliableTime 由 GetTime 返回的本地时间满足，对齐 S3/memcache/KV/HTTP 的做法。
func (x *LedisDBStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *LedisDBStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	if err := ledisPing(client, ctx); err != nil {
		return err
	}
	x.client = client
	return nil
}

// lockKey 返回存储锁信息的 Hash key
func (x *LedisDBStorage) lockKey(lockId string) string {
	return fmt.Sprintf("%s:lock:%s", x.options.KeyPrefix, lockId)
}

// CreateWithVersion 原子创建锁记录
// Lua 脚本保证"检查 key 不存在 + 写入"是原子操作
func (x *LedisDBStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := ledisEval(x.client, ctx, createScript, []string{key},
		strconv.FormatUint(uint64(version), 10),
		lockInformation.ToJsonString(),
	)
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// 记录不存在与版本不符统一返回 ErrVersionMiss（可重试），对齐 memory/etcd 已修口径。
func (x *LedisDBStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := ledisEval(x.client, ctx, updateScript, []string{key},
		strconv.FormatUint(uint64(expectedVersion), 10),
		strconv.FormatUint(uint64(newVersion), 10),
		lockInformation.ToJsonString(),
	)
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
func (x *LedisDBStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := ledisEval(x.client, ctx, deleteScript, []string{key},
		strconv.FormatUint(uint64(expectedVersion), 10),
	)
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// Get 获取锁信息
func (x *LedisDBStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.lockKey(lockId)
	result, err := ledisHGet(x.client, ctx, key, "json")
	if err != nil {
		if err == redis.Nil {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	return result, nil
}

// GetTime LedisDB 不支持 TIME 命令，降级为本地时间。
func (x *LedisDBStorage) GetTime(ctx context.Context) (time.Time, error) { return time.Now(), nil }
func (x *LedisDBStorage) Close(ctx context.Context) error                { return nil }

// List 列出当前所有的锁信息
// 使用 SCAN 匹配 <KeyPrefix>:lock:* 模式 + HGETALL 收集（非原子快照，仅运维查看）。
func (x *LedisDBStorage) List(ctx context.Context) (iterator.Iterator[*storage.LockInformation], error) {
	// XSCAN 的前缀匹配不会消费 '*'，所以这里传不带通配符的前缀。
	pattern := x.options.KeyPrefix + ":lock:"
	var keys []string
	var cursor uint64
	for {
		var batch []string
		var err error
		batch, cursor, err = ledisScan(x.client, ctx, cursor, pattern, 100)
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if cursor == 0 {
			break
		}
	}

	slice := make([]*storage.LockInformation, 0, len(keys))
	for _, key := range keys {
		vals, err := ledisHGetAll(x.client, ctx, key)
		if err != nil {
			continue
		}
		jsonStr, ok := vals["json"]
		if !ok {
			continue
		}
		info := &storage.LockInformation{}
		if err := json.Unmarshal([]byte(jsonStr), info); err != nil {
			continue
		}
		slice = append(slice, info)
	}
	return iterator.FromSlice(slice), nil
}

// Watch 监听指定锁的状态变更。
// LedisDB 没有 PUBLISH/SUBSCRIBE，通知通道不可用，直接轮询。
func (x *LedisDBStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

// newLedisScanner 返回一个带独立游标的扫描函数。
// XSCAN HASH <prefix> count <n> 的回复是 [下一页游标, [key...]]，游标为空字符串表示结束。
// prefix 是前缀匹配，不是带 * 的通配符。
func newLedisScanner() func(client *redis.Client, ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	lastKey := ""
	return func(client *redis.Client, ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
		if cursor == 0 {
			lastKey = ""
		}
		reply, err := client.Do(ctx, "XSCAN", "HASH", pattern, "count", count).Result()
		if err != nil {
			return nil, 0, err
		}
		parts, ok := reply.([]any)
		if !ok || len(parts) != 2 {
			return nil, 0, fmt.Errorf("ledisdb: unexpected XSCAN reply %T", reply)
		}
		next, _ := parts[0].(string)
		rawKeys, _ := parts[1].([]any)
		keys := make([]string, 0, len(rawKeys))
		for _, raw := range rawKeys {
			key, ok := raw.(string)
			if !ok {
				continue
			}
			// XSCAN 的游标本身就是上一页的最后一个 key，下一页会把它再返回一次，跳过它。
			if key == lastKey {
				continue
			}
			keys = append(keys, key)
		}
		lastKey = next
		if next == "" {
			return keys, 0, nil
		}
		return keys, cursor + 1, nil
	}
}
