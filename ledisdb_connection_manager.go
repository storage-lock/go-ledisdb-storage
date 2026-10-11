package ledisdb_storage

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/storage-lock/go-storage"
)

// LedisDBConnectionManager 负责维护与 LedisDB 的连接。
// LedisDB 兼容 Redis 协议，直接复用 go-redis 客户端。
// 对齐 go-redis-storage 的 RedisConnectionManager：mutex + initialized 标志，
// 连接失败不缓存 err，下次 Take 可重试。
type LedisDBConnectionManager struct {

	// 连接到 LedisDB 的地址
	URI string

	// 客户端选项（与 URI 互斥，优先使用 Options）
	Options *redis.Options

	initMu      sync.Mutex
	initialized bool
	client      *redis.Client
}

var _ storage.ConnectionManager[*redis.Client] = &LedisDBConnectionManager{}

// 建连和关闭默认走真实客户端。测试替换它们覆盖失败、重试和关闭分支。
var (
	ledisNewClient = func(options *redis.Options) *redis.Client {
		return redis.NewClient(options)
	}
	ledisParseURL = func(uri string) (*redis.Options, error) {
		return redis.ParseURL(uri)
	}
	ledisPing = func(client *redis.Client, ctx context.Context) error {
		return client.Ping(ctx).Err()
	}
	ledisCloseClient = func(client *redis.Client) error {
		return client.Close()
	}
)

// NewLedisDBConnectionManagerFromURI 从 URI 创建连接管理器
func NewLedisDBConnectionManagerFromURI(uri string) *LedisDBConnectionManager {
	return &LedisDBConnectionManager{
		URI: uri,
	}
}

// NewLedisDBConnectionManagerFromOptions 从选项创建连接管理器
func NewLedisDBConnectionManagerFromOptions(options *redis.Options) *LedisDBConnectionManager {
	return &LedisDBConnectionManager{
		Options: options,
	}
}

// NewLedisDBConnectionManagerFromClient 复用已经存在的客户端
func NewLedisDBConnectionManagerFromClient(client *redis.Client) *LedisDBConnectionManager {
	return &LedisDBConnectionManager{
		client:      client,
		initialized: true,
	}
}

const LedisDBConnectionManagerName = "ledisdb-connection-manager"

func (x *LedisDBConnectionManager) Name() string {
	return LedisDBConnectionManagerName
}

// Take 获取客户端。成功后缓存不再重复 Connect；失败不缓存 err，可重试。
func (x *LedisDBConnectionManager) Take(ctx context.Context) (*redis.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.client, nil
	}
	var client *redis.Client
	if x.Options != nil {
		client = ledisNewClient(x.Options)
	} else {
		opt, err := ledisParseURL(x.URI)
		if err != nil {
			return nil, err
		}
		client = ledisNewClient(opt)
	}
	if err := ledisPing(client, ctx); err != nil {
		_ = ledisCloseClient(client)
		return nil, err
	}
	x.client = client
	x.initialized = true
	return x.client, nil
}

// Return 归还连接。单 client 复用模式为 no-op（对齐 redis/mongo 连接管理器）。
func (x *LedisDBConnectionManager) Return(ctx context.Context, client *redis.Client) error {
	return nil
}

// Shutdown 关闭底层客户端。
func (x *LedisDBConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.client != nil {
		err := ledisCloseClient(x.client)
		x.client = nil
		x.initialized = false
		return err
	}
	return nil
}
