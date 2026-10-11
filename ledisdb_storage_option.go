package ledisdb_storage

import (
	"github.com/storage-lock/go-storage"
	"github.com/redis/go-redis/v9"
)

// LedisDBStorageOptions LedisDB 的存储选项
type LedisDBStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*redis.Client]

	// KeyPrefix key 的前缀，用于隔离不同的锁实例
	// 实际的 key 格式为：<KeyPrefix>:lock:<lockId>
	KeyPrefix string
}

func NewLedisDBStorageOptions() *LedisDBStorageOptions {
	return &LedisDBStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewLedisDBStorageOptionsWithURI(uri string) *LedisDBStorageOptions {
	return NewLedisDBStorageOptions().SetConnectionManager(NewLedisDBConnectionManagerFromURI(uri))
}

func (x *LedisDBStorageOptions) SetKeyPrefix(keyPrefix string) *LedisDBStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *LedisDBStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*redis.Client]) *LedisDBStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *LedisDBStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
