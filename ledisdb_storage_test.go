package ledisdb_storage

import (
	"context"
	"os"
	"testing"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

// TestLedisDBStorage 真实 CAS 实现的测试，需 LedisDB 服务端（兼容 Redis 协议）。
// 无 STORAGE_LOCK_LEDISDB_URI 则跳过；有则跑完整存储契约套件。
func TestLedisDBStorage(t *testing.T) {
	uri := os.Getenv("STORAGE_LOCK_LEDISDB_URI")
	if uri == "" {
		t.Skip("STORAGE_LOCK_LEDISDB_URI not set, skipping LedisDB test")
	}

	ctx := context.Background()
	s, err := NewLedisDBStorage(ctx, NewLedisDBStorageOptionsWithURI(uri))
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}

// TestLedisDBStorage_Contract 本地契约测试：不依赖服务端，验证 Capabilities 与错误语义。
// 用一个指向不可达地址的实例验证构造期失败行为。
func TestLedisDBStorage_Contract(t *testing.T) {
	// 能力声明：CAS + ReliableTime（本地时间降级满足，对齐 S3/memcache/KV/HTTP）+ AtomicDelete + Watch
	s := &LedisDBStorage{options: NewLedisDBStorageOptions()}
	caps := s.Capabilities()
	assert.Contains(t, caps, storage.CapabilityCAS)
	assert.Contains(t, caps, storage.CapabilityReliableTime)
	assert.Contains(t, caps, storage.CapabilityAtomicDelete)
	assert.Contains(t, caps, storage.CapabilityWatch)

	assert.Equal(t, StorageName, s.GetName())

	// 无 ConnectionManager 时构造失败（Check 守卫）
	_, err := NewLedisDBStorage(context.Background(), NewLedisDBStorageOptions())
	assert.ErrorIs(t, err, ErrConnectionManagerNil)

	// 空 KeyPrefix 时构造失败
	opt := NewLedisDBStorageOptionsWithURI("redis://127.0.0.1:6380").SetKeyPrefix("")
	_, err = NewLedisDBStorage(context.Background(), opt)
	assert.ErrorIs(t, err, ErrKeyPrefixEmpty)

	_ = storage_lock.ErrVersionMiss
}
