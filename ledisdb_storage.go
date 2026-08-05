package ledisdb_storage

import (
	"context"
	"sync"
	"time"

	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// LedisDBStorage ledisdb存储实现
type LedisDBStorage struct {
	mu sync.RWMutex
}

var _ storage.Storage = &LedisDBStorage{}

func NewLedisDBStorage(ctx context.Context) (*LedisDBStorage, error) {
	return &LedisDBStorage{}, nil
}

const StorageName = "ledisdb-storage"

func (x *LedisDBStorage) GetName() string { return StorageName }
func (x *LedisDBStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime}
}
func (x *LedisDBStorage) Init(ctx context.Context) error { return nil }
func (x *LedisDBStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}
func (x *LedisDBStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}
func (x *LedisDBStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}
func (x *LedisDBStorage) Get(ctx context.Context, lockId string) (string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return "", storage_lock.ErrLockNotFound
}
func (x *LedisDBStorage) GetTime(ctx context.Context) (time.Time, error) { return time.Now(), nil }
func (x *LedisDBStorage) Close(ctx context.Context) error { return nil }
func (x *LedisDBStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	return go_iterator.FromSlice([]*storage.LockInformation{}), nil
}
