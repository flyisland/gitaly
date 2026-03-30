package relational

import (
	"context"
	"time"
)

// PoolMetadata represents the metadata for an object pool.
type PoolMetadata struct {
	DiskPath    string
	StorageNode string
	Members     []string
	Upstream    string
	UpdatedAt   time.Time
}

// PoolStore provides storage for object pool metadata.
type PoolStore interface {
	StorePoolData(ctx context.Context, storageName string, poolsByDiskPath map[string]*PoolMetadata) error
	GetPoolByDiskPath(ctx context.Context, poolDiskPath string) (*PoolMetadata, error)
	ListPools(ctx context.Context) ([]*PoolMetadata, error)
	ForEachPoolByStorage(ctx context.Context, storageName string, fn func(*PoolMetadata) error) error

	ListPoolMembers(ctx context.Context, poolDiskPath string) ([]string, error)
	GetPoolForMember(ctx context.Context, memberDiskPath string) (string, error)

	DeletePool(ctx context.Context, poolDiskPath string) error
	AddMember(ctx context.Context, poolDiskPath, memberDiskPath string) error
	RemoveMember(ctx context.Context, poolDiskPath, memberDiskPath string) error

	Close() error
}
