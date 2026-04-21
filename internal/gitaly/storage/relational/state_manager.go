package relational

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ObjectPoolStateManager updates the object pool metadata state
type ObjectPoolStateManager interface {
	// NotifyCreatePool records a new object pool in the database
	NotifyCreatePool(ctx context.Context, poolDiskPath, storageName, upstreamPath string) error
	// NotifyDeletePool removes an object pool and its members from the database
	NotifyDeletePool(ctx context.Context, poolDiskPath string) error
	// NotifyLinkRepository adds a repository as a member of an object pool
	NotifyLinkRepository(ctx context.Context, poolDiskPath, memberDiskPath string) error
	// NotifyUnlinkRepository removes a repository from an object pool's members
	NotifyUnlinkRepository(ctx context.Context, poolDiskPath, memberDiskPath string) error
}

type objectPoolStateManager struct {
	mu        sync.Mutex
	poolStore PoolStore
}

// NewObjectPoolStateManager creates a new ObjectPoolStateManager.
// If poolStore is nil, all operations do nothing.
func NewObjectPoolStateManager(poolStore PoolStore) ObjectPoolStateManager {
	return &objectPoolStateManager{
		poolStore: poolStore,
	}
}

func (m *objectPoolStateManager) NotifyCreatePool(ctx context.Context, poolDiskPath, storageName, upstreamPath string) error {
	if m.poolStore == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.poolStore.CreatePool(ctx, poolDiskPath, storageName, upstreamPath, time.Now())
}

func (m *objectPoolStateManager) NotifyDeletePool(ctx context.Context, poolDiskPath string) error {
	if m.poolStore == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.poolStore.DeletePoolMembers(ctx, poolDiskPath); err != nil {
		return fmt.Errorf("delete pool members: %w", err)
	}

	return m.poolStore.DeletePool(ctx, poolDiskPath)
}

func (m *objectPoolStateManager) NotifyLinkRepository(ctx context.Context, poolDiskPath, memberDiskPath string) error {
	if m.poolStore == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.poolStore.AddMember(ctx, poolDiskPath, memberDiskPath)
}

func (m *objectPoolStateManager) NotifyUnlinkRepository(ctx context.Context, poolDiskPath, memberDiskPath string) error {
	if m.poolStore == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.poolStore.RemoveMember(ctx, poolDiskPath, memberDiskPath)
}
