package raftmgr

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// MockConsumer is a mock implementation of the LogConsumer interface.
type MockConsumer struct {
	notifications []mockNotification
	mutex         sync.Mutex
}

type mockNotification struct {
	storageName   string
	partitionID   storage.PartitionID
	lowWaterMark  storage.LSN
	highWaterMark storage.LSN
}

// NotifyNewEntries is a mock implementation of the LogConsumer.NotifyNewEntries method.
func (mc *MockConsumer) NotifyNewEntries(storageName string, partitionID storage.PartitionID, lowWaterMark, committedLSN storage.LSN) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.notifications = append(mc.notifications, mockNotification{
		storageName:   storageName,
		partitionID:   partitionID,
		lowWaterMark:  lowWaterMark,
		highWaterMark: committedLSN,
	})
}

// GetNotifications is a mock implementation of the LogConsumer.GetNotifications method.
func (mc *MockConsumer) GetNotifications() []mockNotification {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	result := make([]mockNotification, len(mc.notifications))
	copy(result, mc.notifications)
	return result
}
