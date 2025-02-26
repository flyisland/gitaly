package raftmgr

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRaftManager struct {
	RaftManager
	logger     logger.LogrusLogger
	transport  Transport
	logManager storage.LogManager
}

// EntryPath returns an absolute path to a given log entry's WAL files.
func (m *mockRaftManager) GetEntryPath(lsn storage.LSN) string {
	return m.logManager.GetEntryPath(lsn)
}

func (m *mockRaftManager) GetLogReader() storage.LogReader {
	return m.logManager
}

// Step is a mock implementation of the raft.Node.Step method.
func (m *mockRaftManager) Step(ctx context.Context, msg raftpb.Message) error {
	return nil
}

type mockConsumer struct {
	notifications []mockNotification
	mutex         sync.Mutex
}

type mockNotification struct {
	storageName   string
	partitionID   storage.PartitionID
	lowWaterMark  storage.LSN
	highWaterMark storage.LSN
}

func (mc *mockConsumer) NotifyNewEntries(storageName string, partitionID storage.PartitionID, lowWaterMark, committedLSN storage.LSN) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.notifications = append(mc.notifications, mockNotification{
		storageName:   storageName,
		partitionID:   partitionID,
		lowWaterMark:  lowWaterMark,
		highWaterMark: committedLSN,
	})
}

func (mc *mockConsumer) GetNotifications() []mockNotification {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	return mc.notifications
}

func openTestDB(t *testing.T, ctx context.Context, cfg config.Cfg, logger logger.Logger) *databasemgr.DBManager {
	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	return dbMgr
}

func getTestDBManager(t *testing.T, ctx context.Context, cfg config.Cfg, logger logger.Logger) keyvalue.Transactioner {
	t.Helper()

	dbMgr := openTestDB(t, ctx, cfg, logger)
	t.Cleanup(dbMgr.Close)

	db, err := dbMgr.GetDB(cfg.Storages[0].Name)
	require.NoError(t, err)

	return db
}
