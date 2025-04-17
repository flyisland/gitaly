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
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"go.etcd.io/raft/v3/raftpb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockReplica struct {
	RaftReplica
	logManager storage.LogManager
	config     config.Raft
}

// EntryPath returns an absolute path to a given log entry's WAL files.
func (m *mockReplica) GetEntryPath(lsn storage.LSN) string {
	return m.logManager.GetEntryPath(lsn)
}

// Step is a mock implementation of the raft.Node.Step method.
func (m *mockReplica) Step(ctx context.Context, msg raftpb.Message) error {
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
	t.Cleanup(dbMgr.Close)
	return dbMgr
}

func getTestDBManager(t *testing.T, ctx context.Context, cfg config.Cfg, logger logger.Logger) keyvalue.Transactioner {
	t.Helper()

	dbMgr := openTestDB(t, ctx, cfg, logger)
	db, err := dbMgr.GetDB(cfg.Storages[0].Name)
	require.NoError(t, err)

	return db
}

func createRaftReplica(t *testing.T, ctx context.Context, raftCfg config.Raft, partitionID storage.PartitionID, metrics *Metrics, opts ...OptionFunc) (*Replica, error) {
	logger := testhelper.NewLogger(t)
	cfg := testcfg.Build(t)

	storageName := cfg.Storages[0].Name
	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	posTracker := log.NewPositionTracker()

	dbMgr := openTestDB(t, ctx, cfg, logger)

	db, err := dbMgr.GetDB(storageName)
	require.NoError(t, err)

	logStore, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, metrics)
	require.NoError(t, err)

	raftNode, err := NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, opts...)

	manager, err := raftFactory(storageName, partitionID, logStore, logger, metrics)

	return manager, err
}
