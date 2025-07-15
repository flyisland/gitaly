package raftmgr

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
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

// TestReplicaConfig holds configuration for creating test replicas
type TestReplicaConfig struct {
	MemberID    uint64
	PartitionID storage.PartitionID
	Address     string // Optional: if provided, creates a replica with server
	Options     []OptionFunc
}

func createRaftReplica(t *testing.T, ctx context.Context, memberID uint64, address string, raftCfg config.Raft, partitionID storage.PartitionID, metrics *Metrics, opts ...OptionFunc) (*Replica, error) {
	config := TestReplicaConfig{
		MemberID:    memberID,
		PartitionID: partitionID,
		Address:     address,
		Options:     opts,
	}

	return createRaftReplicaWithConfig(t, ctx, raftCfg, config, metrics)
}

func createRaftReplicaWithConfig(t *testing.T, ctx context.Context, raftCfg config.Raft, config TestReplicaConfig, metrics *Metrics) (*Replica, error) {
	cfg := testcfg.Build(t)

	logger := testhelper.NewLogger(t)
	dbMgr := openTestDB(t, ctx, cfg, logger)
	storageName := cfg.Storages[0].Name
	db, err := dbMgr.GetDB(storageName)
	require.NoError(t, err)

	if config.Address != "" {
		cfg.SocketPath = config.Address
	}

	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	posTracker := log.NewPositionTracker()

	logStore, err := NewReplicaLogStore(storageName, config.PartitionID, raftCfg, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, metrics)
	if err != nil {
		return nil, err
	}

	conns := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	t.Cleanup(func() {
		err := conns.Close()
		require.NoError(t, err)
	})

	raftNode, err := NewNode(cfg, logger, dbMgr, conns)
	if err != nil {
		return nil, err
	}

	raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, config.Options...)
	return raftFactory(config.MemberID, storageName, NewPartitionKey(storageName, config.PartitionID), logStore, logger, metrics)
}

func createTempServer(t *testing.T, transport *GrpcTransport) (string, *grpc.Server) {
	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv := grpc.NewServer()

	destinationRaftServer := &mockRaftServer{
		node: mockStorageNode{
			transport: transport,
		},
	}

	gitalypb.RegisterRaftServiceServer(srv, destinationRaftServer)

	go testhelper.MustServe(t, srv, listener)

	return socketPath, srv
}
