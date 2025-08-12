package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRaftReplica struct {
	raftmgr.RaftReplica
}

// Step is a mock implementation of the raft.Node.Step method.
func (m *mockRaftReplica) Step(ctx context.Context, msg raftpb.Message) error {
	return nil
}

func (m *mockRaftReplica) IsStarted() bool {
	return true
}

func runRaftServer(t *testing.T, ctx context.Context, cfg config.Cfg, node *raftmgr.Node) gitalypb.RaftServiceClient {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		deps.Cfg = cfg
		deps.Node = node
		gitalypb.RegisterRaftServiceServer(srv, NewServer(deps))
	}, testserver.WithDisablePraefect())

	cfg.SocketPath = serverSocketPath

	conn := gittest.DialService(t, ctx, cfg)

	return gitalypb.NewRaftServiceClient(conn)
}

func raftConfigsForTest(t *testing.T) config.Raft {
	// Speed up initial election overhead in the test setup
	return config.Raft{
		Enabled:                   true,
		ClusterID:                 "test-cluster",
		ElectionTicks:             5,
		HeartbeatTicks:            2,
		RTTMilliseconds:           100,
		ProposalConfChangeTimeout: 1500,
		SnapshotDir:               testhelper.TempDir(t),
	}
}
// createRaftNodeWithStorage creates a Raft enabled Gitaly node with a base storage.
func createRaftNodeWithStorage(t *testing.T, storageName string) (*raftmgr.Node, config.Cfg, error) {
	t.Helper()
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)

	cfg := testcfg.Build(t, testcfg.WithStorages(storageName))
	cfg.Raft = raftConfigsForTest(t)

	dbMgr := setupDB(t, ctx, logger, cfg)
	t.Cleanup(dbMgr.Close)

	conns := client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	t.Cleanup(func() {
		err := conns.Close()
		require.NoError(t, err)
	})

	raftNode, err := raftmgr.NewNode(cfg, logger, dbMgr, conns)
	require.NoError(t, err)

	metrics := storagemgr.NewMetrics(cfg.Prometheus)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	partitionFactoryOptions := []partition.FactoryOption{
		partition.WithRaftConfig(cfg.Raft),
		partition.WithRaftFactory(raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)),
		partition.WithCmdFactory(gitCmdFactory),
		partition.WithRepoFactory(localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache)),
		partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
	}
	nodeMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(logger, dbMgr, partition.NewFactory(partitionFactoryOptions...), 2, metrics))
	require.NoError(t, err)
	t.Cleanup(nodeMgr.Close)

	// Setup the base storage for the node two to support running transactions.
	for _, storageCfg := range cfg.Storages {
		baseStorage, err := nodeMgr.GetStorage(storageCfg.Name)
		require.NoError(t, err)
		require.NoError(t, raftNode.SetBaseStorage(storageCfg.Name, baseStorage))
	}

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		deps.Cfg = cfg
		deps.Node = raftNode
		gitalypb.RegisterRaftServiceServer(srv, NewServer(deps))
	}, testserver.WithDisablePraefect())

	return raftNode, cfg, nil
}
