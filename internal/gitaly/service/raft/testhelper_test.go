package raft

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRaftManager struct {
	raftmgr.RaftManager
}

// Step is a mock implementation of the raft.Node.Step method.
func (m *mockRaftManager) Step(ctx context.Context, msg raftpb.Message) error {
	return nil
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
