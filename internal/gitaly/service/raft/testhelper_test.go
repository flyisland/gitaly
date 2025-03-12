package raft

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
