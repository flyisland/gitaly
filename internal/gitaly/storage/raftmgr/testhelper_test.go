package raftmgr

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	logger "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRaftManager struct {
	logger    logger.LogrusLogger
	transport Transport
	wal       storage.LogManager
}

// EntryPath returns an absolute path to a given log entry's WAL files.
func (m *mockRaftManager) GetEntryPath(lsn storage.LSN) string {
	return m.wal.GetEntryPath(lsn)
}

// Step is a mock implementation of the raft.Node.Step method.
func (m *mockRaftManager) Step(ctx context.Context, msg raftpb.Message) error {
	return nil
}
