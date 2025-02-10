package raftmgr

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// RaftManager is an interface that defines the methods to orchestrate the Raft consensus protocol.
type RaftManager interface {
	GetEntryPath(storage.LSN) string
	GetLogReader() storage.LogReader
	Step(ctx context.Context, msg raftpb.Message) error
}
