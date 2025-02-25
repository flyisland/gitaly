package raftmgr

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// RaftManager is an interface that defines the methods to orchestrate the Raft consensus protocol.
type RaftManager interface {
	GetEntryPath(storage.LSN) string
	GetLogReader() storage.LogReader
	Step(ctx context.Context, msg raftpb.Message) error
}

// ErrObsoleted is returned when an event associated with a LSN is shadowed by another one with higher term. That event
// must be unlocked and removed from the registry.
var ErrObsoleted = fmt.Errorf("event is obsolete, superseded by a recent log entry with higher term")
