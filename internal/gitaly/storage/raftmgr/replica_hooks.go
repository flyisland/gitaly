package raftmgr

import "go.etcd.io/raft/v3/raftpb"

// replicaHooks defines insertion points for testing various stages of a replica's operations.
type replicaHooks struct {
	// BeforeInsertLogEntry is called before inserting a log entry at the specified index.
	BeforeInsertLogEntry func(index uint64)

	// BeforeSaveHardState is called before persisting a new hard state.
	BeforeSaveHardState func()

	// BeforePropose is called before proposing a new log entry with the given path.
	BeforePropose func(path string)

	// BeforeProcessCommittedEntries is called before processing committed entries from a Ready state.
	BeforeProcessCommittedEntries func([]raftpb.Entry)

	// BeforeAdvance is called before advancing the replica state.
	BeforeAdvance func()

	// BeforeSendMessages is called before sending messages to other replicas.
	BeforeSendMessages func()

	// BeforeHandleReady is called before processing a new Ready state of a replica.
	BeforeHandleReady func()
}

// noopHooks returns a Hooks instance with all hooks set to no-op functions.
func noopHooks() replicaHooks {
	return replicaHooks{
		BeforeInsertLogEntry:          func(uint64) {},
		BeforeSaveHardState:           func() {},
		BeforePropose:                 func(string) {},
		BeforeProcessCommittedEntries: func([]raftpb.Entry) {},
		BeforeAdvance:                 func() {},
		BeforeSendMessages:            func() {},
		BeforeHandleReady:             func() {},
	}
}
