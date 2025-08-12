package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"go.etcd.io/raft/v3"
)

const (
	storageOne = "storage-one"
	storageTwo = "storage-two"
)

var timeout = 5 * time.Second

func TestRaftReplicaCreation(t *testing.T) {
	t.Parallel()
	ctxOne := testhelper.Context(t)
	replicaOne, partitionKey, err := createRaftReplica(t, ctxOne, 1)
	require.NoError(t, err)
	require.NoError(t, replicaOne.Initialize(ctxOne, 0))
	t.Cleanup(func() {
		err := replicaOne.Close()
		require.NoError(t, err)
	})
	// Wait for the replica to elect itself as leader
	require.Eventually(t, func() bool {
		state := replicaOne.GetCurrentState()
		return state.State == raft.StateLeader
	}, 10*time.Second, 5*time.Millisecond, "replica should become leader")

	nodeTwo, cfg, err := createRaftNodeWithStorage(t, storageTwo)
	require.NoError(t, err)

	err = replicaOne.AddNode(ctxOne, cfg.SocketPath, storageTwo)
	require.NoError(t, err)

	storageHandle, err := nodeTwo.GetStorage(storageTwo)
	require.NoError(t, err)
	raftEnabledStorage := storageHandle.(*raftmgr.RaftEnabledStorage)
	require.NotNil(t, raftEnabledStorage, "storage should be a RaftEnabledStorage")

	replicaRegistry := raftEnabledStorage.GetReplicaRegistry()
	require.NotNil(t, replicaRegistry, "replica registry should not be nil")

	var replicaTwo raftmgr.RaftReplica
	require.Eventually(t, func() bool {
		replicaTwo, err = replicaRegistry.GetReplica(partitionKey)
		if err != nil {
			return false
		}
		return replicaTwo != nil
	}, timeout, 5*time.Millisecond, "replica should be created")

	require.Eventually(t, func() bool {
		leaderID, err := replicaTwo.(*raftmgr.Replica).GetLeaderID()
		if err != nil {
			return false
		}
		memberID, err := replicaOne.GetMemberID()
		if err != nil {
			return false
		}
		return leaderID == memberID
	}, timeout, 5*time.Millisecond, "replica two should have the same leader as replica one")
}
