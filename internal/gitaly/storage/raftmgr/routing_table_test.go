package raftmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPersistentRoutingTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, kvStore.Close())
	}()

	rt := NewKVRoutingTable(kvStore)

	t.Run("add and translate member", func(t *testing.T) {
		memberID := 1
		address := "localhost:1234"
		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		entry := RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     uint64(memberID),
					StorageName:  "test-storage",
					Metadata: &gitalypb.ReplicaID_Metadata{
						Address: address,
					},
				},
			},
			Term:  1,
			Index: 1,
		}

		err := rt.UpsertEntry(entry)
		require.NoError(t, err)

		replica, err := rt.Translate(partitionKey, uint64(memberID))
		require.NoError(t, err)
		require.Equal(t, address, replica.GetMetadata().GetAddress())
		require.Equal(t, "test-storage", replica.GetStorageName())
	})

	t.Run("stale entry rejected", func(t *testing.T) {
		key := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   2,
		}

		entry1 := RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: key,
					MemberId:     1,
					Metadata: &gitalypb.ReplicaID_Metadata{
						Address: "addr1",
					},
				},
			},
			Term:  2,
			Index: 3,
		}

		require.NoError(t, rt.UpsertEntry(entry1))

		entry2 := entry1
		entry2.Term = 1 // Lower term
		err := rt.UpsertEntry(entry2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "stale entry")
	})

	t.Run("node not found", func(t *testing.T) {
		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   3,
		}

		memberID := 999 // Non-existent node

		_, err := rt.Translate(partitionKey, uint64(memberID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Key not found")
	})
}

func TestApplyConfEntry(t *testing.T) {
	t.Parallel()

	createMetadata := func(address string) *gitalypb.ReplicaID_Metadata {
		return &gitalypb.ReplicaID_Metadata{
			Address: address,
		}
	}

	serializeMetadata := func(metadata *gitalypb.ReplicaID_Metadata) []byte {
		data, err := proto.Marshal(metadata)
		require.NoError(t, err)
		return data
	}

	t.Run("add node with confChange v1", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		confChange := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  1,
			Context: serializeMetadata(createMetadata("localhost:1234")),
		}

		err = rt.ApplyConfChange(1, 1, 1, partitionKey, confChange)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.Replicas[0].GetMemberId())
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, "localhost:1234", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("remove node with confChange v1", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		initialEntry := &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:1234"),
				},
				{
					PartitionKey: partitionKey,
					MemberId:     2,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:5678"),
				},
			},
			Term:  1,
			Index: 1,
		}

		// Initialize with a replica
		err = rt.UpsertEntry(*initialEntry)
		require.NoError(t, err)

		confChange := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 2,
		}

		entry := initialEntry
		entry.Term = 2
		entry.Index = 2

		err = rt.ApplyConfChange(entry.Term, entry.Index, 1, partitionKey, confChange)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, uint64(1), updatedEntry.Replicas[0].GetMemberId())
		require.Equal(t, "localhost:1234", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("if nodeID is zero, it should not be added to the routing table", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		confChange := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  0,
			Context: serializeMetadata(createMetadata("localhost:1234")),
		}

		err = rt.ApplyConfChange(1, 1, 0, partitionKey, confChange)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nodeID should be non-zero")
	})

	t.Run("fails if the last remaining node is removed", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		entry := &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:1234"),
				},
			},
			Term:  1,
			Index: 1,
		}

		err = rt.UpsertEntry(*entry)
		require.NoError(t, err)

		confChange := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 1,
		}

		err = rt.ApplyConfChange(entry.Term, entry.Index, 1, partitionKey, confChange)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no replicas to upsert")
	})

	t.Run("update node with confChange v1", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		initialEntry := &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:1234"),
				},
			},
			Term:  1,
			Index: 1,
		}

		// Initialize with a replica
		err = rt.UpsertEntry(*initialEntry)
		require.NoError(t, err)

		confChange := raftpb.ConfChange{
			Type:    raftpb.ConfChangeUpdateNode,
			NodeID:  1,
			Context: serializeMetadata(createMetadata("localhost:5678")),
		}

		entry := initialEntry
		entry.Term = 2
		entry.Index = 2

		err = rt.ApplyConfChange(entry.Term, entry.Index, 1, partitionKey, confChange)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, "localhost:5678", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("apply multiple changes with ConfChangeV2", func(t *testing.T) {
		dir := t.TempDir()
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		initialEntry := &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:1234"),
				},
			},
			Term:  1,
			Index: 1,
		}

		// Initialize with a replica
		err = rt.UpsertEntry(*initialEntry)
		require.NoError(t, err)

		confChangeV2 := raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeRemoveNode,
					NodeID: 1,
				},
				{
					Type:   raftpb.ConfChangeAddNode,
					NodeID: 2,
				},
			},
			Context: serializeMetadata(createMetadata("localhost:8888")),
		}

		entry := initialEntry
		entry.Term = 2
		entry.Index = 2

		err = rt.ApplyConfChange(2, 2, 1, partitionKey, confChangeV2)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)

		var node2Found bool
		for _, replica := range updatedEntry.Replicas {
			if replica.GetMemberId() == 2 {
				node2Found = true
				require.Equal(t, "localhost:8888", replica.GetMetadata().GetAddress())
			}
		}
		require.True(t, node2Found, "Node 2 not found in updated replicas")
	})
}
