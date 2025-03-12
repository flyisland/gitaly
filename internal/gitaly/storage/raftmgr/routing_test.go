package raftmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
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
		nodeID := 1
		address := "localhost:1234"
		partitionKey := &gitalypb.PartitionKey{
			AuthorityName: "test-authority",
			PartitionId:   1,
		}

		entry := RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					NodeId:       uint64(nodeID),
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

		replica, err := rt.Translate(partitionKey, uint64(nodeID))
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
					NodeId:       1,
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

		nodeID := 999 // Non-existent node

		_, err := rt.Translate(partitionKey, uint64(nodeID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Key not found")
	})
}
