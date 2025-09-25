package raftmgr

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestPersistentRoutingTable(t *testing.T) {
	t.Parallel()

	dir := testhelper.TempDir(t)
	kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, kvStore.Close())
	}()

	rt := NewKVRoutingTable(kvStore)

	t.Run("add and translate member", func(t *testing.T) {
		memberID := 1
		address := "localhost:1234"
		partitionKey := NewPartitionKey("test-authority", 1)

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
		key := NewPartitionKey("test-authority", 2)

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
		partitionKey := NewPartitionKey("test-authority", 3)

		memberID := 999 // Non-existent node

		_, err := rt.Translate(partitionKey, uint64(memberID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Key not found")
	})
}

func TestApplyReplicaConfChange(t *testing.T) {
	t.Parallel()

	createMetadata := func(address string) *gitalypb.ReplicaID_Metadata {
		return &gitalypb.ReplicaID_Metadata{
			Address: address,
		}
	}

	t.Run("add node", func(t *testing.T) {
		t.Parallel()
		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		changes := NewReplicaConfChanges(1, 1, 1, 1, createMetadata("localhost:1234"))
		changes.AddChange(1, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.Replicas[0].GetMemberId())
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, "localhost:1234", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("remove node", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		initialEntry := &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:1234"),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
				{
					PartitionKey: partitionKey,
					MemberId:     2,
					StorageName:  "test-authority",
					Metadata:     createMetadata("localhost:5678"),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
			Term:  1,
			Index: 1,
		}

		// Initialize with a replica
		err = rt.UpsertEntry(*initialEntry)
		require.NoError(t, err)

		changes := NewReplicaConfChanges(2, 2, 1, 1, nil)
		changes.AddChange(2, ConfChangeRemoveNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, uint64(1), updatedEntry.Replicas[0].GetMemberId())
		require.Equal(t, gitalypb.ReplicaID_REPLICA_TYPE_VOTER, updatedEntry.Replicas[0].GetType())
		require.Equal(t, "localhost:1234", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("add learner node", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		changes := NewReplicaConfChanges(1, 1, 1, 1, createMetadata("localhost:1234"))
		changes.AddChange(1, ConfChangeAddLearnerNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, gitalypb.ReplicaID_REPLICA_TYPE_LEARNER, updatedEntry.Replicas[0].GetType())
	})

	t.Run("if member ID is zero, it should not be added to the routing table", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		changes := NewReplicaConfChanges(1, 1, 0, 1, createMetadata("localhost:1234"))
		changes.AddChange(0, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "member ID should be non-zero")
	})

	t.Run("should not add duplicate member ID", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		// First add a node
		changes := NewReplicaConfChanges(1, 1, 1, 1, createMetadata("localhost:1234"))
		changes.AddChange(1, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		// Try to add the same node ID again
		changes = NewReplicaConfChanges(2, 2, 1, 1, createMetadata("localhost:5678"))
		changes.AddChange(1, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "member ID 1 already exists")
	})

	t.Run("should error when updating non-existent member ID", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

		// Add a node with ID 1
		changes := NewReplicaConfChanges(1, 1, 1, 1, createMetadata("localhost:1234"))
		changes.AddChange(1, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		// Try to update a non-existent node with ID 2
		changes = NewReplicaConfChanges(2, 2, 1, 1, createMetadata("localhost:5678"))
		changes.AddChange(2, ConfChangeUpdateNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "member ID 2 not found for update")
	})

	t.Run("fails if the last remaining node is removed", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

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

		changes := NewReplicaConfChanges(entry.Term, entry.Index, 1, 1, nil)
		changes.AddChange(1, ConfChangeRemoveNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no replicas to upsert")
	})

	t.Run("update node", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

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

		changes := NewReplicaConfChanges(2, 2, 1, 1, createMetadata("localhost:5678"))
		changes.AddChange(1, ConfChangeUpdateNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
		require.NoError(t, err)

		updatedEntry, err := rt.GetEntry(partitionKey)
		require.NoError(t, err)

		require.Len(t, updatedEntry.Replicas, 1)
		require.Equal(t, uint64(1), updatedEntry.LeaderID)
		require.Equal(t, "localhost:5678", updatedEntry.Replicas[0].GetMetadata().GetAddress())
	})

	t.Run("apply multiple changes", func(t *testing.T) {
		t.Parallel()

		dir := testhelper.TempDir(t)
		kvStore, err := keyvalue.NewBadgerStore(testhelper.NewLogger(t), dir)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, kvStore.Close())
		}()

		rt := NewKVRoutingTable(kvStore)

		partitionKey := NewPartitionKey("test-authority", 1)

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

		changes := NewReplicaConfChanges(2, 2, 1, 1, createMetadata("localhost:8888"))
		changes.AddChange(1, ConfChangeRemoveNode)
		changes.AddChange(2, ConfChangeAddNode)

		err = rt.ApplyReplicaConfChange("test-authority", partitionKey, changes)
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

func TestPersistentRoutingTable_ListEntries(t *testing.T) {
	t.Parallel()

	// Helper function to create test metadata
	createMetadata := func(address string) *gitalypb.ReplicaID_Metadata {
		return &gitalypb.ReplicaID_Metadata{Address: address}
	}

	// Helper function to create test partition key
	createPartitionKey := func(authority string, partitionID uint64) *gitalypb.RaftPartitionKey {
		return NewPartitionKey(authority, storage.PartitionID(partitionID))
	}

	t.Run("empty routing table", func(t *testing.T) {
		rt := createRoutingTable(t)

		entries, err := rt.ListEntries()
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("single partition entry", func(t *testing.T) {
		rt := createRoutingTable(t)
		partitionKey := createPartitionKey("test-authority", 1)

		entry := RoutingTableEntry{
			RelativePath: "@hashed/test/repo.git",
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-storage",
					Metadata:     createMetadata("localhost:8075"),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
			LeaderID: 1,
			Term:     5,
			Index:    100,
		}

		require.NoError(t, rt.UpsertEntry(entry))

		entries, err := rt.ListEntries()
		require.NoError(t, err)
		require.Len(t, entries, 1)

		// Keys are opaque hashes, so we get the single entry
		actualEntry := entries["raft/"+partitionKey.GetValue()]
		require.NotNil(t, actualEntry)

		// Verify all fields match
		testhelper.ProtoEqual(t, &entry, actualEntry)
	})

	t.Run("multiple partition entries across different authorities", func(t *testing.T) {
		rt := createRoutingTable(t)

		// Create test entries
		entries := []struct {
			key   *gitalypb.RaftPartitionKey
			entry RoutingTableEntry
		}{
			{
				key: createPartitionKey("authority-1", 1),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo1.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-1", 1),
							MemberId:     1,
							StorageName:  "storage-1",
							Metadata:     createMetadata("localhost:8075"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
					},
					LeaderID: 1,
					Term:     2,
					Index:    10,
				},
			},
			{
				key: createPartitionKey("authority-1", 2),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo2.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-1", 2),
							MemberId:     2,
							StorageName:  "storage-2",
							Metadata:     createMetadata("localhost:8076"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
						{
							PartitionKey: createPartitionKey("authority-1", 2),
							MemberId:     3,
							StorageName:  "storage-3",
							Metadata:     createMetadata("localhost:8077"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_LEARNER,
						},
					},
					LeaderID: 2,
					Term:     3,
					Index:    25,
				},
			},
			{
				key: createPartitionKey("authority-2", 1),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo3.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-2", 1),
							MemberId:     1,
							StorageName:  "storage-4",
							Metadata:     createMetadata("localhost:9000"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
					},
					LeaderID: 1,
					Term:     1,
					Index:    5,
				},
			},
		}

		// Insert all entries
		for _, e := range entries {
			require.NoError(t, rt.UpsertEntry(e.entry))
		}

		// Verify all entries are listed
		listedEntries, err := rt.ListEntries()
		require.NoError(t, err)
		require.Len(t, listedEntries, len(entries))

		// Verify each entry
		for _, e := range entries {
			expectedKey := fmt.Sprintf("raft/%s", e.key.GetValue())
			actualEntry, exists := listedEntries[expectedKey]
			require.True(t, exists, "Entry %s should exist", expectedKey)

			testhelper.ProtoEqual(t, e.entry, *actualEntry)
		}
	})

	t.Run("list entries after updates", func(t *testing.T) {
		rt := createRoutingTable(t)
		partitionKey := createPartitionKey("test-authority", 1)

		// Insert initial entry
		initialEntry := RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "test-storage",
					Metadata:     createMetadata("localhost:8000"),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
			LeaderID: 1,
			Term:     1,
			Index:    1,
		}
		require.NoError(t, rt.UpsertEntry(initialEntry))

		// Verify initial listing
		entries, err := rt.ListEntries()
		require.NoError(t, err)
		require.Len(t, entries, 1)

		// Keys are opaque hashes, so we get the single entry
		var entry *RoutingTableEntry
		for _, e := range entries {
			entry = e
			break
		}
		require.NotNil(t, entry)
		require.Equal(t, uint64(1), entry.Term)
		require.Equal(t, uint64(1), entry.Index)

		// Update with higher term/index
		updatedEntry := initialEntry
		updatedEntry.Term = 3
		updatedEntry.Index = 15
		require.NoError(t, rt.UpsertEntry(updatedEntry))

		// Verify updated listing
		entries, err = rt.ListEntries()
		require.NoError(t, err)
		require.Len(t, entries, 1)

		// Keys are opaque hashes, so we get the single entry
		var updatedEntryFromList *RoutingTableEntry
		for _, e := range entries {
			updatedEntryFromList = e
			break
		}
		require.NotNil(t, updatedEntryFromList)
		require.Equal(t, uint64(3), updatedEntryFromList.Term)
		require.Equal(t, uint64(15), updatedEntryFromList.Index)
	})

	t.Run("concurrent access to ListEntries", func(t *testing.T) {
		rt := createRoutingTable(t)
		partitionKey := createPartitionKey("concurrent-authority", 1)

		entry := RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					MemberId:     1,
					StorageName:  "concurrent-storage",
					Metadata:     createMetadata("localhost:8080"),
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
				},
			},
			LeaderID: 1,
			Term:     1,
			Index:    1,
		}
		require.NoError(t, rt.UpsertEntry(entry))

		// Test concurrent reads
		const numConcurrent = 10
		var wg sync.WaitGroup
		wg.Add(numConcurrent)

		for i := 0; i < numConcurrent; i++ {
			go func() {
				defer wg.Done()
				entries, err := rt.ListEntries()
				require.NoError(t, err)
				require.Len(t, entries, 1)
			}()
		}

		wg.Wait()
	})

	t.Run("returns all entries", func(t *testing.T) {
		rt := createRoutingTable(t)

		// Create entries for multiple authorities
		entries := []struct {
			key   *gitalypb.RaftPartitionKey
			entry RoutingTableEntry
		}{
			{
				key: createPartitionKey("authority-1", 1),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo1.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-1", 1),
							MemberId:     1,
							StorageName:  "storage-1",
							Metadata:     createMetadata("localhost:8075"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
					},
					LeaderID: 1,
					Term:     2,
					Index:    10,
				},
			},
			{
				key: createPartitionKey("authority-1", 2),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo2.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-1", 2),
							MemberId:     2,
							StorageName:  "storage-1",
							Metadata:     createMetadata("localhost:8076"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
					},
					LeaderID: 2,
					Term:     3,
					Index:    25,
				},
			},
			{
				key: createPartitionKey("authority-2", 1),
				entry: RoutingTableEntry{
					RelativePath: "@hashed/repo3.git",
					Replicas: []*gitalypb.ReplicaID{
						{
							PartitionKey: createPartitionKey("authority-2", 1),
							MemberId:     1,
							StorageName:  "storage-2",
							Metadata:     createMetadata("localhost:9000"),
							Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						},
					},
					LeaderID: 1,
					Term:     1,
					Index:    5,
				},
			},
		}

		// Insert all entries
		for _, e := range entries {
			require.NoError(t, rt.UpsertEntry(e.entry))
		}

		allEntries, err := rt.ListEntries()
		require.NoError(t, err)
		require.Len(t, allEntries, 3) // All entries returned
	})
}
