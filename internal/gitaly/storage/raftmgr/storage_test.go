package raftmgr

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func setupStorage(t *testing.T, ctx context.Context, cfg config.Cfg) *Storage {
	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	logger := testhelper.NewLogger(t)
	db := getTestDBManager(t, ctx, cfg, logger)
	posTracker := log.NewPositionTracker()
	rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, NewMetrics())
	require.NoError(t, err)

	initStatus, err := rs.initialize(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, InitStatusUnbootstrapped, initStatus)

	t.Cleanup(func() { require.NoError(t, rs.close()) })
	return rs
}

func prepopulateEntries(t *testing.T, ctx context.Context, cfg config.Cfg, stagingDir, stateDir string, num int) {
	logManager := log.NewManager(cfg.Storages[0].Name, 1, stagingDir, stateDir, nil, log.NewPositionTracker())
	require.NoError(t, logManager.Initialize(ctx, 0))
	for i := 1; i <= num; i++ {
		entryLSN := storage.LSN(i)
		entryDir := testhelper.TempDir(t)
		_, err := logManager.CompareAndAppendLogEntry(entryLSN, entryDir)
		require.NoError(t, err)
	}
	require.NoError(t, logManager.Close())
}

func TestStorage_Initialize(t *testing.T) {
	t.Parallel()

	t.Run("raft initially errors out with InitStatusUnknown then bootstraps", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		// Create a storage instance but make the log manager fail during initialize
		// by providing a context that's already been cancelled
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately to force failure

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		// Initialize with canceled context should fail with InitStatusUnknown
		initStatus, err := rs.initialize(canceledCtx, 0)
		require.Error(t, err)
		require.Equal(t, InitStatusUnknown, initStatus)

		// Now try initializing again with a valid context, which should succeed
		initStatus, err = rs.initialize(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, InitStatusUnbootstrapped, initStatus)

		// Verify the storage is functional by checking its indices
		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIndex)

		// Now, bootstrap the storage by saving a hard state
		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 0}))

		// Create a brand new storage instance using the same state directory
		// to verify it properly bootstraps from the saved state
		// Use a new position tracker to avoid "already registered" errors
		posTracker2 := log.NewPositionTracker()
		rs2, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker2, logger, NewMetrics())
		require.NoError(t, err)

		// It should now initialize as bootstrapped
		initStatus, err = rs2.initialize(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, InitStatusBootstrapped, initStatus)

		// Cleanup
		require.NoError(t, rs.close())
		require.NoError(t, rs2.close())
	})

	prepopulateStorage := func(t *testing.T, ctx context.Context, cfg config.Cfg, appended int, committed uint64) (keyvalue.Transactioner, string) {
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		// Pre-populate n entries
		prepopulateEntries(t, ctx, cfg, stagingDir, stateDir, appended)

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		_, err = rs.initialize(ctx, 0)
		require.NoError(t, err)
		// Set on-disk commit LSN to n
		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 2, Vote: 1, Commit: committed}))
		require.NoError(t, rs.close())

		return db, stateDir
	}

	t.Run("raft storage is never bootstrapped", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)
		defer func() { require.NoError(t, rs.close()) }()

		initStatus, err := rs.initialize(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, InitStatusUnbootstrapped, initStatus, "expected fresh installation")

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIndex)

		require.Empty(t, rs.consumer.(*mockConsumer).GetNotifications())
	})

	t.Run("raft storage was bootstrapped, no left-over log entries after restart", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		logger := testhelper.NewLogger(t)

		// Simulate a prior session
		db, stateDir := prepopulateStorage(t, ctx, cfg, 3, 3)

		// Restart the storage using the same state dir
		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, testhelper.TempDir(t), stateDir, &mockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)

		defer func() { require.NoError(t, rs.close()) }()

		// Initialize
		initStatus, err := rs.initialize(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, InitStatusBootstrapped, initStatus, "expected bootstrapped installation")
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, 3))

		// Now the populated committedLSN is 3
		require.Equal(t, storage.LSN(3), rs.committedLSN)

		// First index is 4 (> last index) because all entries are being pruned
		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(4), firstIndex)

		// Last index is also 3
		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(3), lastIndex)

		// Notify for the first time.
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(4),
				highWaterMark: storage.LSN(3),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())
	})

	t.Run("raft storage was bootstrapped, some log entries are left over", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		logger := testhelper.NewLogger(t)

		// Simulate a prior session
		db, stateDir := prepopulateStorage(t, ctx, cfg, 5, 3)

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, testhelper.TempDir(t), stateDir, &mockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)

		defer func() { require.NoError(t, rs.close()) }()

		// Initialize with applied LSN 2
		initStatus, err := rs.initialize(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, InitStatusBootstrapped, initStatus, "expected bootstrapped installation")

		// First index is 3 == AppliedLSN + 1. Applied LSN is pruned.
		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(3), firstIndex)

		// Last index is 5, equal to the latest appended LSN.
		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(5), lastIndex)

		// Notify from low-water mark to the committedLSN for the first time.
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(3),
				highWaterMark: storage.LSN(3),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())
	})
}

func TestStorage_InitializeExistingPartition(t *testing.T) {
	t.Parallel()

	createTestLogEntry := func(t *testing.T, ctx context.Context, content string) string {
		t.Helper()

		entryDir := testhelper.TempDir(t)
		entry := wal.NewEntry(entryDir)
		entry.SetKey([]byte("test-key"), []byte(content))

		require.NoError(t, wal.WriteManifest(ctx, entryDir, &gitalypb.LogEntry{
			Operations: entry.Operations(),
		}))

		return entryDir
	}

	t.Run("detects existing partition and backfills metadata", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		// Setup directories and utilities
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		metrics := NewMetrics()

		// First, create a local (non-Raft) log manager and add some entries
		localLog := log.NewManager("test-storage", 1, stagingDir, stateDir, nil, posTracker)

		// Initialize the local log manager
		err := localLog.Initialize(ctx, 0)
		require.NoError(t, err)

		// Create and append some log entries
		var lsns []storage.LSN
		for i := 1; i <= 5; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("pre-raft-entry-%d", i))
			lsn, err := localLog.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
			lsns = append(lsns, lsn)
		}
		require.Len(t, lsns, 5, "expected 5 entries to be appended")

		// Set the applied LSN to the 3rd entry (index 2)
		appliedLSN := lsns[2]
		require.NoError(t, localLog.AcknowledgePosition(log.AppliedPosition, appliedLSN))
		require.NoError(t, localLog.Close())

		// Now initialize a Raft storage on the same directories
		raftStorage, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, metrics)
		require.NoError(t, err)
		defer func() { require.NoError(t, raftStorage.close()) }()

		// When initializing, it should detect the existing entries and return NeedsBackfill status
		initStatus, err := raftStorage.initialize(ctx, appliedLSN)
		require.NoError(t, err)
		require.Equal(t, InitStatusNeedsBackfill, initStatus,
			"should detect the existing log entries and report NeedsBackfill status")

		// Verify that the existing entries are recognized
		firstIndex, err := raftStorage.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(appliedLSN+1), firstIndex,
			"FirstIndex should be appliedLSN+1 (entries up to appliedLSN are pruned)")

		lastIndex, err := raftStorage.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(lsns[len(lsns)-1]), lastIndex,
			"LastIndex should match the highest existing LSN")

		// Verify we can fetch all non-pruned entries
		// Add one as the upper boundary is non-inclusive
		entries, err := raftStorage.Entries(firstIndex, lastIndex+1, 0)
		require.NoError(t, err)
		require.Equal(t, int(lastIndex+1-firstIndex), len(entries),
			"should return all entries from firstIndex to lastIndex")

		// Check that the entries have been backfilled with proper Raft metadata
		for i, entry := range entries {
			require.Equal(t, raftStorage.lastTerm, entry.Term,
				"entries should be assigned the current term during backfill")
			require.Equal(t, firstIndex+uint64(i), entry.Index,
				"entry index should match its position")
		}
	})

	t.Run("enable raft -> disable raft -> re-enable raft", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		// Setup directories and utilities
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		metrics := NewMetrics()

		// PHASE 1: Initialize with Raft enabled
		raftStorage1, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, metrics)
		require.NoError(t, err)

		initStatus, err := raftStorage1.initialize(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, InitStatusUnbootstrapped, initStatus)

		// Append entries using Raft
		for i := 1; i <= 3; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("raft-phase1-entry-%d", i))

			entry := raftpb.Entry{
				Term:  1,
				Index: uint64(i),
				Type:  raftpb.EntryNormal,
				Data:  []byte(fmt.Sprintf("raft-phase1-data-%d", i)),
			}

			require.NoError(t, raftStorage1.insertLogEntry(entry, logEntryPath))
		}

		// Save commit state and close
		highestRaftLSN := storage.LSN(3)
		require.NoError(t, raftStorage1.saveHardState(raftpb.HardState{
			Term:   1,
			Vote:   1,
			Commit: uint64(highestRaftLSN),
		}))
		require.NoError(t, raftStorage1.close())

		// PHASE 2: Use direct WAL
		localLog := log.NewManager(
			"test-storage",
			1,
			stagingDir,
			stateDir,
			nil,
			log.NewPositionTracker(),
		)

		// Initialize with the highest LSN from Raft phase
		err = localLog.Initialize(ctx, highestRaftLSN)
		require.NoError(t, err)

		// Add more entries directly to WAL
		for i := 1; i <= 2; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("direct-wal-entry-%d", i))
			_, err = localLog.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
		}

		highestDirectLSN := localLog.AppendedLSN()
		require.Equal(t, highestRaftLSN+2, highestDirectLSN,
			"highest direct WAL LSN should be equal to highest Raft LSN + 2 new entries")
		require.NoError(t, localLog.AcknowledgePosition(log.AppliedPosition, highestDirectLSN))
		require.NoError(t, localLog.Close())

		// PHASE 3: Re-enable Raft
		raftStorage2, err := NewStorage(
			"test-storage",
			1,
			cfg.Raft,
			db,
			stagingDir,
			stateDir,
			&mockConsumer{},
			log.NewPositionTracker(), // Create a new position tracker
			logger,
			metrics,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, raftStorage2.close()) }()

		// Re-initialize Raft with the highest LSN
		initStatus, err = raftStorage2.initialize(ctx, highestDirectLSN)
		require.NoError(t, err)

		// Since we had a previous Raft state, it should be detected as bootstrapped
		require.Equal(t, InitStatusBootstrapped, initStatus,
			"should be detected as bootstrapped since we have previous Raft state")

		// Check that the direct WAL entries are recognized in the re-enabled Raft storage
		lastIndex, err := raftStorage2.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(highestDirectLSN), lastIndex,
			"LastIndex should match highest direct WAL LSN")

		// Try to append a new entry after re-enabling Raft
		logEntryPath := createTestLogEntry(t, ctx, "raft-phase3-entry")
		entry := raftpb.Entry{
			Term:  2,
			Index: uint64(highestDirectLSN) + 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte("raft-phase3-data"),
		}
		require.NoError(t, raftStorage2.insertLogEntry(entry, logEntryPath))

		// Verify the new entry is added
		entries, err := raftStorage2.Entries(lastIndex+1, lastIndex+2, 0)
		require.NoError(t, err)
		require.Len(t, entries, 1, "should have one new entry")
		require.Equal(t, entry.Term, entries[0].Term)
		require.Equal(t, entry.Index, entries[0].Index)
	})
}

func TestStorage_InitialState(t *testing.T) {
	t.Parallel()

	t.Run("empty state returns defaults", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		cfg.Raft.SnapshotDir = testhelper.TempDir(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, nil, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		hs, cs, err := rs.InitialState()
		require.NoError(t, err)

		// When no hard state was stored, we expect empty defaults.
		require.Equal(t, raftpb.HardState{}, hs)
		require.Equal(t, raftpb.ConfState{}, cs)
	})

	t.Run("initial state exists", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		prepopulateEntries(t, ctx, cfg, stagingDir, stateDir, 10)

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, nil, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		defer func() { require.NoError(t, rs.close()) }()

		_, err = rs.initialize(ctx, 0)
		require.NoError(t, err)

		// Pre-populate the storage using abstractions
		require.NoError(t, rs.saveHardState(raftpb.HardState{
			Term:   4,
			Vote:   2,
			Commit: 10,
		}))
		require.NoError(t, rs.saveConfState(raftpb.ConfState{
			Voters:   []uint64{1, 2, 3},
			Learners: []uint64{4},
		}))

		hsOut, csOut, err := rs.InitialState()
		require.NoError(t, err)

		// Compare the stored hard state and conf state
		require.Equal(t, raftpb.HardState{
			Term:   4,
			Vote:   2,
			Commit: 10,
		}, hsOut)
		require.Equal(t, raftpb.ConfState{
			Voters:   []uint64{1, 2, 3},
			Learners: []uint64{4},
		}, csOut)
	})
}

func TestStorage_Entries(t *testing.T) {
	setupEntries := func(t *testing.T, ctx context.Context, rs *Storage) {
		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1 - pruned")},
			{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2 - pruned")},
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 2, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4 - overwritten")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 4, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
			{Term: 4, Index: 6, Type: raftpb.EntryNormal, Data: []byte("entry 6")},
		}

		for _, entry := range entries {
			logEntryPath := testhelper.TempDir(t)

			w := wal.NewEntry(logEntryPath)
			w.SetKey(
				[]byte(fmt.Sprintf("key-%d-%d", entry.Term, entry.Index)),
				[]byte(fmt.Sprintf("value-%d-%d", entry.Term, entry.Index)),
			)

			require.NoError(t, wal.WriteManifest(ctx, w.Directory(), &gitalypb.LogEntry{
				Operations: w.Operations(),
			}))
			require.NoError(t, rs.insertLogEntry(entry, logEntryPath))
		}
		// Set committedLSN and appliedLSN to 2. Log entry 1 and 2 are pruned.
		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}))
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, 2))
	}

	t.Run("query all entries from empty WAL", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		cfg.Raft.SnapshotDir = testhelper.TempDir(t)

		rs := setupStorage(t, ctx, cfg)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		fetchedEntries, err := rs.Entries(firstIndex, lastIndex+1, 0)
		require.ErrorIs(t, err, raft.ErrUnavailable)
		require.Empty(t, fetchedEntries)
	})

	t.Run("query all entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		fetchedEntries, err := rs.Entries(firstIndex, lastIndex+1, 0)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 4, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
			{Term: 4, Index: 6, Type: raftpb.EntryNormal, Data: []byte("entry 6")},
		}, fetchedEntries)
	})

	t.Run("query all entries with with a limit < available entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		fetchedEntries, err := rs.Entries(firstIndex, lastIndex+1, 2)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
		}, fetchedEntries)
	})

	t.Run("query all entries with with a limit == available entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		fetchedEntries, err := rs.Entries(firstIndex, lastIndex+1, 4)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 4, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
			{Term: 4, Index: 6, Type: raftpb.EntryNormal, Data: []byte("entry 6")},
		}, fetchedEntries)
	})

	t.Run("query all entries with with a limit > available entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		fetchedEntries, err := rs.Entries(firstIndex, lastIndex+1, 99)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 4, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
			{Term: 4, Index: 6, Type: raftpb.EntryNormal, Data: []byte("entry 6")},
		}, fetchedEntries)
	})

	t.Run("query a subset of entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		fetchedEntries, err := rs.Entries(4, 6, 0)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 4, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
		}, fetchedEntries)
	})

	t.Run("query a subset of entries + limit", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		fetchedEntries, err := rs.Entries(4, 6, 1)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
		}, fetchedEntries)
	})

	t.Run("query compacted entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		fetchedEntries, err := rs.Entries(1, 6, 0)
		require.ErrorIs(t, err, raft.ErrCompacted)
		require.Empty(t, fetchedEntries)
	})

	t.Run("query unavailable entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		// No entries available

		fetchedEntries, err := rs.Entries(3, 6, 0)
		require.ErrorIs(t, err, raft.ErrUnavailable)
		require.Empty(t, fetchedEntries)
	})

	t.Run("query out-of-range entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		fetchedEntries, err := rs.Entries(3, 99, 0)
		require.ErrorContains(t, err, "reading out-of-bound entries")
		require.Empty(t, fetchedEntries)
	})
}

func TestStorage_Term(t *testing.T) {
	t.Parallel()

	insertEntry := func(t *testing.T, ctx context.Context, rs *Storage, entry raftpb.Entry) {
		logEntryPath := testhelper.TempDir(t)
		w := wal.NewEntry(logEntryPath)
		require.NoError(t, wal.WriteManifest(ctx, w.Directory(), &gitalypb.LogEntry{
			Operations: w.Operations(),
		}))
		require.NoError(t, rs.insertLogEntry(entry, logEntryPath))
	}

	setupEntries := func(t *testing.T, ctx context.Context, rs *Storage) {
		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1 - pruned")},
			{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
		}

		for _, entry := range entries {
			insertEntry(t, ctx, rs, entry)
		}
		// Set committedLSN and appliedLSN to 1. Log entry 1 is pruned.
		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 1}))
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, 1))
	}

	t.Run("query term of the last entry of an empty WAL", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		term, err := rs.Term(lastIndex)
		require.NoError(t, err)
		require.Equal(t, uint64(0), term)
	})

	t.Run("query term of normal entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		term, err := rs.Term(2)
		require.NoError(t, err)
		require.Equal(t, uint64(1), term)

		term, err = rs.Term(3)
		require.NoError(t, err)
		require.Equal(t, uint64(2), term)

		term, err = rs.Term(4)
		require.NoError(t, err)
		require.Equal(t, uint64(3), term)
	})

	t.Run("query term of a pruned entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		_, err := rs.Term(1)
		require.ErrorIs(t, err, raft.ErrCompacted)
	})

	t.Run("query term of an entry beyond the last entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		rs := setupStorage(t, ctx, cfg)
		setupEntries(t, ctx, rs)

		_, err := rs.Term(5)
		require.ErrorIs(t, err, raft.ErrUnavailable)
	})

	t.Run("query term of pruned entries after a restart", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)

		_, err = rs.initialize(ctx, 0)
		require.NoError(t, err)
		setupEntries(t, ctx, rs)

		// Commit and apply all entries
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, 4))
		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 4, Vote: 1, Commit: 4}))
		require.NoError(t, rs.close())

		// Now restart the storage
		rs, err = NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)
		defer func() { require.NoError(t, rs.close()) }()

		_, err = rs.initialize(ctx, 4)
		require.NoError(t, err)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)

		// Log entry 4 is pruned. Its term is implied from the last hard state.
		term, err := rs.Term(lastIndex)
		require.NoError(t, err)
		require.Equal(t, uint64(4), term)

		// Insert another log entry and make it pruned
		insertEntry(t, ctx, rs, raftpb.Entry{
			Term: 99, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5"),
		})

		require.NoError(t, rs.saveHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}))
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, 5))

		// First Index > Last Index now. Log entry 5 is pruned.
		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(6), firstIndex)

		lastIndex, err = rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(5), lastIndex)

		// The term is queryable
		term, err = rs.Term(lastIndex)
		require.NoError(t, err)
		require.Equal(t, uint64(99), term)
	})
}

func TestStorage_insertLogEntry(t *testing.T) {
	t.Parallel()

	testAppendLogEntry(t, func(t *testing.T, ctx context.Context, rs *Storage, entry raftpb.Entry) error {
		logEntryPath := testhelper.TempDir(t)

		w := wal.NewEntry(logEntryPath)
		w.SetKey(
			[]byte(fmt.Sprintf("key-%d-%d", entry.Term, entry.Index)),
			[]byte(fmt.Sprintf("value-%d-%d", entry.Term, entry.Index)),
		)

		require.NoError(t, wal.WriteManifest(ctx, w.Directory(), &gitalypb.LogEntry{
			Operations: w.Operations(),
		}))

		return rs.insertLogEntry(entry, logEntryPath)
	})
}

func TestStorage_draftLogEntry(t *testing.T) {
	t.Parallel()

	testAppendLogEntry(t, func(t *testing.T, ctx context.Context, rs *Storage, entry raftpb.Entry) error {
		return rs.draftLogEntry(entry, func(w *wal.Entry) error {
			w.SetKey(
				[]byte(fmt.Sprintf("key-%d-%d", entry.Term, entry.Index)),
				[]byte(fmt.Sprintf("value-%d-%d", entry.Term, entry.Index)),
			)
			return nil
		})
	})
}

func assertEntries(
	t *testing.T,
	rs *Storage,
	expectedEntries []raftpb.Entry,
	actualEntries []raftpb.Entry,
) {
	t.Helper()

	require.Equal(t, len(expectedEntries), len(actualEntries))
	for i, expectedEntry := range expectedEntries {
		require.Equal(t, expectedEntry, actualEntries[i])

		term, err := rs.Term(expectedEntry.Index)
		require.NoError(t, err)
		require.Equal(t, expectedEntry.Term, term)

		logEntry, err := rs.readLogEntry(storage.LSN(expectedEntry.Index))
		require.NoError(t, err)
		testhelper.ProtoEqual(t, &gitalypb.LogEntry{
			Operations: []*gitalypb.LogEntry_Operation{
				{
					Operation: &gitalypb.LogEntry_Operation_SetKey_{
						SetKey: &gitalypb.LogEntry_Operation_SetKey{
							Key:   []byte(fmt.Sprintf("key-%d-%d", expectedEntry.Term, expectedEntry.Index)),
							Value: []byte(fmt.Sprintf("value-%d-%d", expectedEntry.Term, expectedEntry.Index)),
						},
					},
				},
			},
		}, logEntry)
	}
}

func testAppendLogEntry(t *testing.T, appendFunc func(*testing.T, context.Context, *Storage, raftpb.Entry) error) {
	t.Run("insert a log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		raftEntry := raftpb.Entry{
			Term:  99,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte("content 1"),
		}

		require.NoError(t, appendFunc(t, ctx, rs, raftEntry))

		entries, err := rs.Entries(1, 2, 0)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{raftEntry}, entries)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), lastIndex)
	})

	t.Run("insert multiple log entries in sequence", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
			{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
			{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
		}

		for _, entry := range entries {
			require.NoError(t, appendFunc(t, ctx, rs, entry))
		}

		fetchedEntries, err := rs.Entries(1, 4, 0)
		require.NoError(t, err)

		assertEntries(t, rs, entries, fetchedEntries)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(3), lastIndex)
	})

	t.Run("insert overlapping log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		originalEntry := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("original")}
		newEntry := raftpb.Entry{Term: 2, Index: 1, Type: raftpb.EntryNormal, Data: []byte("replacement")}

		require.NoError(t, appendFunc(t, ctx, rs, originalEntry))
		require.NoError(t, appendFunc(t, ctx, rs, newEntry))

		fetchedEntries, err := rs.Entries(1, 2, 0)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{newEntry}, fetchedEntries)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), lastIndex)
	})

	t.Run("insert multiple overlapping entries with full range overlap", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		entriesBatches := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
			{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
			{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2 - replacement")},
			{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3 - replacement")},
			{Term: 2, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
			{Term: 3, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3 - second replacement")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4 - replacement")},
			{Term: 3, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
		}

		for _, entry := range entriesBatches {
			require.NoError(t, appendFunc(t, ctx, rs, entry))
		}

		// Final expected entries after resolving overlaps
		expectedEntries := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2 - replacement")},
			{Term: 3, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3 - second replacement")},
			{Term: 3, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4 - replacement")},
			{Term: 3, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
		}

		// Validate that only the correct entries remain after overlaps
		fetchedEntries, err := rs.Entries(1, 6, 0)
		require.NoError(t, err)
		assertEntries(t, rs, expectedEntries, fetchedEntries)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(5), lastIndex)
	})

	t.Run("insert large log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		largeData := make([]byte, 10*1024*1024) // 10MB payload
		raftEntry := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: largeData}

		require.NoError(t, appendFunc(t, ctx, rs, raftEntry))

		entries, err := rs.Entries(1, 2, 0)
		require.NoError(t, err)

		assertEntries(t, rs, []raftpb.Entry{raftEntry}, entries)

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), lastIndex)
	})

	t.Run("insert log entry beyond current LSN", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		raftEntry := raftpb.Entry{Term: 1, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry out of range")}
		err := appendFunc(t, ctx, rs, raftEntry)

		// Expecting an error as the LSN is beyond the current range
		require.Error(t, err, "expected error when inserting entry beyond current LSN")

		firstIndex, err := rs.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(1), firstIndex)

		lastIndex, err := rs.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIndex)
	})

	t.Run("insert log entry below committed LSN", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))

		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		logger := testhelper.NewLogger(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		prepopulateEntries(t, ctx, cfg, stagingDir, stateDir, 10)

		rs, err := NewStorage("test-storage", 1, cfg.Raft, db, stagingDir, stateDir, &mockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)
		_, err = rs.initialize(ctx, 0)
		require.NoError(t, err)

		defer func() { require.NoError(t, rs.close()) }()

		require.NoError(t, rs.saveHardState(raftpb.HardState{
			Term:   1,
			Vote:   1,
			Commit: 3,
		}))

		raftEntry := raftpb.Entry{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry below committed LSN")}

		// Expecting an error as the entry's index is below the committed LSN
		require.Error(t, appendFunc(t, ctx, rs, raftEntry), "expected error when inserting entry below committed LSN")
	})
}

func TestStorage_SaveHardState(t *testing.T) {
	t.Parallel()

	insertEntry := func(t *testing.T, ctx context.Context, rs *Storage, entry raftpb.Entry) error {
		logEntryPath := testhelper.TempDir(t)

		w := wal.NewEntry(logEntryPath)
		require.NoError(t, wal.WriteManifest(ctx, w.Directory(), &gitalypb.LogEntry{
			Operations: w.Operations(),
		}))

		return rs.insertLogEntry(entry, logEntryPath)
	}

	t.Run("advance committed LSN successfully", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		// Pre-populate the log with entries
		entries := []raftpb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		}
		for _, entry := range entries {
			require.NoError(t, insertEntry(t, ctx, rs, entry))
		}

		// Has not received any notification, yet. Highest appendedLSN is 3.
		require.Empty(t, rs.consumer.(*mockConsumer).GetNotifications())

		// Committed set to 1
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 1, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(1), rs.committedLSN)

		// Receive notification from low water mark -> 1
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())

		// Committed set to 2
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 2, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(2), rs.committedLSN)

		// Receive notification from low water mark -> 2
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(2),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())

		// Committed set to 3
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 3, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(3), rs.committedLSN)

		// Receive notification from low water mark -> 3
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(2),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(3),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())
	})

	t.Run("notify consumer since the low water mark", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		// Pre-populate the log with entries
		entries := []raftpb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		}
		for _, entry := range entries {
			require.NoError(t, insertEntry(t, ctx, rs, entry))
		}

		// Has not received any notification, yet. Highest appendedLSN is 3.
		require.Empty(t, rs.consumer.(*mockConsumer).GetNotifications())

		// Committed set to 1
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 1, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(1), rs.committedLSN)

		// Receive notification from 1 -> 1
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())

		// Simulate applying up to log entry 1
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, storage.LSN(1)))
		require.Equal(t, storage.LSN(2), rs.localLog.LowWaterMark())

		// Committed set to 2
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 2, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(2), rs.committedLSN)

		// Receive notification from 2 -> 2
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(2),
				highWaterMark: storage.LSN(2),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())

		// Committed set to 3, but don't update low water mark
		require.NoError(t, rs.saveHardState(raftpb.HardState{Commit: 3, Vote: 1, Term: 1}))
		require.Equal(t, storage.LSN(3), rs.committedLSN)

		// Receive notification from 2 -> 3
		require.Equal(t, []mockNotification{
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(1),
				highWaterMark: storage.LSN(1),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(2),
				highWaterMark: storage.LSN(2),
			},
			{
				storageName:   rs.authorityName,
				partitionID:   rs.partitionID,
				lowWaterMark:  storage.LSN(2),
				highWaterMark: storage.LSN(3),
			},
		}, rs.consumer.(*mockConsumer).GetNotifications())

		// Simulate applying up to log entry 3
		require.NoError(t, rs.localLog.AcknowledgePosition(log.AppliedPosition, storage.LSN(3)))
		require.Equal(t, storage.LSN(4), rs.localLog.LowWaterMark())

		// No new notifications are sent.
		require.Equal(t, 3, len(rs.consumer.(*mockConsumer).GetNotifications()))
	})

	t.Run("reject LSN beyond appendedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
			Raft: config.Raft{SnapshotDir: testhelper.TempDir(t)},
		}))
		rs := setupStorage(t, ctx, cfg)

		entries := []raftpb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		}
		for _, entry := range entries {
			require.NoError(t, insertEntry(t, ctx, rs, entry))
		}

		err := rs.saveHardState(raftpb.HardState{
			Term:   1,
			Vote:   1,
			Commit: 3,
		})
		require.ErrorContains(t, err, "next committed LSN exceeds appended LSN 3 > 2")
	})
}
