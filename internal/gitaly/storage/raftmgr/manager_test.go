package raftmgr

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func raftConfigsForTest(t *testing.T) config.Raft {
	// Speed up initial election overhead in the test setup
	return config.Raft{
		Enabled:         true,
		ClusterID:       "test-cluster",
		ElectionTicks:   5,
		HeartbeatTicks:  2,
		RTTMilliseconds: 100,
		SnapshotDir:     testhelper.TempDir(t),
	}
}

func TestManager_Initialize(t *testing.T) {
	t.Parallel()

	t.Run("succeeds when raft is enabled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		recorder := NewEntryRecorder()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger, WithEntryRecorder(recorder))
		require.NoError(t, err)

		// Initialize the manager
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Verify that the manager is properly initialized
		require.True(t, mgr.started)
		require.NotNil(t, mgr.node)

		// Verify that the first config change is recorded
		// After initialization, Raft typically creates a config change
		// entry to establish the initial configuration
		require.Eventually(t, func() bool {
			return recorder.Len() > 0
		}, 5*time.Second, 10*time.Millisecond, "expected at least one entry to be recorded")

		// Verify at least one entry from Raft was recorded (typically a config change)
		raftEntries := recorder.FromRaft()
		require.NotEmpty(t, raftEntries, "expected at least one Raft entry after initialization")

		// Close the manager
		require.NoError(t, mgr.Close())
	})

	t.Run("fails when raft is not enabled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)
		raftCfg.Enabled = false

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		// Configure manager with Raft disabled
		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger)
		require.Nil(t, mgr)
		require.ErrorContains(t, err, "raft is not enabled")
	})

	t.Run("fails when manager is reused", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger)
		require.NoError(t, err)

		// First initialization should succeed
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Second initialization should fail
		err = mgr.Initialize(ctx, 0)
		require.EqualError(t, err, fmt.Sprintf("raft manager for partition %q already started", partitionID))

		require.NoError(t, mgr.Close())
	})
}

func TestManager_AppendLogEntry(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, ctx context.Context, cfg config.Cfg) (*Manager, *EntryRecorder) {
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		recorder := NewEntryRecorder()

		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger, WithEntryRecorder(recorder))
		require.NoError(t, err)

		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		return mgr, recorder
	}

	createLogEntry := func(t *testing.T, ctx context.Context, content string) string {
		entryDir := testhelper.TempDir(t)
		entry := wal.NewEntry(entryDir)
		entry.SetKey([]byte("test-key"), []byte(content))

		// Create a few files in the directory to simulate actual log entry data
		for i := 1; i <= 3; i++ {
			filePath := filepath.Join(entryDir, fmt.Sprintf("file-%d", i))
			require.NoError(t, os.WriteFile(filePath, []byte(content), 0o644))
		}

		// Write the manifest
		require.NoError(t, wal.WriteManifest(ctx, entryDir, &gitalypb.LogEntry{
			Operations: entry.Operations(),
		}))

		return entryDir
	}

	t.Run("append single log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, recorder := setup(t, ctx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		logEntryPath := createLogEntry(t, ctx, "test-content-1")
		lsn, err := mgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Greater(t, lsn, storage.LSN(0), "expected a valid LSN")

		// Verify that the log entry was recorded
		require.Eventually(t, func() bool {
			// The entry might be recorded with an offset due to Raft internal entries
			return recorder.Len() >= 3
		}, 5*time.Second, 10*time.Millisecond, "expected log entry to be recorded")

		// Check that our entry is not marked as coming from Raft
		require.False(t, recorder.IsFromRaft(lsn), "expected user-submitted entry not to be from Raft")

		// Verify entry content
		logEntry, err := wal.ReadManifest(mgr.GetEntryPath(lsn))
		require.NoError(t, err)
		require.NotNil(t, logEntry)
		require.Len(t, logEntry.GetOperations(), 1)
		require.Equal(t, []byte("test-key"), logEntry.GetOperations()[0].GetSetKey().GetKey())
		require.Equal(t, []byte("test-content-1"), logEntry.GetOperations()[0].GetSetKey().GetValue())
	})

	t.Run("append multiple log entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, recorder := setup(t, ctx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Create and append multiple log entries
		var lsns []storage.LSN

		for i := 1; i <= 3; i++ {
			logEntryPath := createLogEntry(t, ctx, fmt.Sprintf("test-content-%d", i))
			lsn, err := mgr.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
			lsns = append(lsns, lsn)
		}

		// Verify that all log entries were recorded
		require.Eventually(t, func() bool {
			return recorder.Len() >= 3
		}, 5*time.Second, 10*time.Millisecond, "expected all log entries to be recorded")

		// Verify entries are in order
		require.IsIncreasing(t, lsns, "expected increasing LSNs")

		for i := 0; i < 3; i++ {
			logEntry, err := wal.ReadManifest(mgr.GetEntryPath(lsns[i]))
			require.NoError(t, err)
			require.NotNil(t, logEntry)
			require.Len(t, logEntry.GetOperations(), 1)
			require.Equal(t, []byte("test-key"), logEntry.GetOperations()[0].GetSetKey().GetKey())
			require.Equal(t, []byte(fmt.Sprintf("test-content-%d", i+1)), logEntry.GetOperations()[0].GetSetKey().GetValue())
		}
	})

	t.Run("append multiple log entries concurrently", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, _ := setup(t, ctx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Number of concurrent entries to append. We use a gate so that all log entries are appended at the
		// same time. We need to verify if the event loop works correctly when handling multiple entries in the
		// same batch. However, the order is non-deterministic. There's also no guarantee (although very likely)
		// a batch contains more than one entry.
		numEntries := 20
		var wg sync.WaitGroup
		wg.Add(numEntries)

		// Create a starting gate to coordinate concurrent execution
		startGate := make(chan struct{})

		// Store results
		results := make(chan struct {
			lsn storage.LSN
			err error
			idx int
		}, numEntries)

		// Launch goroutines to append entries concurrently
		for i := 1; i <= numEntries; i++ {
			go func(idx int) {
				defer wg.Done()

				// Wait for the starting gate
				<-startGate

				// Create and append log entry
				logEntryPath := createLogEntry(t, ctx, fmt.Sprintf("test-content-%d", idx))
				lsn, err := mgr.AppendLogEntry(logEntryPath)

				// Send the result back
				results <- struct {
					lsn storage.LSN
					err error
					idx int
				}{lsn, err, idx}
			}(i)
		}

		// Start all goroutines at once
		close(startGate)

		// Collect all results
		var lsns []storage.LSN
		lsnMap := make(map[int]storage.LSN) // Maps index to LSN for content verification

		wg.Wait()
		close(results)

		for res := range results {
			require.NoError(t, res.err, "AppendLogEntry should not fail for entry %d", res.idx)
			require.Greater(t, res.lsn, storage.LSN(0), "should return a valid LSN for entry %d", res.idx)
			lsns = append(lsns, res.lsn)
			lsnMap[res.idx] = res.lsn
		}

		// Verify entries are ordered when sorted
		sortedLSNs := make([]storage.LSN, len(lsns))
		copy(sortedLSNs, lsns)
		sort.Slice(sortedLSNs, func(i, j int) bool {
			return sortedLSNs[i] < sortedLSNs[j]
		})

		require.IsIncreasing(t, sortedLSNs, "LSNs should be unique and increasing")

		// Verify each entry's content matches its index
		for idx, lsn := range lsnMap {
			logEntry, err := wal.ReadManifest(mgr.GetEntryPath(lsn))
			require.NoError(t, err)
			require.NotNil(t, logEntry)
			require.Len(t, logEntry.GetOperations(), 1)
			require.Equal(t, []byte("test-key"), logEntry.GetOperations()[0].GetSetKey().GetKey())
			require.Equal(t, []byte(fmt.Sprintf("test-content-%d", idx)), logEntry.GetOperations()[0].GetSetKey().GetValue())
		}
	})

	t.Run("operation timeout", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		recorder := NewEntryRecorder()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		// Create manager with very short operation timeout
		mgr, err := NewManager(
			storageName,
			partitionID,
			raftCfg,
			raftStorage,
			logger,
			WithEntryRecorder(recorder),
			WithOpTimeout(1*time.Nanosecond), // Set a very short timeout to force failure
		)
		require.NoError(t, err)

		// Initialize the manager
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Attempting to append should time out
		logEntryPath := createLogEntry(t, ctx, "timeout-test")
		_, err = mgr.AppendLogEntry(logEntryPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		cancelCtx, cancel := context.WithCancel(testhelper.Context(t))
		mgr, _ := setup(t, cancelCtx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Cancel the context before append
		cancel()

		// Attempt to append should fail with context canceled
		logEntryPath := createLogEntry(t, ctx, "cancel-test")
		_, err := mgr.AppendLogEntry(logEntryPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context canceled")
	})
}

func TestManager_Close(t *testing.T) {
	t.Parallel()

	t.Run("close initialized manager", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := config.Raft{
			Enabled:         true,
			RTTMilliseconds: 100,
			ElectionTicks:   10,
			HeartbeatTicks:  1,
			SnapshotDir:     testhelper.TempDir(t),
		}

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger)
		require.NoError(t, err)

		// Initialize the manager
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Close the manager
		err = mgr.Close()
		require.NoError(t, err, "expected Close to succeed")

		// Second close should still work (idempotent)
		err = mgr.Close()
		require.NoError(t, err, "expected second Close to succeed")
	})

	t.Run("close uninitialized manager", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := config.Raft{
			Enabled:         true,
			RTTMilliseconds: 100,
			ElectionTicks:   10,
			HeartbeatTicks:  1,
			SnapshotDir:     testhelper.TempDir(t),
		}

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger)
		require.NoError(t, err)

		// Close without initializing
		err = mgr.Close()
		require.NoError(t, err, "expected Close to succeed even without initialization")
	})

	t.Run("verify raft internal entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db := getTestDBManager(t, ctx, cfg, logger)
		posTracker := log.NewPositionTracker()
		recorder := NewEntryRecorder()

		// Create a raft storage
		raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
		require.NoError(t, err)

		mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger, WithEntryRecorder(recorder))
		require.NoError(t, err)

		// Initialize the manager
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Get entries generated by Raft internal processes
		raftEntries := recorder.FromRaft()
		require.NotEmpty(t, raftEntries, "expected some internal entries generated by Raft")

		// Verify that at least one entry is a config change (usually the first one)
		foundConfigChange := false
		for _, entry := range raftEntries {
			// Look for entries that might be related to configuration
			for _, op := range entry.GetOperations() {
				if op.GetSetKey() != nil && string(op.GetSetKey().GetKey()) == string(KeyLastConfigChange) {
					foundConfigChange = true
					break
				}
			}
			if foundConfigChange {
				break
			}
		}
		require.True(t, foundConfigChange, "expected to find at least one config change entry")
	})
}

func TestManager_NotImplementedLogMethods(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	// Configure manager with Raft enabled
	raftCfg := config.Raft{
		Enabled:         true,
		RTTMilliseconds: 100,
		ElectionTicks:   10,
		HeartbeatTicks:  1,
		SnapshotDir:     testhelper.TempDir(t),
	}

	storageName := cfg.Storages[0].Name
	partitionID := storage.PartitionID(1)
	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	db := getTestDBManager(t, ctx, cfg, logger)
	posTracker := log.NewPositionTracker()

	// Create a raft storage
	raftStorage, err := NewStorage(raftCfg, logger, storageName, partitionID, db, stagingDir, stateDir, &mockConsumer{}, posTracker, NewMetrics())
	require.NoError(t, err)

	mgr, err := NewManager(storageName, partitionID, raftCfg, raftStorage, logger)
	require.NoError(t, err)

	// Initialize the manager
	err = mgr.Initialize(ctx, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mgr.Close())
	}()

	// Test CompareAndAppendLogEntry - should not be implemented
	_, err = mgr.CompareAndAppendLogEntry(1, "/path/to/log")
	require.ErrorContains(t, err, "raft manager does not support CompareAndAppendLogEntry")

	// Test DeleteLogEntry - should not be implemented
	err = mgr.DeleteLogEntry(1)
	require.ErrorContains(t, err, "raft manager does not support DeleteLogEntry")
}
