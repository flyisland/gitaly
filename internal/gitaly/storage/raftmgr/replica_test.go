package raftmgr

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	logrus "gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

const (
	waitTimeout = 10 * time.Second
)

func raftConfigsForTest(t *testing.T) config.Raft {
	// Speed up initial election overhead in the test setup
	return config.Raft{
		Enabled:                   true,
		ClusterID:                 "test-cluster",
		ElectionTicks:             5,
		HeartbeatTicks:            2,
		RTTMilliseconds:           100,
		ProposalConfChangeTimeout: 1500,
		SnapshotDir:               testhelper.TempDir(t),
	}
}

func createTestLogEntry(t *testing.T, ctx context.Context, content string) string {
	t.Helper()

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

func dbSetup(t *testing.T, ctx context.Context, cfg config.Cfg, dbPath string, storageName string, logger logrus.Logger) (keyvalue.Store, *databasemgr.DBManager) {
	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		func(log logrus.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(log, filepath.Join(dbPath, path))
		},
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	db, err := dbMgr.GetDB(storageName)
	require.NoError(t, err)

	return db, dbMgr
}

func TestReplica_Initialize(t *testing.T) {
	t.Parallel()
	t.Run("succeeds when raft is enabled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()
		raftCfg := raftConfigsForTest(t)
		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, storage.PartitionID(1), metrics, WithEntryRecorder(recorder))
		require.NoError(t, err)

		// Initialize the replica
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Verify that the replica is properly initialized
		require.True(t, mgr.started)
		require.NotNil(t, mgr.node)

		// Verify that the first config change is recorded
		require.Eventually(t, func() bool {
			return recorder.Len() > 0
		}, waitTimeout, 10*time.Millisecond, "expected at least one entry to be recorded")

		// Verify at least one entry from Raft was recorded (typically a config change)
		raftEntries := recorder.FromRaft()
		require.NotEmpty(t, raftEntries, "expected at least one Raft entry after initialization")

		// Close the replica
		require.NoError(t, mgr.Close())

		testhelper.RequirePromMetrics(t, metrics, `
			# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
			# TYPE gitaly_raft_log_entries_processed counter
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
			# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
			# TYPE gitaly_raft_proposal_queue_depth gauge
			gitaly_raft_proposal_queue_depth{storage="default"} 0
		`)
	})

	t.Run("fails when raft is not enabled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		raftCfg.Enabled = false

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, storage.PartitionID(1), NewMetrics())
		require.Nil(t, mgr)
		require.ErrorContains(t, err, "raft is not enabled")
	})

	t.Run("fails when replica is reused", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		partitionID := storage.PartitionID(1)

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics())
		require.NoError(t, err)

		// First initialization should succeed
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Second initialization should fail
		err = mgr.Initialize(ctx, 0)
		require.EqualError(t, err, fmt.Sprintf("raft replica %q already started", mgr.partitionKey.GetValue()))

		require.NoError(t, mgr.Close())
	})

	t.Run("fail waiting for raft group to be ready", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		partitionID := storage.PartitionID(1)
		recorder := NewReplicaEntryRecorder()

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics(), WithEntryRecorder(recorder))
		require.NoError(t, err)

		releaseReady := make(chan struct{})
		mgr.hooks.BeforeHandleReady = func() {
			<-releaseReady
		}

		// Initialize the replica
		err = mgr.Initialize(ctx, 0)
		require.ErrorIs(t, err, ErrReadyTimeout)

		close(releaseReady)

		require.NoError(t, mgr.Close())
	})

	t.Run("enable Raft on an existing partition with all entrires applied", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		posTracker := log.NewPositionTracker()
		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()

		db, dbMgr := dbSetup(t, ctx, cfg, testhelper.TempDir(t), storageName, logger)

		// First, create a local log manager and add some entries
		localLog := log.NewManager(
			storageName,
			partitionID,
			stagingDir,
			stateDir,
			nil,
			posTracker,
		)

		// Initialize the local log manager
		err := localLog.Initialize(ctx, 0)
		require.NoError(t, err)

		// Create and append some log entries
		var highestLSN storage.LSN
		for i := 1; i <= 3; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("pre-raft-entry-%d", i))
			lsn, err := localLog.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
			highestLSN = lsn
		}

		require.NoError(t, localLog.AcknowledgePosition(log.AppliedPosition, highestLSN))
		require.NoError(t, localLog.Close())

		// Now create a Raft log store pointing to the same directories
		logStore, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		raftNode, err := NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(recorder))
		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")

		mgr, err := raftFactory(ctx, storageName, logStore, logger, metrics)
		require.NoError(t, err)
		defer func() { require.NoError(t, mgr.Close()) }()

		// Initialize the raft replica with the highest LSN
		err = mgr.Initialize(ctx, highestLSN)
		require.NoError(t, err)

		// Verify that the existing entries are recognized and accessible
		require.Equal(t, highestLSN, mgr.AppendedLSN(), "AppendedLSN should match the highest pre-existing LSN")

		// Append a new entry to verify normal operation
		logEntryPath := createTestLogEntry(t, ctx, "post-raft-entry")
		newLSN, err := mgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Equal(t, highestLSN+1, mgr.LowWaterMark())
		require.Equal(t, newLSN, recorder.WithoutRaftEntries(highestLSN+1))
	})

	t.Run("enable Raft on an existing partition with some entrires have not been applied", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		posTracker := log.NewPositionTracker()
		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()

		db, dbMgr := dbSetup(t, ctx, cfg, testhelper.TempDir(t), storageName, logger)

		// First, create a local log manager and add some entries
		localLog := log.NewManager(
			storageName,
			partitionID,
			stagingDir,
			stateDir,
			nil,
			posTracker,
		)

		// Initialize the local log manager
		err := localLog.Initialize(ctx, 0)
		require.NoError(t, err)

		// Create and append 5 log entries
		var lsns []storage.LSN
		for i := 1; i <= 5; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("pre-raft-entry-%d", i))
			lsn, err := localLog.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
			lsns = append(lsns, lsn)
		}
		require.Len(t, lsns, 5, "expected 5 entries to be appended")
		highestLSN := lsns[len(lsns)-1]

		// Set the applied LSN to be the 3rd entry (not the highest)
		appliedLSN := lsns[2]
		require.NoError(t, localLog.AcknowledgePosition(log.AppliedPosition, appliedLSN))

		// Close the local log manager
		require.NoError(t, localLog.Close())

		// Now create a Raft log store pointing to the same directories
		logStore, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, posTracker, logger, NewMetrics())
		require.NoError(t, err)

		raftNode, err := NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(recorder))

		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		mgr, err := raftFactory(ctx, storageName, logStore, logger, metrics)
		require.NoError(t, err)
		defer func() { require.NoError(t, mgr.Close()) }()

		// Initialize the raft replica with the applied LSN (not the highest)
		err = mgr.Initialize(ctx, appliedLSN)
		require.NoError(t, err)

		// Verify that all existing entries (including unapplied ones) are recognized
		require.Equal(t, highestLSN, mgr.AppendedLSN(), "AppendedLSN should match the highest pre-existing LSN")

		// Append a new entry to verify normal operation
		logEntryPath := createTestLogEntry(t, ctx, "post-raft-entry")
		newLSN, err := mgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Equal(t, appliedLSN+1, mgr.LowWaterMark())
		require.Equal(t, newLSN, recorder.WithoutRaftEntries(highestLSN+1))
	})

	t.Run("enable raft -> disable raft -> enable raft again", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		storageName := cfg.Storages[0].Name
		partitionID := storage.PartitionID(1)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		db, dbMgr := dbSetup(t, ctx, cfg, testhelper.TempDir(t), storageName, logger)

		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()

		// PHASE 1: Create a new partition with Raft enabled
		logStore1, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)

		raftNode, err := NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(recorder))

		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		mgr1, err := raftFactory(ctx, storageName, logStore1, logger, metrics)
		require.NoError(t, err)

		// Initialize the raft replica
		err = mgr1.Initialize(ctx, 0)
		require.NoError(t, err)

		// Append 3 entries using Raft
		for i := 1; i <= 3; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("raft-phase1-entry-%d", i))
			_, err = mgr1.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
		}

		// Get the highest LSN after Raft phase
		highestRaftLSN := mgr1.AppendedLSN()

		// Close the raft replica (simulate disabling Raft)
		require.NoError(t, mgr1.Close())

		// PHASE 2: Use direct WAL
		localLog := log.NewManager(
			storageName,
			partitionID,
			stagingDir,
			stateDir,
			nil,
			log.NewPositionTracker(),
		)

		// Initialize the local log manager with the last applied LSN
		err = localLog.Initialize(ctx, highestRaftLSN)
		require.NoError(t, err)

		require.Equal(t, highestRaftLSN, localLog.AppendedLSN(),
			"Highest direct WAL LSN should be equal to highest Raft LSN from previous session")

		// Append 2 more entries directly to WAL
		for i := 1; i <= 2; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("direct-wal-entry-%d", i))
			_, err = localLog.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
		}

		highestDirectLSN := localLog.AppendedLSN()
		require.Equal(t, highestDirectLSN, highestRaftLSN+2,
			"Highest direct WAL LSN should be equal than highest Raft LSN + 2 new entries")

		// Close the local log manager
		require.NoError(t, localLog.Close())

		// Verify we can read the entry appended.
		for i := 1; i <= 2; i++ {
			entry, err := wal.ReadManifest(localLog.GetEntryPath(highestRaftLSN + storage.LSN(i)))
			require.NoError(t, err)
			require.NotNil(t, entry, "Should be able to read entry appended under Raft")
		}

		// PHASE 3: Re-enable Raft
		logStore2, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, log.NewPositionTracker(), logger, NewMetrics())
		require.NoError(t, err)

		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		mgr2, err := raftFactory(ctx, storageName, logStore2, logger, metrics)
		require.NoError(t, err)

		// Re-initialize Raft with the highest LSN
		err = mgr2.Initialize(ctx, highestDirectLSN)
		require.NoError(t, err)

		// Verify that all entries are recognized
		require.Equal(t, recorder.WithRaftEntries(highestDirectLSN), mgr2.AppendedLSN(),
			"Re-enabled Raft should see highest LSN from direct WAL phase")

		// Append one more entry with re-enabled Raft
		logEntryPath := createTestLogEntry(t, ctx, "raft-phase3-entry")
		finalLSN, err := mgr2.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Greater(t, recorder.WithRaftEntries(finalLSN), recorder.WithRaftEntries(highestDirectLSN)+1,
			"Final LSN should be greater than highest direct WAL LSN")

		// Verify Phase 3 entry (re-enabled Raft). Entries from phase 1 and phase 2 are properly pruned.
		entry, err := wal.ReadManifest(mgr2.GetEntryPath(finalLSN))
		require.NoError(t, err)
		require.Contains(t, string(entry.GetOperations()[0].GetSetKey().GetValue()),
			"raft-phase3-entry", "Should be able to read final Raft entry")

		// Clean up
		require.NoError(t, mgr2.Close())
	})
}

func TestReplica_AppendLogEntry(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, ctx context.Context, cfg config.Cfg) (*Replica, *Metrics, *ReplicaEntryRecorder) {
		raftCfg := raftConfigsForTest(t)
		partitionID := storage.PartitionID(1)
		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()
		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics, WithEntryRecorder(recorder))
		require.NoError(t, err)

		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		return mgr, metrics, recorder
	}

	t.Run("append single log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, metrics, recorder := setup(t, ctx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		logEntryPath := createTestLogEntry(t, ctx, "test-content-1")
		lsn, err := mgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Greater(t, lsn, storage.LSN(0), "expected a valid LSN")

		// Verify that the log entry was recorded
		require.Eventually(t, func() bool {
			// The entry might be recorded with an offset due to Raft internal entries
			return recorder.Len() >= 3
		}, waitTimeout, 10*time.Millisecond, "expected log entry to be recorded")

		// Check that our entry is not marked as coming from Raft
		require.False(t, recorder.IsFromRaft(lsn), "expected user-submitted entry not to be from Raft")

		// Verify entry content
		logEntry, err := wal.ReadManifest(mgr.GetEntryPath(lsn))
		require.NoError(t, err)
		require.NotNil(t, logEntry)
		require.Len(t, logEntry.GetOperations(), 1)
		require.Equal(t, []byte("test-key"), logEntry.GetOperations()[0].GetSetKey().GetKey())
		require.Equal(t, []byte("test-content-1"), logEntry.GetOperations()[0].GetSetKey().GetValue())

		testhelper.RequirePromMetrics(t, metrics, `
        		# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
        		# TYPE gitaly_raft_log_entries_processed counter
        		gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 1
        		# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        		# TYPE gitaly_raft_proposal_queue_depth gauge
        		gitaly_raft_proposal_queue_depth{storage="default"} 0
        		# HELP gitaly_raft_proposals_total Counter of all Raft proposals.
        		# TYPE gitaly_raft_proposals_total counter
        		gitaly_raft_proposals_total{result="success",storage="default"} 1
		`)
	})

	t.Run("append multiple log entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, metrics, recorder := setup(t, ctx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Create and append multiple log entries
		var lsns []storage.LSN

		for i := 1; i <= 3; i++ {
			logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("test-content-%d", i))
			lsn, err := mgr.AppendLogEntry(logEntryPath)
			require.NoError(t, err)
			lsns = append(lsns, lsn)
		}

		// Verify that all log entries were recorded
		require.Eventually(t, func() bool {
			return recorder.Len() >= 3
		}, waitTimeout, 10*time.Millisecond, "expected all log entries to be recorded")

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

		testhelper.RequirePromMetrics(t, metrics, `
        		# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
        		# TYPE gitaly_raft_log_entries_processed counter
        		gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 3
        		gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 3
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 1
        		# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        		# TYPE gitaly_raft_proposal_queue_depth gauge
        		gitaly_raft_proposal_queue_depth{storage="default"} 0
        		# HELP gitaly_raft_proposals_total Counter of all Raft proposals.
        		# TYPE gitaly_raft_proposals_total counter
        		gitaly_raft_proposals_total{result="success",storage="default"} 3
		`)
	})

	t.Run("append multiple log entries concurrently", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		mgr, metrics, _ := setup(t, ctx, cfg)
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
				logEntryPath := createTestLogEntry(t, ctx, fmt.Sprintf("test-content-%d", idx))
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

		testhelper.RequirePromMetrics(t, metrics, `
        		# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
        		# TYPE gitaly_raft_log_entries_processed counter
        		gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 20
        		gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 20
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 1
        		gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 1
        		# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
        		# TYPE gitaly_raft_proposal_queue_depth gauge
        		gitaly_raft_proposal_queue_depth{storage="default"} 0
        		# HELP gitaly_raft_proposals_total Counter of all Raft proposals.
        		# TYPE gitaly_raft_proposals_total counter
        		gitaly_raft_proposals_total{result="success",storage="default"} 20
		`)
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
		posTracker := log.NewPositionTracker()
		recorder := NewReplicaEntryRecorder()
		metrics := NewMetrics()

		db, dbMgr := dbSetup(t, ctx, cfg, testhelper.TempDir(t), storageName, logger)
		// Create a Raft log store
		logStore, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, posTracker, logger, metrics)
		require.NoError(t, err)

		raftNode, err := NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(recorder), WithOpTimeout(1*time.Nanosecond))

		// Create replica with very short operation timeout
		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		mgr, err := raftFactory(
			ctx,
			storageName,
			logStore,
			logger,
			metrics,
		)
		require.NoError(t, err)

		// Initialize the replica
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Attempting to append should time out
		logEntryPath := createTestLogEntry(t, ctx, "timeout-test")
		_, err = mgr.AppendLogEntry(logEntryPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context deadline exceeded")

		testhelper.RequirePromMetrics(t, metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
		`)
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		cancelCtx, cancel := context.WithCancel(testhelper.Context(t))
		mgr, metrics, _ := setup(t, cancelCtx, cfg)
		defer func() {
			require.NoError(t, mgr.Close())
		}()

		// Cancel the context before append
		cancel()

		// Attempt to append should fail with context canceled
		logEntryPath := createTestLogEntry(t, ctx, "cancel-test")
		_, err := mgr.AppendLogEntry(logEntryPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context canceled")

		testhelper.RequirePromMetrics(t, metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
		`)
	})
}

func TestReplica_AppendLogEntry_CrashRecovery(t *testing.T) {
	t.Parallel()

	// testEnv encapsulates the test environment for raft replica crash tests
	type testEnv struct {
		mgr         *Replica
		db          keyvalue.Transactioner
		dbMgr       *databasemgr.DBManager
		stagingDir  string
		stateDir    string
		cfg         config.Cfg
		recorder    *ReplicaEntryRecorder
		baseLSN     storage.LSN
		storageName string
		partitionID storage.PartitionID
		metrics     *Metrics
	}

	// Helper for setting up a test environment
	setupTest := func(t *testing.T, ctx context.Context, partitionID storage.PartitionID, setupFuncs ...func(*Replica)) testEnv {
		t.Helper()

		cfg := testcfg.Build(t)
		raftCfg := raftConfigsForTest(t)
		logger := testhelper.NewLogger(t)

		storageName := cfg.Storages[0].Name
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)
		recorder := NewReplicaEntryRecorder()

		metrics := NewMetrics()

		dbMgr := openTestDB(t, ctx, cfg, logger)
		t.Cleanup(dbMgr.Close)

		db, err := dbMgr.GetDB(cfg.Storages[0].Name)
		require.NoError(t, err)

		logStore, err := NewReplicaLogStore(storageName, partitionID, raftCfg, db, stagingDir, stateDir, &MockConsumer{}, log.NewPositionTracker(), logger, metrics)
		require.NoError(t, err)

		raftNode, err := NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(recorder))

		// Configure replica
		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		mgr, err := raftFactory(ctx, storageName, logStore, logger, metrics)
		require.NoError(t, err)

		for _, f := range setupFuncs {
			f(mgr)
		}

		// Initialize the replica
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Create a log entry and append it to establish a baseline
		logEntryPath := createTestLogEntry(t, ctx, "base-content")
		lsn, err := mgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)
		require.Greater(t, lsn, storage.LSN(0))

		return testEnv{
			mgr:         mgr,
			db:          db,
			dbMgr:       dbMgr,
			stagingDir:  stagingDir,
			stateDir:    stateDir,
			cfg:         cfg,
			recorder:    recorder,
			baseLSN:     lsn,
			storageName: storageName,
			partitionID: partitionID,
			metrics:     metrics,
		}
	}

	// Helper to create a recovery replica -- a new instance of the raft replica that picks resumes from where the
	// crashed manager left off.
	createRecoveryReplica := func(t *testing.T, ctx context.Context, env testEnv, lastLSN storage.LSN) *Replica {
		t.Helper()

		logger := testhelper.NewLogger(t)
		raftCfg := raftConfigsForTest(t)

		// Get a new DB connection from the existing DB manager
		dbMgr := env.dbMgr
		db, err := dbMgr.GetDB(env.cfg.Storages[0].Name)
		require.NoError(t, err)

		logStore, err := NewReplicaLogStore(env.storageName, env.partitionID, raftCfg, db, env.stagingDir, env.stateDir, &MockConsumer{}, log.NewPositionTracker(), logger, env.metrics)
		require.NoError(t, err)

		raftNode, err := NewNode(env.cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := DefaultFactoryWithNode(raftCfg, raftNode, WithEntryRecorder(env.recorder))

		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(env.storageName, env.partitionID), 1, "")
		recoveryMgr, err := raftFactory(ctx, env.storageName, logStore, logger, env.metrics)
		require.NoError(t, err)

		// Initialize with the last known LSN
		err = recoveryMgr.Initialize(ctx, lastLSN)
		require.NoError(t, err)

		return recoveryMgr
	}

	// Helper to verify recovery when change is NOT expected to be persisted or retained
	verifyNonPersistingRecovery := func(t *testing.T, ctx context.Context, recoveryMgr *Replica, baseLSN storage.LSN, crashContent string) {
		t.Helper()

		// First append a new entry - this is crucial because it may trigger overwriting
		// of any uncommitted entries that might exist from before the crash
		logEntryPath := createTestLogEntry(t, ctx, "recovery-content")
		newLSN, err := recoveryMgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)

		// Get the recorder which contains all recorded entries
		recorder := recoveryMgr.EntryRecorder
		require.NotNil(t, recorder, "EntryRecorder must be configured for verification")

		// Check all entries in the recorder after the base LSN
		// We should NOT find our crash content in any user (non-Raft) entries
		crashEntryFound := false

		// Examine all entries from baseLSN+1 to newLSN
		for lsn := baseLSN + 1; lsn <= newLSN; lsn++ {
			// Skip Raft internal entries - we only care about user entries
			if recorder.IsFromRaft(lsn) {
				continue
			}

			// For user entries, check if the content matches our crash content
			entry, err := wal.ReadManifest(recoveryMgr.GetEntryPath(lsn))
			if err != nil {
				continue // Skip entries that can't be read
			}

			// Check if this entry contains our crash content
			for _, op := range entry.GetOperations() {
				if op.GetSetKey() != nil &&
					string(op.GetSetKey().GetKey()) == "test-key" &&
					string(op.GetSetKey().GetValue()) == crashContent {
					crashEntryFound = true
					break
				}
			}

			if crashEntryFound {
				break
			}
		}

		// Our crash content should NOT be persisted anywhere after recovery and new append
		require.False(t, crashEntryFound,
			"entry with crash content '%s' should not exist after recovery and a new append", crashContent)

		// Verify our recovery entry has the expected content
		newEntry, err := wal.ReadManifest(recoveryMgr.GetEntryPath(newLSN))
		require.NoError(t, err)

		// Check for the specific recovery content
		recoveryContentFound := false
		for _, op := range newEntry.GetOperations() {
			if op.GetSetKey() != nil &&
				string(op.GetSetKey().GetKey()) == "test-key" &&
				string(op.GetSetKey().GetValue()) == "recovery-content" {
				recoveryContentFound = true
				break
			}
		}
		require.True(t, recoveryContentFound, "recovery content should be found in new entry")
	}

	// Helper to verify recovery when change IS expected to be persisted and retained
	verifyPersistingRecovery := func(t *testing.T, ctx context.Context, recoveryMgr *Replica, baseLSN storage.LSN, crashContent string) {
		t.Helper()

		// First append a new entry - this will tell us if the crash entry is truly committed
		// If the crash entry wasn't actually committed, it would be overwritten now
		logEntryPath := createTestLogEntry(t, ctx, "recovery-content")
		newLSN, err := recoveryMgr.AppendLogEntry(logEntryPath)
		require.NoError(t, err)

		// Get the recorder which contains all recorded entries
		recorder := recoveryMgr.EntryRecorder
		require.NotNil(t, recorder, "EntryRecorder must be configured for verification")

		// Now check all user entries to see if our crash content survived the append of a new entry
		crashEntryLSN := storage.LSN(0)

		// Examine all entries from baseLSN+1 to newLSN
		for lsn := baseLSN + 1; lsn < newLSN; lsn++ {
			// Skip Raft internal entries - we only care about user entries
			if recorder.IsFromRaft(lsn) {
				continue
			}

			// For user entries, check if the content matches our crash content
			entry, err := wal.ReadManifest(recoveryMgr.GetEntryPath(lsn))
			if err != nil {
				continue // Skip entries that can't be read
			}

			// Check if this entry contains our crash content
			for _, op := range entry.GetOperations() {
				if op.GetSetKey() != nil &&
					string(op.GetSetKey().GetKey()) == "test-key" &&
					string(op.GetSetKey().GetValue()) == crashContent {
					crashEntryLSN = lsn
					break
				}
			}

			if crashEntryLSN != 0 {
				break
			}
		}

		// Our crash content should still be persisted somewhere even after a new append
		require.NotEqual(t, storage.LSN(0), crashEntryLSN,
			"committed entry with crash content '%s' should exist even after a new append", crashContent)

		// Verify our recovery entry is also there and has the expected content
		newEntry, err := wal.ReadManifest(recoveryMgr.GetEntryPath(newLSN))
		require.NoError(t, err)
		require.False(t, recorder.IsFromRaft(newLSN), "recovery entry should not be from Raft")

		// Check for the specific recovery content
		recoveryContentFound := false
		for _, op := range newEntry.GetOperations() {
			if op.GetSetKey() != nil &&
				string(op.GetSetKey().GetKey()) == "test-key" &&
				string(op.GetSetKey().GetValue()) == "recovery-content" {
				recoveryContentFound = true
				break
			}
		}
		require.True(t, recoveryContentFound, "recovery content should be found in new entry")
	}

	t.Run("AppendLogEntry crash during propose", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		env := setupTest(t, ctx, storage.PartitionID(1))

		// Set up hook to panic during propose
		env.mgr.hooks.BeforePropose = func(logEntryPath string) {
			panic("simulated crash during propose")
		}

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-propose"
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		// Try to append - should panic
		require.PanicsWithValue(t, "simulated crash during propose", func() {
			_, _ = env.mgr.AppendLogEntry(logEntryPath)
		})

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// Verify recovery - change should NOT be persisted
		verifyNonPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		// This simulation crashes caller's goroutine, not in raft replica's life cycle.
		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="success",storage="default"} 3
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 0
		`)
	})

	t.Run("AppendLogEntry crash during commit entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		env := setupTest(t, ctx, storage.PartitionID(2))

		// Set up hook to panic during commit entries
		env.mgr.hooks.BeforeProcessCommittedEntries = func([]raftpb.Entry) {
			panic("simulated crash during commit entries")
		}

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-commit"
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		// Try to append - should fail with ErrReplicaStopped
		_, err := env.mgr.AppendLogEntry(logEntryPath)
		require.ErrorIs(t, err, ErrReplicaStopped)

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-env.mgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during commit entries")

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// Verify recovery - change SHOULD be persisted
		// Even though client received an error, the entry was persisted before the crash
		verifyPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
                        gitaly_raft_proposals_total{result="success",storage="default"} 2
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})

	t.Run("AppendLogEntry crash during node advance, after committed", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		// The trigger prevents node advance from being triggered during first election. The hook must be
		// inserted before Initialize(). Otherwise, we'll have a race.
		var trigger atomic.Bool
		env := setupTest(t, ctx, storage.PartitionID(3), func(mgr *Replica) {
			mgr.hooks.BeforeAdvance = func() {
				if trigger.Load() {
					panic("simulated crash during node advance")
				}
			}
		})

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-node-advance"
		env.mgr.hooks.BeforeProcessCommittedEntries = func(committed []raftpb.Entry) {
			if len(committed) > 0 {
				trigger.Store(true)
			}
		}
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		// Try to append - should return nil error since entry is committed before crash
		lsn, err := env.mgr.AppendLogEntry(logEntryPath)
		// At this point, the log entry is committed. Callers should receive the result
		require.NoError(t, err, "client should receive success before the crash")
		require.Greater(t, lsn, env.baseLSN, "should return valid LSN before crash")

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-env.mgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during node advance")

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// Verify recovery - change SHOULD be persisted and client already got success
		verifyPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="success",storage="default"} 3
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})

	t.Run("AppendLogEntry crash during handle ready", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		env := setupTest(t, ctx, storage.PartitionID(4))

		// Set up hook to panic during handle ready
		env.mgr.hooks.BeforeHandleReady = func() {
			panic("simulated crash during handle ready")
		}

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-handle-ready"
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		_, err := env.mgr.AppendLogEntry(logEntryPath)
		require.ErrorIs(t, err, ErrReplicaStopped)

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-env.mgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during handle ready")

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// Verify recovery - change should NOT be persisted
		// In a single-node setup, this behaves like a propose crash (change not persisted)
		// In a multi-node setup, this could behave differently as the entry might already
		// be replicated to other nodes before the crash
		verifyNonPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
                        gitaly_raft_proposals_total{result="success",storage="default"} 2
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})

	t.Run("AppendLogEntry crash during insert log entry", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		env := setupTest(t, ctx, storage.PartitionID(5))

		// Set up hook to panic during insert log entry
		env.mgr.logStore.hooks.BeforeInsertLogEntry = func(index uint64) {
			panic("simulated crash during insert log entry")
		}

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-insert"
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		_, err := env.mgr.AppendLogEntry(logEntryPath)
		require.ErrorIs(t, err, ErrReplicaStopped)

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-env.mgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during insert log entry")

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// Verify recovery - change should NOT be persisted
		// In a single-node setup, this behaves like a propose crash (change not persisted)
		// In a multi-node setup, this could behave differently as the entry might already
		// be replicated to other nodes before the crash
		verifyNonPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
                        gitaly_raft_proposals_total{result="success",storage="default"} 2
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})

	t.Run("AppendLogEntry crash during save hard state", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		env := setupTest(t, ctx, storage.PartitionID(6))

		// Set up hook to panic during save hard state
		env.mgr.logStore.hooks.BeforeSaveHardState = func() {
			panic("simulated crash during save hard state")
		}

		// Create a test entry that will trigger the panic
		crashContent := "crash-during-save-hard-state"
		logEntryPath := createTestLogEntry(t, ctx, crashContent)

		// In a single-node setup, this behaves like a propose crash (change not persisted)
		// In a multi-node setup, this could behave differently as the entry might already
		// be replicated to other nodes before the crash
		_, err := env.mgr.AppendLogEntry(logEntryPath)
		require.ErrorIs(t, err, ErrReplicaStopped)

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-env.mgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during save hard state")

		// Create recovery replica
		require.NoError(t, env.mgr.Close())
		recoveryMgr := createRecoveryReplica(t, ctx, env, env.baseLSN)
		defer testhelper.MustClose(t, recoveryMgr)

		// When a crash occurs during saveHardState, the log entry is still persisted, unlike crashes
		// during propose, handle ready, or insert log entry. This is because the entry is already
		// physically written to disk in Storage.insertLogEntry (including fsync calls) before
		// saveHardState is invoked. This behavior follows the guideline:
		// https://pkg.go.dev/go.etcd.io/raft/v3#section-readme.
		// The hard state update merely records metadata about what's committed, but doesn't affect the entry's
		// persistence. During recovery, Raft will find the entry on disk even though the hard state wasn't
		// updated to reflect it. In a single-node setup, this entry will be considered valid and retained
		// because there's no conflicting entry with a higher term from another node. In multi-node setups, this
		// behavior might differ as entries could be overwritten by entries with higher terms from the new
		// leader.
		verifyPersistingRecovery(t, ctx, recoveryMgr, env.baseLSN, crashContent)

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 3
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 2
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 2
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
                        gitaly_raft_proposals_total{result="success",storage="default"} 2
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})

	t.Run("AppendLogEntry multiple crash and recovery cycle", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		env := setupTest(t, ctx, storage.PartitionID(7))

		// Keep track of LSNs for each successful append
		lastKnownLSN := env.baseLSN

		// First recovery cycle - crash during propose
		require.NoError(t, env.mgr.Close())
		firstRecoveryMgr := createRecoveryReplica(t, ctx, env, lastKnownLSN)

		// Add one successful entry to advance the LSN
		logEntryPath1 := createTestLogEntry(t, ctx, "first-recovery-success")
		lsn1, err := firstRecoveryMgr.AppendLogEntry(logEntryPath1)
		require.NoError(t, err)
		require.Greater(t, lsn1, lastKnownLSN)
		lastKnownLSN = lsn1

		// Set up hook to crash during propose
		firstRecoveryMgr.hooks.BeforePropose = func(logEntryPath string) {
			panic("simulated crash during first recovery")
		}

		// Attempt that will crash
		logEntryPath2 := createTestLogEntry(t, ctx, "first-recovery-crash")
		require.PanicsWithValue(t, "simulated crash during first recovery", func() {
			_, _ = firstRecoveryMgr.AppendLogEntry(logEntryPath2)
		})

		// Close the replica only, keep the DB manager
		require.NoError(t, firstRecoveryMgr.Close())

		// Second recovery cycle - crash during commit
		secondRecoveryMgr := createRecoveryReplica(t, ctx, env, lastKnownLSN)

		// Add one successful entry to advance the LSN
		logEntryPath3 := createTestLogEntry(t, ctx, "second-recovery-success")
		lsn2, err := secondRecoveryMgr.AppendLogEntry(logEntryPath3)
		require.NoError(t, err)
		require.Greater(t, lsn2, lastKnownLSN)
		lastKnownLSN = lsn2

		// Set up hook to crash during commit
		secondRecoveryMgr.hooks.BeforeProcessCommittedEntries = func(committed []raftpb.Entry) {
			if len(committed) > 0 {
				panic("simulated crash during second recovery")
			}
		}

		// Attempt that will crash
		logEntryPath4 := createTestLogEntry(t, ctx, "second-recovery-crash")
		_, err = secondRecoveryMgr.AppendLogEntry(logEntryPath4)
		require.ErrorIs(t, err, ErrReplicaStopped)

		var finalErr error
		require.Eventually(t, func() bool {
			finalErr = <-secondRecoveryMgr.GetNotificationQueue()
			return finalErr != nil
		}, waitTimeout, 10*time.Millisecond)
		require.ErrorContains(t, finalErr, "simulated crash during second recovery")

		// Close the replica only, keep the DB manager
		require.NoError(t, secondRecoveryMgr.Close())

		// Final recovery - verify system state after multiple crashes
		finalRecoveryMgr := createRecoveryReplica(t, ctx, env, lastKnownLSN)

		// For commit crash, the crashed entry should be persisted
		require.Greater(t, finalRecoveryMgr.AppendedLSN(), lastKnownLSN,
			"commit entry crash should persist the change")

		// Verify crashed entry content
		entry, err := wal.ReadManifest(finalRecoveryMgr.GetEntryPath(lastKnownLSN + 1))
		require.NoError(t, err)
		require.Contains(t, entry.String(), "second-recovery-crash",
			"entry should contain content from crash during commit")

		// Should be able to append new entries after recovery
		logEntryPath5 := createTestLogEntry(t, ctx, "final-recovery-success")
		finalLSN, err := finalRecoveryMgr.AppendLogEntry(logEntryPath5)
		require.NoError(t, err)
		require.Greater(t, finalLSN, lastKnownLSN+1)

		require.NoError(t, finalRecoveryMgr.Close())

		testhelper.RequirePromMetrics(t, env.metrics, `
                        # HELP gitaly_raft_log_entries_processed Rate of log entries processed.
                        # TYPE gitaly_raft_log_entries_processed counter
                        gitaly_raft_log_entries_processed{entry_type="application",operation="append",storage="default"} 5
                        gitaly_raft_log_entries_processed{entry_type="application",operation="commit",storage="default"} 4
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 1
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 4
                        gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 4
                        # HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
                        # TYPE gitaly_raft_proposal_queue_depth gauge
                        gitaly_raft_proposal_queue_depth{storage="default"} 0
                        # HELP gitaly_raft_proposals_total Counter of all Raft proposals.
                        # TYPE gitaly_raft_proposals_total counter
                        gitaly_raft_proposals_total{result="error",storage="default"} 1
                        gitaly_raft_proposals_total{result="success",storage="default"} 5
                        # HELP gitaly_raft_event_loop_crashes_total Counter of Raft event loop crashes
                        # TYPE gitaly_raft_event_loop_crashes_total counter
                        gitaly_raft_event_loop_crashes_total{storage="default"} 1
		`)
	})
}

func TestReplica_Close(t *testing.T) {
	t.Parallel()

	t.Run("close initialized replica", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		partitionID := storage.PartitionID(1)
		raftCfg := config.Raft{
			Enabled:         true,
			RTTMilliseconds: 100,
			ElectionTicks:   10,
			HeartbeatTicks:  1,
			SnapshotDir:     testhelper.TempDir(t),
		}

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics())
		require.NoError(t, err)

		// Initialize the replica
		err = mgr.Initialize(ctx, 0)
		require.NoError(t, err)

		// Close the replica
		err = mgr.Close()
		require.NoError(t, err, "expected Close to succeed")

		// Second close should still work (idempotent)
		err = mgr.Close()
		require.NoError(t, err, "expected second Close to succeed")
	})

	t.Run("close uninitialized replica", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := config.Raft{
			Enabled:         true,
			RTTMilliseconds: 100,
			ElectionTicks:   10,
			HeartbeatTicks:  1,
			SnapshotDir:     testhelper.TempDir(t),
		}
		partitionID := storage.PartitionID(1)

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics())
		require.NoError(t, err)

		// Close without initializing
		err = mgr.Close()
		require.NoError(t, err, "expected Close to succeed even without initialization")
	})

	t.Run("verify raft internal entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)

		partitionID := storage.PartitionID(1)
		recorder := NewReplicaEntryRecorder()

		mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics(), WithEntryRecorder(recorder))
		require.NoError(t, err)

		// Initialize the replica
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

func TestReplica_NotImplementedLogMethods(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	// Configure replica with Raft enabled
	raftCfg := config.Raft{
		Enabled:         true,
		RTTMilliseconds: 100,
		ElectionTicks:   10,
		HeartbeatTicks:  1,
		SnapshotDir:     testhelper.TempDir(t),
	}

	partitionID := storage.PartitionID(1)
	mgr, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, NewMetrics())
	require.NoError(t, err)

	// Initialize the replica
	err = mgr.Initialize(ctx, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mgr.Close())
	}()

	// Test CompareAndAppendLogEntry - should not be implemented
	_, err = mgr.CompareAndAppendLogEntry(1, "/path/to/log")
	require.ErrorContains(t, err, "raft replica does not support CompareAndAppendLogEntry")

	// Test DeleteLogEntry - should not be implemented
	err = mgr.DeleteLogEntry(1)
	require.ErrorContains(t, err, "raft replica does not support DeleteLogEntry")
}

func TestReplica_StorageConnection(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)

	cfg.Raft = config.Raft{
		Enabled:         true,
		ElectionTicks:   5,
		RTTMilliseconds: 100,
		SnapshotDir:     testhelper.TempDir(t),
	}

	dbPath := testhelper.TempDir(t)
	storageName := cfg.Storages[0].Name
	db, dbMgr := dbSetup(t, ctx, cfg, dbPath, storageName, logger)

	partitionID := storage.PartitionID(1)
	stagingDir := testhelper.TempDir(t)
	stateDir := testhelper.TempDir(t)
	posTracker := log.NewPositionTracker()

	logStore, err := NewReplicaLogStore(
		storageName,
		partitionID,
		cfg.Raft,
		db,
		stagingDir,
		stateDir,
		nil,
		posTracker,
		logger,
		NewMetrics(),
	)
	require.NoError(t, err)

	raftNode, err := NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	// Create factory that connects replicas to storage
	raftFactory := DefaultFactoryWithNode(cfg.Raft, raftNode)

	ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
	replica, err := raftFactory(ctx, storageName, logStore, logger, NewMetrics())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })

	replicaStorage := replica.raftEnabledStorage
	require.NotNil(t, replicaStorage, "replica should have a raft-enabled storage")

	gitalyStorage, err := raftNode.GetStorage(storageName)
	require.NoError(t, err)
	require.NotNil(t, gitalyStorage, "raft node should have a storage")

	raftNodeStorage, ok := gitalyStorage.(*RaftEnabledStorage)
	require.True(t, ok, "storage should be a RaftEnabledStorage")

	require.Same(t, raftNodeStorage, replicaStorage, "replica should point to correct storage")

	t.Run("replica connection is bidirectional", func(t *testing.T) {
		registry := raftNodeStorage.GetReplicaRegistry()
		require.NotNil(t, registry)

		partitionKey := NewPartitionKey(storageName, partitionID)

		registeredReplica, err := registry.GetReplica(partitionKey)
		require.NoError(t, err)

		// Verify bidirectional connection
		require.Same(t, replica, registeredReplica,
			"replica->storage->replica should return same instance")
	})

	t.Run("multiple replicas for same partition key", func(t *testing.T) {
		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		duplicateReplica, err := raftFactory(ctx, storageName, logStore, logger, NewMetrics())
		require.NoError(t, err)
		require.NotNil(t, duplicateReplica)
	})

	t.Run("Register different replicas for different partition keys", func(t *testing.T) {
		partitionID := storage.PartitionID(2)
		ctx = storage.ContextWithPartitionInfo(ctx, NewPartitionKey(storageName, partitionID), 1, "")
		replicaTwo, err := raftFactory(ctx, storageName, logStore, logger, NewMetrics())
		require.NoError(t, err)
		require.NotNil(t, replicaTwo)
	})
}

func TestReplica_AddNode(t *testing.T) {
	t.Parallel()

	createTestNode := func(t *testing.T, ctx context.Context, memberID uint64, partitionID storage.PartitionID, raftCfg config.Raft, metrics *Metrics, opts ...OptionFunc) (*Replica, string, *grpc.Server) {
		cfg := testcfg.Build(t)
		logger := testhelper.NewLogger(t)
		registry := NewReplicaRegistry()
		db, _ := dbSetup(t, ctx, cfg, testhelper.TempDir(t), cfg.Storages[0].Name, logger)
		routingTable := NewKVRoutingTable(db)

		transport := NewGrpcTransport(logger, cfg, routingTable, registry, nil)
		socketPath, srv := createTempServer(t, transport)
		cfg.SocketPath = socketPath

		address, err := cfg.GetAddressWithScheme()
		require.NoError(t, err)

		replica, err := createRaftReplica(t, ctx, memberID, socketPath, raftCfg, partitionID, metrics, opts...)
		require.NoError(t, err)

		partitionKey := NewPartitionKey(cfg.Storages[0].Name, partitionID)

		registry.RegisterReplica(partitionKey, replica)
		transport.registry = registry

		err = replica.Initialize(ctx, 0)
		require.NoError(t, err)

		return replica, address, srv
	}

	// waitForLeadership waits for the replica to become leader
	waitForLeadership := func(t *testing.T, replica *Replica, timeout time.Duration) {
		require.Eventually(t, func() bool {
			return replica.AppendedLSN() > 1 && replica.leadership.IsLeader()
		}, timeout, 5*time.Millisecond, "replica should become leader")
	}

	// waitForReplicaLeader waits for both replicas to have the same leader
	waitForReplicaLeader := func(t *testing.T, replicaOne, replicaTwo *Replica, timeout time.Duration) {
		require.Eventually(t, func() bool {
			return replicaTwo.leadership.GetLeaderID() == replicaOne.memberID
		}, timeout, 5*time.Millisecond, "replica two should have the same leader as replica one")
	}

	drainNotificationQueues := func(t *testing.T, replicas ...*Replica) {
		for _, replica := range replicas {
			require.Eventually(t, func() bool {
				select {
				case err := <-replica.GetNotificationQueue():
					if err != nil {
						require.NoError(t, err)
						return false
					}
					return true
				default:
					return false
				}
			}, waitTimeout, 10*time.Millisecond)
		}
	}

	t.Run("successful when node is leader", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, replicaAddress, srv := createTestNode(t, ctx, 1, partitionID, raftCfg, metrics)
		defer func() {
			srv.Stop()
			require.NoError(t, replica.Close())
		}()

		// Wait for the replica to elect itself as leader
		waitForLeadership(t, replica, waitTimeout)

		raftEnabledStorage := replica.raftEnabledStorage
		require.NotNil(t, raftEnabledStorage, "storage should be a RaftEnabledStorage")

		routingTable := raftEnabledStorage.GetRoutingTable()
		partitionKey := replica.partitionKey

		// verify the routing table is updated
		require.Eventually(t, func() bool {
			entry, err := routingTable.GetEntry(partitionKey)
			if err != nil {
				return false
			}
			leaderEntry := entry.Replicas[0]
			require.Equal(t, uint64(1), leaderEntry.GetMemberId())
			require.Equal(t, replicaAddress, leaderEntry.GetMetadata().GetAddress())
			require.Equal(t, replica.logStore.storageName, leaderEntry.GetStorageName())
			return len(entry.Replicas) == 1
		}, waitTimeout, 5*time.Millisecond, "routing table should be updated")

		// Create second node
		replicaTwo, replicaTwoAddress, srvTwo := createTestNode(t, ctx, 3, partitionID, raftCfg, metrics)
		defer func() {
			srvTwo.Stop()
			require.NoError(t, replicaTwo.Close())
		}()

		registryTwo := replicaTwo.raftEnabledStorage.GetReplicaRegistry()
		registryTwo.RegisterReplica(partitionKey, replicaTwo)

		routingTableTwo := replicaTwo.raftEnabledStorage.GetRoutingTable()
		err := routingTableTwo.UpsertEntry(RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{
				{
					PartitionKey: partitionKey,
					Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
					MemberId:     1,
					Metadata: &gitalypb.ReplicaID_Metadata{
						Address: replicaAddress,
					},
				},
			},
			Term:  1,
			Index: 2,
		})

		require.NoError(t, err)
		destination := "default"

		err = replica.AddNode(ctx, replicaTwoAddress, destination)
		require.NoError(t, err, "adding node should succeed when leader")

		require.Eventually(t, func() bool {
			entry, err := routingTable.GetEntry(partitionKey)
			if err != nil {
				return false
			}
			return slices.ContainsFunc(entry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMetadata().GetAddress() == replicaTwoAddress && r.GetType() == gitalypb.ReplicaID_REPLICA_TYPE_VOTER
			})
		}, waitTimeout, 5*time.Millisecond, "routing table should be updated")

		drainNotificationQueues(t, replica, replicaTwo)

		waitForReplicaLeader(t, replica, replicaTwo, waitTimeout)

		testhelper.RequirePromMetrics(t, metrics, `
			# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
			# TYPE gitaly_raft_log_entries_processed counter
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 3
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 3
			gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 1
			gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 1
			# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
			# TYPE gitaly_raft_proposal_queue_depth gauge
			gitaly_raft_proposal_queue_depth{storage="default"} 0
			# HELP gitaly_raft_membership_changes_total Counter of Raft membership changes by type
			# TYPE gitaly_raft_membership_changes_total counter
			gitaly_raft_membership_changes_total{change_type="add_voter"} 1
		`)
	})

	t.Run("concurrent add node proposals", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, replicaOneAddress, srv := createTestNode(t, ctx, 1, partitionID, raftCfg, metrics)
		defer func() {
			srv.Stop()
			require.NoError(t, replica.Close())
		}()

		waitForLeadership(t, replica, waitTimeout)

		raftEnabledStorage := replica.raftEnabledStorage
		require.NotNil(t, raftEnabledStorage, "storage should be a RaftEnabledStorage")

		routingTable := raftEnabledStorage.GetRoutingTable()
		partitionKey := replica.partitionKey

		var destinationAddresses []string
		var servers []*grpc.Server
		addressesToReplicas := make(map[string]*Replica)

		// Cleanup
		defer func() {
			for _, srv := range servers {
				srv.Stop()
			}
			for _, replica := range addressesToReplicas {
				require.NoError(t, replica.Close())
			}
		}()

		lastMemberID := uint64(3)

		for i := uint64(2); i <= lastMemberID; i++ {
			// create multiple replicas with new addresses
			replica, address, srv := createTestNode(t, ctx, i, partitionID, raftCfg, metrics)

			servers = append(servers, srv)
			destinationAddresses = append(destinationAddresses, address)
			addressesToReplicas[address] = replica

			// Register the replica so it can receive Raft messages from other nodes.
			// Without registration, the transport layer cannot route messages to this replica,
			// preventing it from learning about the current leader.
			replicaRegistry := replica.raftEnabledStorage.GetReplicaRegistry()
			replicaRegistry.RegisterReplica(partitionKey, replica)

			replicaRoutingTable := replica.raftEnabledStorage.GetRoutingTable()
			err := replicaRoutingTable.UpsertEntry(RoutingTableEntry{
				Replicas: []*gitalypb.ReplicaID{
					{
						PartitionKey: partitionKey,
						Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
						MemberId:     1,
						Metadata: &gitalypb.ReplicaID_Metadata{
							Address: replicaOneAddress,
						},
					},
				},
				Term:  1,
				Index: 2,
			})
			require.NoError(t, err)
		}

		destination := "default"

		// Propose concurrent membership changes. Same member ID but different address.
		var errs []error
		for i := 0; i < 2; i++ {
			err := replica.proposeMembershipChange(ctx, string(addVoter), destination, lastMemberID, ConfChangeAddNode, &gitalypb.ReplicaID_Metadata{
				Address: destinationAddresses[i],
			})
			errs = append(errs, err)
		}

		// only one of the proposals should succeed
		successfulProposals := 0
		for _, err := range errs {
			if err == nil {
				successfulProposals++
				continue
			}
			require.ErrorContains(t, err, "configuration change timed out after")
		}

		require.Equal(t, 1, successfulProposals, "only one of the proposals should succeed")

		var addedAddress string
		require.Eventually(t, func() bool {
			entry, err := routingTable.GetEntry(partitionKey)
			if err != nil {
				return false
			}

			// We should have exactly 2 replicas: the original (memberID=1) and the newly added one
			if len(entry.Replicas) != 2 {
				return false
			}

			idx := slices.IndexFunc(entry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetType() == gitalypb.ReplicaID_REPLICA_TYPE_VOTER && r.GetMemberId() != 1
			})

			if idx == -1 {
				return false
			}
			addedAddress = entry.Replicas[idx].GetMetadata().GetAddress()

			return true
		}, waitTimeout, 5*time.Millisecond, "routing table should be updated")

		addedReplica := addressesToReplicas[addedAddress]

		// Wait for voters count to stabilize between both replicas
		waitForReplicaLeader(t, replica, addedReplica, waitTimeout)

		drainNotificationQueues(t, replica, addedReplica)

		testhelper.RequirePromMetrics(t, metrics, `
			# HELP gitaly_raft_log_entries_processed Rate of log entries processed.
			# TYPE gitaly_raft_log_entries_processed counter
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="append",storage="default"} 3
			gitaly_raft_log_entries_processed{entry_type="config_change",operation="commit",storage="default"} 3
			gitaly_raft_log_entries_processed{entry_type="verify",operation="append",storage="default"} 1
			gitaly_raft_log_entries_processed{entry_type="verify",operation="commit",storage="default"} 1
			# HELP gitaly_raft_proposal_queue_depth Depth of proposal queue.
			# TYPE gitaly_raft_proposal_queue_depth gauge
			gitaly_raft_proposal_queue_depth{storage="default"} 0
			# HELP gitaly_raft_membership_changes_total Counter of Raft membership changes by type
			# TYPE gitaly_raft_membership_changes_total counter
			gitaly_raft_membership_changes_total{change_type="add_voter"} 2
		`,
			"gitaly_raft_log_entries_processed{entry_type=\"config_change\"}",
			"gitaly_raft_log_entries_processed{entry_type=\"config_change\"}",
			"gitaly_raft_membership_changes_total",
		)
	})

	t.Run("remove node that does not exist", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		// Create first node
		replica, _, srv := createTestNode(t, ctx, 1, partitionID, raftCfg, metrics)
		defer func() {
			srv.Stop()
			require.NoError(t, replica.Close())
		}()

		// Wait for the replica to elect itself as leader
		waitForLeadership(t, replica, waitTimeout)

		err := replica.RemoveNode(ctx, 999, storageName)
		require.EqualError(t, err, "translating member ID: no address found for memberID 999")
	})

	t.Run("fails when node is not leader", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		// Create first node
		replica, _, srv := createTestNode(t, ctx, 1, partitionID, raftCfg, metrics)
		defer func() {
			srv.Stop()
			require.NoError(t, replica.Close())
		}()

		// Set a random leader ID to simulate a non-leader
		replica.leadership.SetLeader(999, false)

		err := replica.AddNode(ctx, "gitaly-node-2:8075", storageName)
		require.EqualError(t, err, "replica is not the leader", "adding node should fail when not leader")

		testhelper.RequirePromMetrics(t, metrics, `
			# HELP gitaly_raft_membership_errors_total Counter of Raft membership operation errors
			# TYPE gitaly_raft_membership_errors_total counter
			gitaly_raft_membership_errors_total{change_type="add_voter",reason="not_leader"} 1
		`)
	})
}

func TestReplica_GetCurrentState(t *testing.T) {
	t.Parallel()

	t.Run("before initialization", func(t *testing.T) {
		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		// Create an uninitialized replica (node will be nil until Initialize is called)
		replica, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replica.Close())
		}()

		// Before initialization, node should be nil, term should be 0,
		// index should come from logStore, and state should be follower
		state := replica.GetCurrentState()
		require.NotNil(t, state, "state should not be nil")
		require.Equal(t, uint64(0), state.Term, "should return 0 term when node is nil")
		require.GreaterOrEqual(t, state.Index, uint64(0), "should return non-negative index from logStore")
		require.Equal(t, raft.StateFollower, state.State, "should return StateFollower when node is nil")
	})

	t.Run("after initialization", func(t *testing.T) {
		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replica.Close())
		}()

		require.NoError(t, replica.Initialize(ctx, 1))

		// Wait for replica to be ready and process initial entries
		waitTimeout := 10 * time.Second
		require.Eventually(t, func() bool {
			return replica.AppendedLSN() > 1
		}, waitTimeout, 5*time.Millisecond, "replica should process initial entries")

		state := replica.GetCurrentState()
		require.NotNil(t, state, "state should not be nil")
		require.Greater(t, state.Term, uint64(0), "should return positive term after initialization")
		require.Greater(t, state.Index, uint64(0), "should return positive index after initialization")

		// In a single-node cluster, the replica should become leader
		require.Equal(t, raft.StateLeader, state.State, "single-node replica should become leader")

		// Index should be equivalent to AppendedLSN
		require.Equal(t, uint64(replica.AppendedLSN()), state.Index, "state.Index should equal AppendedLSN")

		// Term should be consistent with what the Raft node reports
		require.Equal(t, replica.node.Status().Term, state.Term, "state.Term should match node status")
		require.Equal(t, replica.node.Status().RaftState, state.State, "state.State should match node status")
	})

	t.Run("consistency across multiple calls", func(t *testing.T) {
		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replica.Close())
		}()

		require.NoError(t, replica.Initialize(ctx, 1))

		// Wait for replica to be ready and process initial entries
		waitTimeout := 10 * time.Second
		require.Eventually(t, func() bool {
			return replica.AppendedLSN() > 1
		}, waitTimeout, 5*time.Millisecond, "replica should process initial entries")

		// Test multiple times to ensure consistency
		for i := 0; i < 5; i++ {
			state := replica.GetCurrentState()
			require.NotNil(t, state, "state should not be nil (iteration %d)", i)

			// Verify consistency with AppendedLSN and node status
			require.Equal(t, uint64(replica.AppendedLSN()), state.Index,
				"state.Index should equal AppendedLSN (iteration %d)", i)
			require.Equal(t, replica.node.Status().Term, state.Term,
				"state.Term should match node status (iteration %d)", i)
			require.Equal(t, replica.node.Status().RaftState, state.State,
				"state.State should match node status (iteration %d)", i)

			// Add some delay to allow for any background processing
			time.Sleep(10 * time.Millisecond)
		}
	})

	t.Run("advances after log entries", func(t *testing.T) {
		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replica.Close())
		}()

		require.NoError(t, replica.Initialize(ctx, 0))

		// Wait for leadership
		waitTimeout := 10 * time.Second
		require.Eventually(t, func() bool {
			return replica.AppendedLSN() > 1 && replica.leadership.IsLeader()
		}, waitTimeout, 5*time.Millisecond)

		initialState := replica.GetCurrentState()
		require.NotNil(t, initialState)

		// Append a log entry
		logEntryPath := createTestLogEntry(t, ctx, "test-content")
		_, err = replica.AppendLogEntry(logEntryPath)
		require.NoError(t, err)

		// Index should have advanced
		newState := replica.GetCurrentState()
		require.NotNil(t, newState)
		require.Greater(t, newState.Index, initialState.Index, "index should advance after appending entry")
		require.GreaterOrEqual(t, newState.Term, initialState.Term, "term should not decrease")
	})

	t.Run("concurrent access", func(t *testing.T) {
		ctx := testhelper.Context(t)
		raftCfg := raftConfigsForTest(t)
		metrics := NewMetrics()
		partitionID := storage.PartitionID(1)

		replica, err := createRaftReplica(t, ctx, 1, "", raftCfg, partitionID, metrics)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replica.Close())
		}()

		require.NoError(t, replica.Initialize(ctx, 1))

		// Wait for replica to be ready and process initial entries
		waitTimeout := 10 * time.Second
		require.Eventually(t, func() bool {
			return replica.AppendedLSN() > 1
		}, waitTimeout, 5*time.Millisecond)

		// Test concurrent access to GetCurrentState
		const numGoroutines = 100
		results := make([]*ReplicaState, numGoroutines)
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx] = replica.GetCurrentState()
			}(i)
		}

		wg.Wait()

		// All results should be consistent with current state
		for i, result := range results {
			require.NotNil(t, result, "result %d should not be nil", i)
			// Results should have consistent term and index (they may vary slightly due to timing)
			// But they should all be positive values for an active replica
			require.Greater(t, result.Term, uint64(0), "result %d term should be positive", i)
			require.Greater(t, result.Index, uint64(0), "result %d index should be positive", i)
			require.NotEqual(t, raft.StateType(0), result.State, "result %d state should be valid", i)
		}
	})

	t.Run("StateString method", func(t *testing.T) {
		testCases := []struct {
			state    raft.StateType
			expected string
		}{
			{raft.StateFollower, "follower"},
			{raft.StateCandidate, "candidate"},
			{raft.StateLeader, "leader"},
			{raft.StatePreCandidate, "precandidate"},
		}

		for _, tc := range testCases {
			t.Run(tc.expected, func(t *testing.T) {
				replicaState := &ReplicaState{
					Term:  1,
					Index: 1,
					State: tc.state,
				}
				result := StateString(replicaState.State)
				require.Equal(t, tc.expected, result)
			})
		}
	})
}
