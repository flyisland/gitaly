package raftmgr

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestRaftSnapshotter_materializeSnapshot(t *testing.T) {
	t.Parallel()

	if !testhelper.IsWALEnabled() {
		t.Skip(`Transactions must be enabled for raft snapshots to work.`)
	}

	// Setup partition with transaction enabled
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Raft: config.Raft{
			SnapshotDir: testhelper.TempDir(t),
		},
	}))
	logger := testhelper.NewLogger(t)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	// Setup factories
	cmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)
	localRepoFactory := localrepo.NewFactory(logger, locator, cmdFactory, catfileCache)
	partitionFactory := partition.NewFactory(cmdFactory, localRepoFactory, partition.NewMetrics(nil), nil)

	storageName := cfg.Storages[0].Name
	storagePath := cfg.Storages[0].Path

	// Setup db mgr and storage mgr
	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	defer dbMgr.Close()
	storageMgr, err := storagemgr.NewStorageManager(
		logger,
		cfg.Storages[0].Name,
		cfg.Storages[0].Path,
		dbMgr,
		partitionFactory,
		1,
		storagemgr.NewMetrics(cfg.Prometheus),
	)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Retrieve partition from storage
	p, err := storageMgr.GetPartition(ctx, storage.PartitionID(1))
	require.NoError(t, err)

	// Create repo so partition is not empty
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Storage:                config.Storage{Name: storageName, Path: storagePath},
	})

	metrics := NewMetrics().Scope("default")
	// Setup snapshotter
	s, err := NewRaftSnapshotter(cfg.Raft, logger, metrics)
	require.NoError(t, err)

	// Begin transaction on partition
	txn, err := p.Begin(ctx, storage.BeginOptions{
		RelativePaths: []string{repo.GetRelativePath()},
	})
	require.NoError(t, err)

	// Package partition's disk into snapshot
	got, err := s.materializeSnapshot(SnapshotMetadata{
		index:       10,
		term:        10,
		partitionID: storage.PartitionID(1),
	}, txn)
	require.NoError(t, err)

	// Get path to compressed tar file
	tarPath := got.file.Name()
	tar, err := os.Open(tarPath)
	require.NoError(t, err)
	defer tar.Close()

	require.NoError(t, txn.Commit(ctx))

	// Tar should contain a directory with repository
	testhelper.ContainsTarState(t, bufio.NewReader(tar), testhelper.DirectoryState{
		filepath.Join("fs", repo.GetRelativePath()):                    {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "HEAD"):            {Mode: archive.TarFileMode, Content: []byte("ref: refs/heads/main\n")},
		filepath.Join("fs", repo.GetRelativePath(), "objects"):         {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "objects", "info"): {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "objects", "pack"): {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "refs"):            {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "refs", "heads"):   {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "refs", "tags"):    {Mode: archive.DirectoryMode},
		"kv-state": {Mode: archive.TarFileMode, Content: []byte{}},
	})
}

func TestRaftStorage_TriggerSnapshot(t *testing.T) {
	t.Parallel()

	if !testhelper.IsWALEnabled() {
		t.Skip(`Transactions must be enabled for raft snapshots to work.`)
	}

	ctx := testhelper.Context(t)
	t.Run("reject snapshot creation if no transaction found in context", func(t *testing.T) {
		// Create a mock Storage and mock RaftSnapshotter
		mockSnapshotter := &MockRaftSnapshotter{
			Mutex:     sync.Mutex{},
			callCount: 0,
		}
		raftStorage := &Storage{
			snapshotter: mockSnapshotter,
		}
		// No transaction created in this context
		snapshot, err := raftStorage.TriggerSnapshot(ctx, storage.LSN(1), 10)
		require.ErrorContains(t, err, "transaction not initialized")
		require.Nil(t, snapshot)
	})

	t.Run("concurrent invocations result in sequential snapshot creation", func(t *testing.T) {
		mockSnapshotter := &MockRaftSnapshotter{
			Mutex:     sync.Mutex{},
			callCount: 0,
		}
		raftStorage := &Storage{
			snapshotter: mockSnapshotter,
		}

		concurrentCalls := 5
		var wg sync.WaitGroup
		wg.Add(concurrentCalls)

		type result struct {
			snapshot *Snapshot
			seqNum   int
		}

		results := make(chan result, concurrentCalls)
		errCh := make(chan error, concurrentCalls)

		orderCh := make(chan struct{}, 1)
		orderCh <- struct{}{}
		// Simulate concurrent calls
		for i := 0; i < concurrentCalls; i++ {
			<-orderCh
			go func(seqNum int) {
				defer wg.Done()

				tx := &mockTransaction{}
				ctx = storage.ContextWithTransaction(ctx, tx)
				snapshot, err := raftStorage.TriggerSnapshot(ctx, tx.SnapshotLSN(), 10)

				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
				default:
					if err != nil {
						errCh <- err
					} else if snapshot != nil {
						results <- result{snapshot: snapshot, seqNum: seqNum}
					}
				}
				orderCh <- struct{}{}
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(results)
		close(errCh)
		close(orderCh)

		// Check no errors
		for err := range errCh {
			require.NoError(t, err)
		}
		// Collect snapshots
		var snapshots []result
		for snapshot := range results {
			snapshots = append(snapshots, snapshot)
		}

		require.Equal(t, concurrentCalls, mockSnapshotter.callCount, "Expected 5 call counts")
		require.Len(t, snapshots, concurrentCalls, "Expected 5 snapshots to be created")

		for i, s := range snapshots {
			require.Equal(t, i, s.seqNum, "Snapshot created out of order")
		}
	})
}

// Mocks
type MockRaftSnapshotter struct {
	sync.Mutex
	callCount int
}

// Mock materializeSnapshot
func (m *MockRaftSnapshotter) materializeSnapshot(snapshotMetadata SnapshotMetadata, tx storage.Transaction) (*Snapshot, error) {
	m.callCount++
	return &Snapshot{
		file:     &os.File{},
		metadata: snapshotMetadata,
	}, nil
}

type mockTransaction struct{ storage.Transaction }

// Mock SnapshotLSN
func (*mockTransaction) SnapshotLSN() storage.LSN {
	return storage.LSN(1)
}
