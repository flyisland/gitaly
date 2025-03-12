package raftmgr

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/fsrecorder"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protodelim"
)

func TestRaftSnapshotter_materializeSnapshot(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Raft: config.Raft{
			SnapshotDir: testhelper.TempDir(t),
		},
	}))
	logger := testhelper.NewLogger(t)
	storageName := cfg.Storages[0].Name
	storagePath := cfg.Storages[0].Path

	// Create repo so partition is not empty
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Storage:                config.Storage{Name: storageName, Path: storagePath},
	})

	// Add some KV entries to the DB so that the collected DB is not empty.
	db := getTestDBManager(t, ctx, cfg, logger)
	dbTxn := db.NewTransaction(true)
	require.NoError(t, dbTxn.Set([]byte("hello"), []byte("world")))
	require.NoError(t, dbTxn.Commit())

	// Setup a proper snapshot with partition snapshot manager
	snapshotManager, err := snapshot.NewManager(
		logger,
		storagePath,
		testhelper.TempDir(t),
		snapshot.NewMetrics().Scope(storageName),
	)
	require.NoError(t, err)
	defer testhelper.MustClose(t, snapshotManager)

	// Create a new snapshot of the partition
	snapshot, err := snapshotManager.GetSnapshot(ctx, []string{repo.GetRelativePath()}, false)
	require.NoError(t, err)
	defer testhelper.MustClose(t, snapshot)

	// Create a mock transaction which depends on the newly created snapshot.
	txn := &mockTransaction{
		db: db,
		fs: fsrecorder.NewFS(snapshot.Root(), wal.NewEntry(testhelper.TempDir(t))),
	}

	// Setup snapshotter
	s, err := NewRaftSnapshotter(cfg.Raft, logger, NewMetrics().Scope("default"))
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

	// This commit is a no-op
	require.NoError(t, txn.Commit(ctx))

	// Tar should contain a directory with repository
	directoryState := testhelper.DirectoryState{
		filepath.Join("fs", repo.GetRelativePath()):                    {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "objects"):         {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "objects", "info"): {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "objects", "pack"): {Mode: archive.DirectoryMode},
		filepath.Join("fs", repo.GetRelativePath(), "config"): {
			Mode:    archive.TarFileMode,
			Content: "config content",
			ParseContent: func(tb testing.TB, path string, content []byte) any {
				require.Equal(t, filepath.Join("fs", repo.GetRelativePath(), "config"), path)
				return "config content"
			},
		},
		filepath.Join("fs", repo.GetRelativePath(), "refs"): {Mode: archive.DirectoryMode},
		"kv-state": {Mode: archive.TarFileMode, ParseContent: func(tb testing.TB, path string, content []byte) any {
			var keyPair gitalypb.KVPair
			require.NoError(t, protodelim.UnmarshalFrom(bytes.NewReader(content), &keyPair))

			testhelper.ProtoEqual(t, &gitalypb.KVPair{
				Key:   []byte("hello"),
				Value: []byte("world"),
			}, &keyPair)
			return nil
		}},
	}
	if testhelper.IsReftableEnabled() {
		directoryState[filepath.Join("fs", repo.GetRelativePath(), "refs", "heads")] = testhelper.DirectoryEntry{
			Mode:    archive.TarFileMode,
			Content: []byte("this repository uses the reftable format\n"),
		}
	} else {
		directoryState[filepath.Join("fs", repo.GetRelativePath(), "HEAD")] = testhelper.DirectoryEntry{
			Mode: archive.TarFileMode, Content: []byte("ref: refs/heads/main\n"),
		}
		directoryState[filepath.Join("fs", repo.GetRelativePath(), "refs", "heads")] = testhelper.DirectoryEntry{
			Mode: archive.DirectoryMode,
		}
		directoryState[filepath.Join("fs", repo.GetRelativePath(), "refs", "tags")] = testhelper.DirectoryEntry{
			Mode: archive.DirectoryMode,
		}
	}
	testhelper.ContainsTarState(t, bufio.NewReader(tar), directoryState)
}

func TestRaftStorage_TriggerSnapshot(t *testing.T) {
	t.Parallel()

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

type mockTransaction struct {
	storage.Transaction
	db keyvalue.Transactioner
	fs fsrecorder.FS
}

func (*mockTransaction) SnapshotLSN() storage.LSN {
	return storage.LSN(1)
}

func (m *mockTransaction) KV() keyvalue.ReadWriter {
	return m.db.NewTransaction(true)
}

func (m *mockTransaction) FS() storage.FS {
	return m.fs
}

func (m *mockTransaction) Commit(context.Context) error {
	// No-Op
	return nil
}
