package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"google.golang.org/grpc/metadata"
)

func TestMigrationManager_Begin(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	disabledFn := func(context.Context) bool { return true }
	migrationErr := errors.New("migration error")
	md := metadata.MD{"foo": []string{"bar"}}

	errFn := func(context.Context, storage.Transaction, string, string) error { return migrationErr }
	recordingFn := func(id uint64) func(context.Context, storage.Transaction, string, string) error {
		return func(ctx context.Context, txn storage.Transaction, _ string, _ string) error {
			// Ensure that the context is carrying over metadata
			// from the request's context.
			actualMD, _ := metadata.FromIncomingContext(ctx)
			assert.Equal(t, md, actualMD)

			return txn.KV().Set(uint64ToBytes(id), nil)
		}
	}

	migrationFn := func(id uint64) Migration {
		return Migration{
			ID: id,
			Fn: recordingFn(id),
		}
	}

	for _, tc := range []struct {
		desc                 string
		migrations           []Migration
		startingMigration    *Migration
		noRepository         bool
		expectedState        *migrationState
		expectedMigrationIDs map[uint64]struct{}
		expectedErr          error
		expectedLastID       uint64
	}{
		{
			desc:                 "no migrations configured",
			migrations:           nil,
			expectedState:        nil,
			expectedMigrationIDs: nil,
		},
		{
			desc:                 "repository does not exist",
			migrations:           []Migration{migrationFn(1)},
			startingMigration:    nil,
			noRepository:         true,
			expectedState:        &migrationState{},
			expectedMigrationIDs: nil,
		},
		{
			desc:                 "no migration key in preexisting repository",
			migrations:           []Migration{migrationFn(1), migrationFn(2)},
			startingMigration:    nil,
			noRepository:         false,
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{1: {}, 2: {}},
			expectedLastID:       2,
		},
		{
			desc:                 "no outstanding migrations",
			migrations:           []Migration{migrationFn(1), migrationFn(2)},
			startingMigration:    &Migration{ID: 2},
			expectedState:        &migrationState{},
			expectedMigrationIDs: nil,
			expectedLastID:       2,
		},
		{
			desc:                 "single outstanding migration applied",
			migrations:           []Migration{migrationFn(1), migrationFn(2)},
			startingMigration:    &Migration{ID: 1},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{2: {}},
			expectedLastID:       2,
		},
		{
			desc:                 "multiple outstanding migration applied",
			migrations:           []Migration{migrationFn(1), migrationFn(2), migrationFn(3)},
			startingMigration:    &Migration{ID: 1},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{2: {}, 3: {}},
			expectedLastID:       3,
		},
		{
			desc:                 "disabled migration",
			migrations:           []Migration{migrationFn(1), {ID: 2, IsDisabled: disabledFn, Fn: recordingFn(2)}, migrationFn(3)},
			startingMigration:    &Migration{ID: 0},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{1: {}},
			expectedLastID:       1,
		},
		{
			desc:              "error returned during migrations",
			migrations:        []Migration{migrationFn(1), {ID: 2, Fn: errFn}, migrationFn(3)},
			startingMigration: &Migration{ID: 1},
			expectedState: &migrationState{
				err: migrationErr,
			},
			expectedMigrationIDs: nil,
			expectedErr:          migrationErr,
			expectedLastID:       1,
		},
		{
			desc:              "starting migration key invalid",
			migrations:        []Migration{migrationFn(1), migrationFn(2), migrationFn(3)},
			startingMigration: &Migration{ID: 4},
			expectedState: &migrationState{
				err: errors.New("repository has invalid migration key: 4"),
			},
			expectedMigrationIDs: nil,
			expectedErr:          errors.New("repository has invalid migration key: 4"),
			expectedLastID:       4,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			cfg := testcfg.Build(t)

			repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			relativePath := repo.GetRelativePath()
			if tc.noRepository {
				relativePath = "does-not-exist"
			}

			testPartitionID := storage.PartitionID(1)
			logger := testhelper.NewLogger(t)

			storageName := cfg.Storages[0].Name
			storagePath := cfg.Storages[0].Path

			dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			database, err := dbMgr.GetDB(storageName)
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			stateDir := filepath.Join(storagePath, "state")
			require.NoError(t, os.MkdirAll(stateDir, mode.Directory))

			stagingDir := filepath.Join(storagePath, "staging")
			require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

			cmdFactory := gittest.NewCommandFactory(t, cfg)
			cache := catfile.NewCache(cfg)
			defer cache.Stop()

			repositoryFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, cache)

			m := partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))
			raftNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
			require.NoError(t, err)

			raftFactory := raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)

			partitionFactoryOptions := []partition.FactoryOption{
				partition.WithCmdFactory(cmdFactory),
				partition.WithRepoFactory(repositoryFactory),
				partition.WithMetrics(m),
				partition.WithRaftConfig(cfg.Raft),
				partition.WithRaftFactory(raftFactory),
			}
			factory := partition.NewFactory(partitionFactoryOptions...)
			tm := factory.New(ctx, logger, testPartitionID, database, storageName, storagePath, stateDir, stagingDir)

			ctx, cancel := context.WithCancel(ctx)

			mm := migrationManager{
				ctx:             ctx,
				cancelFn:        cancel,
				Partition:       tm,
				logger:          logger,
				migrations:      &tc.migrations,
				metrics:         NewMetrics(),
				migrationStates: map[string]*migrationState{},
			}

			managerErr := make(chan error)
			go func() {
				managerErr <- tm.Run()
			}()

			if tc.startingMigration != nil {
				txn, err := tm.Begin(ctx, storage.BeginOptions{
					Write:         true,
					RelativePaths: []string{relativePath},
				})
				require.NoError(t, err)

				require.NoError(t, txn.KV().Set(migrationKey(relativePath), uint64ToBytes(tc.startingMigration.ID)))
				_, err = txn.Commit(ctx)
				require.NoError(t, err)
			}

			ctx = metadata.NewIncomingContext(ctx, md)

			// Begin and commit transaction through the migration manager to exercise the migration logic.
			if txn, err := mm.Begin(ctx, storage.BeginOptions{
				Write:         false,
				RelativePaths: []string{relativePath},
			}); err != nil {
				require.ErrorContains(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)

				// In this test, each executed migration records its ID in the KV store. Validate
				// that the expected migrations were performed.
				for _, m := range tc.migrations {
					_, expected := tc.expectedMigrationIDs[m.ID]
					if _, err := txn.KV().Get(uint64ToBytes(m.ID)); err != nil {
						require.ErrorIs(t, err, badger.ErrKeyNotFound)
						require.False(t, expected)
					} else {
						require.NoError(t, err)
						require.True(t, expected)
					}
				}

				_, err := txn.Commit(ctx)
				require.NoError(t, err)
			}

			if state, ok := mm.migrationStates[relativePath]; ok {
				require.NotNil(t, tc.expectedState)
				if tc.expectedState.err != nil {
					require.ErrorContains(t, state.err, tc.expectedState.err.Error())
				} else {
					require.NoError(t, state.err)
				}
			} else {
				require.Nil(t, tc.expectedState)
				require.Empty(t, mm.migrationStates)
			}

			id, err := mm.getLastMigrationID(ctx, repo.GetRelativePath())
			require.NoError(t, err)
			require.Equal(t, tc.expectedLastID, id)

			tm.Close()
			require.NoError(t, tm.CloseSnapshots())
			require.NoError(t, <-managerErr)
		})
	}
}

func TestMigrationManager_Concurrent(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	noopFn := func(context.Context, storage.Transaction, string, string) error { return nil }

	setupMockPartition := func(firstTransactionFn func(context.Context) error) *mockPartition {
		kvFn := func() keyvalue.ReadWriter {
			return &mockReadWriter{
				getFn: func(key []byte) (keyvalue.Item, error) {
					return &mockItem{
						valueFn: func(fn func(value []byte) error) error {
							return fn(uint64ToBytes(0))
						},
					}, nil
				},
			}
		}

		firstTransaction := true
		return &mockPartition{
			beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
				if firstTransaction {
					firstTransaction = false
					return &mockTransaction{
						kvFn:     kvFn,
						commitFn: firstTransactionFn,
					}, nil
				}
				return &mockTransaction{kvFn: kvFn}, nil
			},
		}
	}

	for _, tc := range []struct {
		desc            string
		samePath        bool
		expectedBlocked bool
		expectedErr     error
	}{
		{
			desc:            "same repo concurrent transaction blocked",
			samePath:        true,
			expectedBlocked: true,
		},
		{
			desc:            "different repo concurrent transaction not blocked",
			samePath:        false,
			expectedBlocked: false,
		},
		{
			desc:            "failed migration propagated to concurrent transaction",
			samePath:        true,
			expectedBlocked: true,
			expectedErr:     errors.New("migration failed"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			path1, path2 := "foo", "bar"
			if tc.samePath {
				path1 = path2
			}

			// The underlying transaction manager is mocked to provide a means to control when the
			// first transaction gets committed. In this test, the first transaction that gets
			// opened happens as part of the migration. Blocking here allows validation of
			// concurrent transactions to ensure they are being blocked when required.
			firstTransactionStarted := make(chan struct{})
			firstTransactionBlocked := make(chan struct{})
			mockPartition := setupMockPartition(func(ctx context.Context) error {
				close(firstTransactionStarted)
				<-firstTransactionBlocked
				return tc.expectedErr
			})

			ctx, cancel := context.WithCancel(ctx)

			// In this test, the configured migrations are never executed because the repository
			// does not exist in the snapshot. The migration is configured only to trigger the
			// migration manager to block concurrent transactions.
			mm := migrationManager{
				ctx:             ctx,
				cancelFn:        cancel,
				Partition:       mockPartition,
				logger:          testhelper.NewLogger(t),
				metrics:         NewMetrics(),
				migrations:      &[]Migration{{ID: 1, Fn: noopFn}},
				migrationStates: map[string]*migrationState{},
			}

			// Start a transaction that triggers a migration. The mocks are set up in a such a way
			// that migration manager determines no migrations need to be performed.
			errCh1 := make(chan error)
			go func() {
				_, err := mm.Begin(ctx, storage.BeginOptions{
					RelativePaths: []string{path1},
				})
				errCh1 <- err
			}()

			// Start a second transaction after the migration has started in the first.
			errCh2 := make(chan error)
			go func() {
				<-firstTransactionStarted
				_, err := mm.Begin(ctx, storage.BeginOptions{
					RelativePaths: []string{path2},
				})

				// When a concurrent transaction is started with the same relative path, it is
				// expected to be blocked until the migration has completed. If the concurrent
				// transaction is started with a different relative path it can proceed without
				// being blocked.
				select {
				case <-firstTransactionBlocked:
					if !tc.expectedBlocked {
						require.Fail(t, "transaction was not blocked")
					}
				default:
					if tc.expectedBlocked {
						require.Fail(t, "transaction was not blocked")
					}
				}
				errCh2 <- err
			}()

			// Wait a small amount of time before releasing the first transaction to ensure
			// concurrent transaction against the same repository are blocked and concurrent
			// transactions against a different repository are not blocked.
			time.Sleep(time.Second)
			close(firstTransactionBlocked)

			if tc.expectedErr != nil {
				// If the migration returns an error, it is expected that the error message be
				// propagated to the blocked concurrent transactions.
				require.ErrorIs(t, <-errCh1, tc.expectedErr)
				require.ErrorIs(t, <-errCh2, tc.expectedErr)
			} else {
				// If the migration succeeds, blocked concurrent transaction are expected to proceed
				// without error.
				require.NoError(t, <-errCh1)
				require.NoError(t, <-errCh2)
			}
		})
	}
}

func TestMigrationManager_Context(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	requestCtx, requestCancel := context.WithCancel(ctx)

	var mm *migrationManager
	called := false
	mm = newPartition(
		mockPartition{
			beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
				return mockTransaction{
					kvFn: func() keyvalue.ReadWriter {
						return &mockReadWriter{
							getFn: func(key []byte) (keyvalue.Item, error) {
								return &mockItem{
									valueFn: func(fn func(value []byte) error) error {
										return fn(uint64ToBytes(0))
									},
								}, nil
							},
						}
					},
				}, nil
			},
			closeFn: func() {},
		},
		testhelper.NewLogger(t),
		NewMetrics(),
		"sample-storage",
		&[]Migration{{ID: 1, Fn: func(ctx context.Context, tx storage.Transaction, _ string, _ string) error {
			// Canceling the context of the request that started this migraiton
			// should not lead to canceling the migration.
			requestCancel()
			require.NoError(t, ctx.Err())

			mm.Close()
			require.Equal(t, context.Canceled, ctx.Err())
			called = true
			return nil
		}}},
	).(*migrationManager)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	relativePath := repo.GetRelativePath()

	_, err := mm.Begin(requestCtx, storage.BeginOptions{
		Write:         true,
		RelativePaths: []string{relativePath},
	})

	require.NoError(t, err)
	require.True(t, called)
}

type mockPartition struct {
	storagemgr.Partition
	beginFn func(context.Context, storage.BeginOptions) (storage.Transaction, error)
	closeFn func()
	runFn   func() error
}

func (m mockPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	return m.beginFn(ctx, opts)
}

func (m mockPartition) Close() {
	m.closeFn()
}

func (m mockPartition) Run() error {
	return m.runFn()
}
