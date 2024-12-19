package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestMigrationManager_Begin(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	disabledFn := func(context.Context) bool { return true }
	migrationErr := errors.New("migration error")

	errFn := func(context.Context, storage.Transaction) error { return migrationErr }
	recordingFn := func(id uint64) func(_ context.Context, txn storage.Transaction) error {
		return func(_ context.Context, txn storage.Transaction) error {
			return txn.KV().Set(uint64ToBytes(id), nil)
		}
	}

	migrationFn := func(id uint64) migration {
		return migration{
			id: id,
			fn: recordingFn(id),
		}
	}

	for _, tc := range []struct {
		desc                 string
		migrations           []migration
		startingMigration    *migration
		noRepository         bool
		expectedState        *migrationState
		expectedMigrationIDs map[uint64]struct{}
		expectedErr          error
	}{
		{
			desc:                 "no migrations configured",
			migrations:           nil,
			expectedState:        nil,
			expectedMigrationIDs: nil,
		},
		{
			desc:                 "repository does not exist",
			migrations:           []migration{migrationFn(1)},
			startingMigration:    nil,
			noRepository:         true,
			expectedState:        &migrationState{},
			expectedMigrationIDs: nil,
		},
		{
			desc:                 "no migration key in preexisting repository",
			migrations:           []migration{migrationFn(1), migrationFn(2)},
			startingMigration:    nil,
			noRepository:         false,
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{1: {}, 2: {}},
		},
		{
			desc:                 "no outstanding migrations",
			migrations:           []migration{migrationFn(1), migrationFn(2)},
			startingMigration:    &migration{id: 2},
			expectedState:        &migrationState{},
			expectedMigrationIDs: nil,
		},
		{
			desc:                 "single outstanding migration applied",
			migrations:           []migration{migrationFn(1), migrationFn(2)},
			startingMigration:    &migration{id: 1},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{2: {}},
		},
		{
			desc:                 "multiple outstanding migration applied",
			migrations:           []migration{migrationFn(1), migrationFn(2), migrationFn(3)},
			startingMigration:    &migration{id: 1},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{2: {}, 3: {}},
		},
		{
			desc:                 "disabled migration",
			migrations:           []migration{migrationFn(1), {id: 2, isDisabled: disabledFn, fn: recordingFn(2)}, migrationFn(3)},
			startingMigration:    &migration{id: 0},
			expectedState:        &migrationState{},
			expectedMigrationIDs: map[uint64]struct{}{1: {}},
		},
		{
			desc:              "error returned during migrations",
			migrations:        []migration{migrationFn(1), {id: 2, fn: errFn}, migrationFn(3)},
			startingMigration: &migration{id: 1},
			expectedState: &migrationState{
				err: migrationErr,
			},
			expectedMigrationIDs: nil,
			expectedErr:          migrationErr,
		},
		{
			desc:              "starting migration key invalid",
			migrations:        []migration{migrationFn(1), migrationFn(2), migrationFn(3)},
			startingMigration: &migration{id: 4},
			expectedState: &migrationState{
				err: errors.New("repository has invalid migration key: 4"),
			},
			expectedMigrationIDs: nil,
			expectedErr:          errors.New("repository has invalid migration key: 4"),
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
			database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			storageName := cfg.Storages[0].Name
			storagePath := cfg.Storages[0].Path

			stateDir := filepath.Join(storagePath, "state")
			require.NoError(t, os.MkdirAll(stateDir, mode.Directory))

			stagingDir := filepath.Join(storagePath, "staging")
			require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

			cmdFactory := gittest.NewCommandFactory(t, cfg)
			cache := catfile.NewCache(cfg)
			defer cache.Stop()

			repositoryFactory, err := localrepo.NewFactory(
				logger, config.NewLocator(cfg), cmdFactory, cache,
			).ScopeByStorage(ctx, cfg.Storages[0].Name)
			require.NoError(t, err)

			m := partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)).Scope(storageName)

			logManager := log.NewManager(storageName, testPartitionID, stagingDir, stateDir, nil)
			tm := partition.NewTransactionManager(
				testPartitionID,
				logger,
				database,
				storageName,
				storagePath,
				stateDir,
				stagingDir,
				cmdFactory,
				repositoryFactory,
				m,
				logManager,
			)

			mm := migrationManager{
				Partition:       tm,
				logger:          logger,
				migrations:      tc.migrations,
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

				require.NoError(t, txn.KV().Set(migrationKey(relativePath), uint64ToBytes(tc.startingMigration.id)))
				require.NoError(t, txn.Commit(ctx))
			}

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
					_, expected := tc.expectedMigrationIDs[m.id]
					if _, err := txn.KV().Get(uint64ToBytes(m.id)); err != nil {
						require.ErrorIs(t, err, badger.ErrKeyNotFound)
						require.False(t, expected)
					} else {
						require.NoError(t, err)
						require.True(t, expected)
					}
				}

				require.NoError(t, txn.Commit(ctx))
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

			tm.Close()
			require.NoError(t, tm.CloseSnapshots())
			require.NoError(t, <-managerErr)
		})
	}
}

func TestMigrationManager_Concurrent(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	noopFn := func(context.Context, storage.Transaction) error { return nil }

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

			// In this test, the configured migrations are never executed because the repository
			// does not exist in the snapshot. The migration is configured only to trigger the
			// migration manager to block concurrent transactions.
			mm := migrationManager{
				Partition:       mockPartition,
				logger:          testhelper.NewLogger(t),
				migrations:      []migration{{id: 1, fn: noopFn}},
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

type mockPartition struct {
	storagemgr.Partition
	beginFn func(context.Context, storage.BeginOptions) (storage.Transaction, error)
}

func (m mockPartition) Begin(ctx context.Context, opts storage.BeginOptions) (storage.Transaction, error) {
	return m.beginFn(ctx, opts)
}
