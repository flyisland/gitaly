package reftable

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type mockMigrationHandler struct {
	ch <-chan struct{}
}

func (m *mockMigrationHandler) Migrate(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error {
	if m.ch != nil {
		<-m.ch
		<-m.ch
	}

	return nil
}

func TestMigrator_RegisterMigration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		state        migratorState
		expectedData bool
	}{
		{
			desc: "migration already completed",
			state: migratorState{
				completed: true,
			},
		},
		{
			desc: "migration in cooldown period",
			state: migratorState{
				coolDown: time.Now().Add(1 * time.Hour),
			},
		},
		{
			desc: "migration with expired cooldown period",
			state: migratorState{
				coolDown: time.Now().Add(-1 * time.Hour),
			},
			expectedData: true,
		},
		{
			desc:         "migration state doesn't exist",
			expectedData: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &migrator{
				migrateCh: make(chan migrationData),
				state:     sync.Map{},
				ctx:       parentCtx,
				ctxCancel: parentCancel,
			}

			var wg sync.WaitGroup
			defer wg.Wait()

			stopCh := make(chan struct{})

			// We raise multiple gorountines, only one would go through.
			// The others would go to the default case.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					default:
						wg.Add(1)
						go func() {
							defer wg.Done()
							m.RegisterMigration("foo", "bar")
						}()
					}
				}
			}()

			defer func() {
				close(stopCh)
			}()

			if tc.expectedData {
				data := <-m.migrateCh

				require.Equal(t, "foo", data.storageName)
				require.Equal(t, "bar", data.relativePath)
			} else {
				select {
				case <-m.migrateCh:
					t.Fatal("unexpected migration data")
				default:
					// Expected: channel should be empty
				}
			}
		})
	}
}

func TestMigrator_CancelMigration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		data        *migrationData
		expectedErr error
	}{
		{
			desc: "current migration not-set",
			data: &migrationData{},
		},
		{
			desc: "wrong relative path",
			data: &migrationData{
				storageName:  "foo",
				relativePath: "buzz",
			},
		},
		{
			desc: "wrong storage name",
			data: &migrationData{
				storageName:  "buzz",
				relativePath: "bar",
			},
		},
		{
			desc: "success path",
			data: &migrationData{
				storageName:  "foo",
				relativePath: "bar",
			},
			expectedErr: context.Canceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &migrator{
				state:     sync.Map{},
				ctx:       parentCtx,
				ctxCancel: parentCancel,
			}
			m.state.Store(migrationKey(tc.data.storageName, tc.data.relativePath), migratorState{cancelCtx: cancel})

			m.CancelMigration("foo", "bar")
			require.ErrorIs(t, ctx.Err(), tc.expectedErr)
		})
	}
}

func TestMigrator(t *testing.T) {
	t.Parallel()

	testhelper.SkipWithRaft(t, "specifically test interaction directly with the transaction manager")

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	metrics := NewMetrics()

	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)
	require.NoError(t, err)
	defer dbMgr.Close()

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	localRepoFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, catfileCache)

	partitionFactory := partition.NewFactory(cmdFactory, localRepoFactory, partition.NewMetrics(nil), nil, cfg.Raft, nil)

	ptnMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(
		logger, dbMgr, partitionFactory, storagemgr.DefaultMaxInactivePartitions, storagemgr.NewMetrics(cfg.Prometheus),
	))
	require.NoError(t, err)
	defer ptnMgr.Close()

	for _, tc := range []struct {
		desc           string
		setup          func() (func(m *migrator, repo *gitalypb.Repository), migrationHandler)
		completed      bool
		expectedLogMsg string
	}{
		{
			desc: "cancelled migration",
			setup: func() (func(m *migrator, repo *gitalypb.Repository), migrationHandler) {
				ch := make(chan struct{})

				return func(m *migrator, repo *gitalypb.Repository) {
					ch <- struct{}{}
					m.CancelMigration(cfg.Storages[0].Name, repo.GetRelativePath())
					ch <- struct{}{}
				}, &mockMigrationHandler{ch: ch}
			},
			completed:      false,
			expectedLogMsg: "migration failed for repository",
		},
		{
			desc: "successful migration",
			setup: func() (func(m *migrator, repo *gitalypb.Repository), migrationHandler) {
				return func(m *migrator, repo *gitalypb.Repository) {}, &mockMigrationHandler{}
			},
			completed:      true,
			expectedLogMsg: "migration successful for repository",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			run, migrationHandler := tc.setup()

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &migrator{
				wg:               sync.WaitGroup{},
				migrateCh:        make(chan migrationData),
				logger:           logger,
				metrics:          metrics,
				node:             ptnMgr,
				state:            sync.Map{},
				migrationHandler: migrationHandler,
				ctx:              parentCtx,
				ctxCancel:        parentCancel,
			}

			storageName := cfg.Storages[0].Name

			repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			m.Run()
			defer m.Close()

			// It is not guaranteed that the migration is registered, so run it in a
			// loop until it is.
			for {
				if _, ok := m.state.Load(migrationKey(storageName, repo.GetRelativePath())); ok {
					break
				}

				m.RegisterMigration(storageName, repo.GetRelativePath())
			}

			run(m, repo)

			// Block till the old migration is complete.
			m.migrateCh <- migrationData{}

			val, ok := m.state.Load(migrationKey(storageName, repo.GetRelativePath()))
			state := val.(migratorState)

			require.True(t, ok)
			require.Equal(t, tc.completed, state.completed)
			require.Equal(t, uint(1), state.attempts)

			entries := hook.AllEntries()
			require.Equal(t, tc.expectedLogMsg, entries[len(entries)-1].Message)
		})
	}
}
