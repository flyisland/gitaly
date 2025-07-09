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
	ch  <-chan struct{}
	err error
}

func (m *mockMigrationHandler) Migrate(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error {
	if m.ch != nil {
		<-m.ch
		<-m.ch
	}

	if m.err != nil {
		return m.err
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

	partitionFactoryOptions := []partition.FactoryOption{
		partition.WithCmdFactory(cmdFactory),
		partition.WithRepoFactory(localRepoFactory),
		partition.WithMetrics(partition.NewMetrics(nil)),
		partition.WithRaftConfig(cfg.Raft),
	}

	partitionFactory := partition.NewFactory(partitionFactoryOptions...)

	ptnMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(
		logger, dbMgr, partitionFactory, config.DefaultMaxInactivePartitions, storagemgr.NewMetrics(cfg.Prometheus),
	))
	require.NoError(t, err)
	defer ptnMgr.Close()

	type setupData struct {
		run              func(m *migrator, repo *gitalypb.Repository)
		migrationHandler migrationHandler
		repo             *gitalypb.Repository
	}

	for _, tc := range []struct {
		desc           string
		setup          func() setupData
		completed      bool
		attempts       uint
		expectedLogMsg string
	}{
		{
			desc: "cancelled migration",
			setup: func() setupData {
				ch := make(chan struct{})

				repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				return setupData{
					run: func(m *migrator, repo *gitalypb.Repository) {
						ch <- struct{}{}
						m.CancelMigration(cfg.Storages[0].Name, repo.GetRelativePath())
						ch <- struct{}{}
					},
					migrationHandler: &mockMigrationHandler{ch: ch},
					repo:             repo,
				}
			},
			completed:      false,
			attempts:       1,
			expectedLogMsg: "migration failed for repository",
		},
		{
			desc: "repository not found error",
			setup: func() setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				return setupData{
					run:              func(m *migrator, repo *gitalypb.Repository) {},
					migrationHandler: &mockMigrationHandler{err: storage.ErrRepositoryNotFound},
					repo:             repo,
				}
			},
			// When we encounter a ErrRepositoryNotFound error, we simply
			// skip the migration and don't mark it as completed or attempted.
			completed: false,
			attempts:  0,
		},
		{
			desc: "successful migration",
			setup: func() setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				return setupData{
					run:              func(m *migrator, repo *gitalypb.Repository) {},
					migrationHandler: &mockMigrationHandler{},
					repo:             repo,
				}
			},
			completed:      true,
			attempts:       1,
			expectedLogMsg: "migration successful for repository",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			data := tc.setup()

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &migrator{
				wg:               sync.WaitGroup{},
				migrateCh:        make(chan migrationData),
				logger:           logger,
				metrics:          metrics,
				node:             ptnMgr,
				state:            sync.Map{},
				migrationHandler: data.migrationHandler,
				ctx:              parentCtx,
				ctxCancel:        parentCancel,
			}

			storageName := cfg.Storages[0].Name

			m.Run()
			defer m.Close()

			// It is not guaranteed that the migration is registered, so run it in a
			// loop until it is.
			for {
				if _, ok := m.state.Load(migrationKey(storageName, data.repo.GetRelativePath())); ok {
					break
				}

				m.RegisterMigration(storageName, data.repo.GetRelativePath())
			}

			data.run(m, data.repo)

			// Block till the old migration is complete.
			m.migrateCh <- migrationData{}

			val, ok := m.state.Load(migrationKey(storageName, data.repo.GetRelativePath()))
			state := val.(migratorState)

			require.True(t, ok)
			require.Equal(t, tc.completed, state.completed)
			require.Equal(t, tc.attempts, state.attempts)

			if tc.expectedLogMsg != "" {
				entries := hook.AllEntries()
				require.Equal(t, tc.expectedLogMsg, entries[len(entries)-1].Message)
			}
		})
	}
}
