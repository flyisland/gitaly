package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
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
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestDryRunPartition(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.DryRunMigrations).Run(t, testDryRunPartition)
}

func testDryRunPartition(t *testing.T, ctx context.Context) {
	t.Parallel()

	if featureflag.DryRunMigrations.IsDisabled(ctx) {
		return
	}

	rollbackCalled := false
	p := dryRunPartition{mockPartition{
		beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
			return mockTransaction{
				commitFn: func(context.Context) error {
					t.Errorf("commit should not be called")
					return nil
				},
				rollbackFn: func(context.Context) error {
					rollbackCalled = true
					return nil
				},
			}, nil
		},
	}}

	txn, err := p.Begin(ctx, storage.BeginOptions{})
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctx))
	require.True(t, rollbackCalled)
}

func TestCombinedMigrations(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.DryRunMigrations).Run(t, testCombinedMigrations)
}

func testCombinedMigrations(t *testing.T, ctx context.Context) {
	t.Parallel()

	type setup struct {
		desc             string
		mainPartition    storagemgr.Partition
		dryRunPartition  storagemgr.Partition
		requiredErr      error
		requiredLogMsg   string
		requiredLogError error
	}

	for _, tc := range []setup{
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "both partitions raise no error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
					runFn: func() error {
						ch <- struct{}{}
						if featureflag.DryRunMigrations.IsEnabled(ctx) {
							ch <- struct{}{}
						}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
				},
				requiredErr: nil,
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "main partition raises an error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("main partition error")
					},
					runFn: func() error {
						ch <- struct{}{}
						if featureflag.DryRunMigrations.IsEnabled(ctx) {
							ch <- struct{}{}
						}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
				},
				requiredErr: errors.New("main partition error"),
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "dry-run partition raises an error",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return mockTransaction{}, nil
					},
					runFn: func() error {
						ch <- struct{}{}
						if featureflag.DryRunMigrations.IsEnabled(ctx) {
							ch <- struct{}{}
						}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("dryrun partition error")
					},
				},
				requiredErr:      nil,
				requiredLogMsg:   "failed to begin migration dry-run",
				requiredLogError: errors.New("dryrun partition error"),
			}
		}(),
		func() setup {
			ch := make(chan struct{})
			return setup{
				desc: "both partition raise errors",
				mainPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("main partition error")
					},
					runFn: func() error {
						ch <- struct{}{}
						if featureflag.DryRunMigrations.IsEnabled(ctx) {
							ch <- struct{}{}
						}
						return nil
					},
				},
				dryRunPartition: mockPartition{
					beginFn: func(context.Context, storage.BeginOptions) (storage.Transaction, error) {
						<-ch
						return nil, errors.New("dryrun partition error")
					},
				},
				requiredErr:      errors.New("main partition error"),
				requiredLogMsg:   "failed to begin migration dry-run",
				requiredLogError: errors.New("dryrun partition error"),
			}
		}(),
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			done := make(chan struct{})

			logger := testhelper.NewLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			partition := combinedMigrationPartition{
				Partition: tc.mainPartition,
				logger:    logger,
				dryRun:    tc.dryRunPartition,
			}

			// The storagemgr.Partition API is constructed in such a way that
			// the partition runs in the background to process transactions while
			// the begin function is called synchronously.
			//
			// Since we use mock partitions, we simulate that behavior here.
			go func() {
				if err := partition.Run(); err != nil {
					t.Errorf("expected nil, got %s", err)
				}
				done <- struct{}{}
			}()

			_, err := partition.Begin(ctx, storage.BeginOptions{})
			require.Equal(t, tc.requiredErr, err)

			<-done

			if entry := loggerHook.LastEntry(); entry != nil {
				require.Equal(t, tc.requiredLogMsg, entry.Message)
				require.Equal(t, tc.requiredLogError, entry.Data["error"])
			}
		})
	}
}

func TestCombinedMigrationClosesPartition(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.DryRunMigrations).
		Run(t, testCombinedMigrationClosesPartition)
}

func testCombinedMigrationClosesPartition(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

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

	repositoryFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, cache)

	m := partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))

	factory := partition.NewFactory(cmdFactory, repositoryFactory, m, nil)
	tm := factory.New(logger, testPartitionID, database, storageName, storagePath, stateDir, stagingDir)

	combinedPartiton := newCombinedMigrationPartition(tm, logger, NewMetrics(), storageName, []Migration{}, []Migration{
		{
			ID:   100,
			Name: "foo",
			Fn: func(ctx context.Context, _ storage.Transaction, _, _ string) error {
				// When the partition is cancelled, the context cancellation should
				// also be propagated to the migrations.
				<-ctx.Done()

				return nil
			},
		},
	})

	done := make(chan struct{})
	go func() {
		if err := combinedPartiton.Run(); err != nil {
			t.Errorf("expected nil, got %s", err)
		}
		done <- struct{}{}
	}()

	_, err = combinedPartiton.Begin(ctx, storage.BeginOptions{})
	require.NoError(t, err)

	// Closing the main partition and check if the `Run()` function also returns.
	combinedPartiton.Close()
	<-done
}
