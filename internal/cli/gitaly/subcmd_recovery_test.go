package gitaly

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestRecoveryCLI_status(t *testing.T) {
	t.Parallel()

	type setupOptions struct {
		cfg           config.Cfg
		storageMgr    node.Storage
		locator       storage.Locator
		gitCmdFactory gitcmd.CommandFactory
		catfileCache  catfile.Cache
		backupRoot    string
	}

	type setupData struct {
		storageName    string
		partitionID    storage.PartitionID
		expectedErr    error
		expectedOutput string
	}

	for _, tc := range []struct {
		desc  string
		setup func(tb testing.TB, ctx context.Context, opts setupOptions) setupData
	}{
		{
			desc: "unknown storage",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:    "pineapple",
					expectedErr:    errors.New("exit status 1"),
					expectedOutput: "get storage: storage name not found\n",
				}
			},
		},
		{
			desc: "partition 0",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:    opts.cfg.Storages[0].Name,
					partitionID:    0,
					expectedErr:    errors.New("exit status 1"),
					expectedOutput: fmt.Sprintf("invalid partition ID %s\n", storage.PartitionID(0)),
				}
			},
		},
		{
			desc: "unknown partition",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					partitionID: 42,
					// TODO: This currently will create arbitrary partitions.
					// It should return an error instead.
					// https://gitlab.com/gitlab-org/gitaly/-/issues/6478
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
`,
						storage.PartitionID(42),
						storage.LSN(0),
					),
				}
			},
		},
		{
			desc: "success, no backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				return setupData{
					storageName: repo.GetStorageName(),
					partitionID: 2,
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Relative paths:
 - %s
`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
					),
				}
			},
		},
		{
			desc: "success, backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					partitionID: 2,
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Relative paths:
 - %s
Available backup entries:
 - from %s to %s
`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
						storage.LSN(2),
						storage.LSN(3),
					),
				}
			},
		},
		{
			desc: "success, non-contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(5).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(6).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(7).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(8).String()+".tar"):  "",
					filepath.Join(partitionPath, storage.LSN(10).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					partitionID: 2,
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Relative paths:
 - %s
Available backup entries:
 - from %s to %s
 - from %s to %s
 - %s
`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
						storage.LSN(2),
						storage.LSN(3),
						storage.LSN(5),
						storage.LSN(8),
						storage.LSN(10),
					),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := t.TempDir()
			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			cfg.Backup.WALGoCloudURL = backupRoot
			configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)
			testcfg.BuildGitaly(t, cfg)

			logger := testhelper.SharedLogger(t)

			dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			locator := config.NewLocator(cfg)
			gitCmdFactory := gittest.NewCommandFactory(t, cfg)
			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			storageMgr, err := storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache),
					partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)),
					nil,
				),
				1,
				storagemgr.NewMetrics(cfg.Prometheus),
			)
			require.NoError(t, err)

			data := tc.setup(t, ctx, setupOptions{
				cfg:           cfg,
				storageMgr:    storageMgr,
				locator:       locator,
				gitCmdFactory: gitCmdFactory,
				catfileCache:  catfileCache,
				backupRoot:    backupRoot,
			})

			// Stop storage and DB so that we can run the command "offline"
			storageMgr.Close()
			dbMgr.Close()

			cmd := exec.Command(cfg.BinaryPath("gitaly"), "recovery", "-config", configPath, "status", "-storage", data.storageName, "-partition", data.partitionID.String())

			output, err := cmd.CombinedOutput()
			testhelper.RequireGrpcError(t, data.expectedErr, err)

			require.Contains(t, string(output), data.expectedOutput)
		})
	}
}

func TestRecoveryCLI_replay(t *testing.T) {
	t.Parallel()

	type setupOptions struct {
		cfg           config.Cfg
		storageMgr    node.Storage
		locator       storage.Locator
		gitCmdFactory gitcmd.CommandFactory
		catfileCache  catfile.Cache
		backupRoot    string
	}

	type setupData struct {
		storageName    string
		partitionID    storage.PartitionID
		expectedErr    error
		expectedOutput string
		expectedLSN    storage.LSN
	}

	for _, tc := range []struct {
		desc  string
		setup func(tb testing.TB, ctx context.Context, opts setupOptions) setupData
	}{
		{
			desc: "unknown storage",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:    "pineapple",
					expectedErr:    errors.New("exit status 1"),
					expectedOutput: "get storage: storage name not found\n",
					expectedLSN:    storage.LSN(0),
				}
			},
		},
		{
			desc: "partition 0",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:    opts.cfg.Storages[0].Name,
					partitionID:    0,
					expectedErr:    errors.New("exit status 1"),
					expectedOutput: fmt.Sprintf("invalid partition ID %s\n", storage.PartitionID(0)),
					expectedLSN:    storage.LSN(0),
				}
			},
		},
		{
			desc: "unknown partition",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					partitionID: 42,
					// TODO: This currently will create arbitrary partitions.
					// It should return an error instead.
					// https://gitlab.com/gitlab-org/gitaly/-/issues/6478
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Starting archived log entries import
Successfully processed log entries up to LSN %s
`,
						storage.PartitionID(42),
						storage.LSN(0),
						storage.LSN(0),
					),
					expectedLSN: storage.LSN(0),
				}
			},
		},
		{
			desc: "success, no backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				return setupData{
					storageName: repo.GetStorageName(),
					partitionID: 2,
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Starting archived log entries import
Successfully processed log entries up to LSN %s
`,
						storage.PartitionID(2),
						storage.LSN(1),
						storage.LSN(1),
					),
					expectedLSN: storage.LSN(1),
				}
			},
		},
		{
			desc: "success, contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
				})

				return setupData{
					storageName: repo.GetStorageName(),
					partitionID: 2,
					expectedOutput: fmt.Sprintf(`Partition ID: %s
Applied LSN: %s
Starting archived log entries import
Successfully processed log entries up to LSN %s
`,
						storage.PartitionID(2),
						storage.LSN(1),
						storage.LSN(3),
					),
					expectedLSN: storage.LSN(3),
				}
			},
		},
		{
			desc: "non-contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				logger := testhelper.SharedLogger(t)
				repo := &gitalypb.Repository{
					StorageName:  opts.cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				}

				txn, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					AllowPartitionAssignmentWithoutRepository: true,
				})
				require.NoError(t, err)

				err = repoutil.Create(
					storage.ContextWithTransaction(ctx, txn),
					logger,
					opts.locator,
					opts.gitCmdFactory,
					opts.catfileCache,
					transaction.NewTrackingManager(),
					counter.NewRepositoryCounter(opts.cfg.Storages),
					txn.RewriteRepository(repo),
					func(repo *gitalypb.Repository) error {
						return nil
					},
				)
				require.NoError(t, err)

				require.NoError(t, txn.Commit(ctx))

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
					filepath.Join(partitionPath, storage.LSN(4).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath()),
				})

				return setupData{
					storageName:    repo.GetStorageName(),
					partitionID:    2,
					expectedErr:    errors.New("exit status 1"),
					expectedOutput: "there is discontinuity in the WAL entries. Expected: 3, Got: 4\n",
					expectedLSN:    storage.LSN(2),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := t.TempDir()
			tCtx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			cfg.Backup.WALGoCloudURL = backupRoot
			configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)
			testcfg.BuildGitaly(t, cfg)

			logger := testhelper.SharedLogger(t)

			ctx, cancel := context.WithCancel(tCtx)
			defer cancel()

			dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			locator := config.NewLocator(cfg)
			gitCmdFactory := gittest.NewCommandFactory(t, cfg)
			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			storageMgr, err := storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache),
					partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)),
					nil,
				),
				1,
				storagemgr.NewMetrics(cfg.Prometheus),
			)
			require.NoError(t, err)

			data := tc.setup(t, ctx, setupOptions{
				cfg:           cfg,
				storageMgr:    storageMgr,
				locator:       locator,
				gitCmdFactory: gitCmdFactory,
				catfileCache:  catfileCache,
				backupRoot:    backupRoot,
			})

			// Stop storage and DB so that we can run the command "offline"
			storageMgr.Close()
			dbMgr.Close()

			cmd := exec.Command(cfg.BinaryPath("gitaly"), "recovery", "-config", configPath, "replay", "-storage", data.storageName, "-partition", data.partitionID.String())

			output, err := cmd.CombinedOutput()
			testhelper.RequireGrpcError(t, data.expectedErr, err)

			require.Contains(t, string(output), data.expectedOutput)

			// Creating storage manager again as we had to close it previously to run the command in offline mode
			dbMgr, err = databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			storageMgr, err = storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(
					gitCmdFactory,
					localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache),
					partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)),
					nil,
				),
				1,
				storagemgr.NewMetrics(cfg.Prometheus),
			)
			require.NoError(t, err)
			defer storageMgr.Close()

			partition, err := storageMgr.GetPartition(ctx, data.partitionID)
			require.NoError(t, err)

			tr, err := partition.Begin(ctx, storage.BeginOptions{})
			require.NoError(t, err)
			appliedLSN := tr.SnapshotLSN()
			require.NoError(t, tr.Rollback(ctx))
			require.Equal(t, data.expectedLSN, appliedLSN)
		})
	}
}

func createValidLogEntryArchive(t *testing.T, repoRelativePath string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Create a dummy MANIFEST file
	manifest := &gitalypb.LogEntry{
		RelativePath: repoRelativePath,
		Operations:   []*gitalypb.LogEntry_Operation{},
	}
	manifestBytes, err := proto.Marshal(manifest)
	require.NoError(t, err)

	err = tw.WriteHeader(&tar.Header{
		Name: "MANIFEST",
		Mode: 0o644,
		Size: int64(len(manifestBytes)),
	})
	require.NoError(t, err)
	_, err = tw.Write(manifestBytes)
	require.NoError(t, err)

	require.NoError(t, tw.Close())

	return buf.Bytes()
}
