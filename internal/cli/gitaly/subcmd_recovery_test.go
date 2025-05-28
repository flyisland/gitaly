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

type setupOptions struct {
	cfg           config.Cfg
	storageMgr    node.Storage
	locator       storage.Locator
	gitCmdFactory gitcmd.CommandFactory
	catfileCache  catfile.Cache
	backupRoot    string
}

type setupData struct {
	storageName     string
	args            []string
	expectedErr     error
	expectedOutputs []string
	expectedLSN     map[storage.PartitionID]storage.LSN
}

func TestRecoveryCLI_status(t *testing.T) {
	t.Parallel()

	testhelper.SkipWithRaft(t, "Raft must not be enabled during recovery")

	for _, tc := range []struct {
		desc  string
		setup func(tb testing.TB, ctx context.Context, opts setupOptions) setupData
	}{
		{
			desc: "unknown storage",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:     "pineapple",
					expectedErr:     errors.New("exit status 1"),
					expectedOutputs: []string{"get storage: storage name not found\n"},
				}
			},
		},
		{
			desc: "partition 0",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:     opts.cfg.Storages[0].Name,
					args:            []string{"-partition", storage.PartitionID(0).String()},
					expectedErr:     errors.New("exit status 1"),
					expectedOutputs: []string{fmt.Sprintf("invalid partition ID %s\n", storage.PartitionID(0))},
				}
			},
		},
		{
			desc: "unknown partition",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					args:        []string{"-partition", storage.PartitionID(42).String()},
					// TODO: This currently will create arbitrary partitions.
					// It should return an error instead.
					// https://gitlab.com/gitlab-org/gitaly/-/issues/6478
					expectedOutputs: []string{fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Available WAL backup entries: No entries found
recovery status completed: 1 succeeded, 0 failed`,
						storage.PartitionID(42),
						storage.LSN(0),
					)},
				}
			},
		},
		{
			desc: "not all necessary flags provided",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{},
					expectedErr: errors.New("exit status 1"),
					expectedOutputs: []string{
						"this command requires one of --all, --partition or --repository flags",
					},
				}
			},
		},
		{
			desc: "both partition ID and relative path provided",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String(), "-repository", repo.GetRelativePath()},
					expectedErr: errors.New("exit status 1"),
					expectedOutputs: []string{
						"--partition and --repository flags can not be provided at the same time",
					},
				}
			},
		},
		{
			desc: "success, no backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedOutputs: []string{fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: No entries found
recovery status completed: 1 succeeded, 0 failed`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
					)},
				}
			},
		},
		{
			desc: "success, backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedOutputs: []string{fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: up to LSN: %s
recovery status completed: 1 succeeded, 0 failed`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
						storage.LSN(3),
					)},
				}
			},
		},
		{
			desc: "success using relative path",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-repository", repo.GetRelativePath()},
					expectedOutputs: []string{fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: up to LSN: %s
recovery status completed: 1 succeeded, 0 failed`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
						storage.LSN(3),
					)},
				}
			},
		},
		{
			desc: "success, non-contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(4).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(5).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(7).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(8).String()+".tar"): "",
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedOutputs: []string{fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: up to LSN: %s
There is a gap in WAL archive after LSN: %s
recovery status completed: 1 succeeded, 0 failed`,
						storage.PartitionID(2),
						storage.LSN(1),
						repo.GetRelativePath(),
						storage.LSN(5),
						storage.LSN(5),
					)},
				}
			},
		},
		{
			desc: "success with all flag and multiple partitions",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo1, err := createRepository(t, ctx, opts)
				require.NoError(t, err)
				partitionPath := filepath.Join(repo1.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
				})

				repo2, err := createRepository(t, ctx, opts)
				require.NoError(t, err)
				partitionPath = filepath.Join(repo2.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(3)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): "",
					filepath.Join(partitionPath, storage.LSN(4).String()+".tar"): "",
				})

				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					args:        []string{"-all", "-parallel", "2"},
					expectedOutputs: []string{
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: up to LSN: %s`,
							storage.PartitionID(2),
							storage.LSN(1),
							repo1.GetRelativePath(),
							storage.LSN(3)),
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Relative paths:
 - %s
Available WAL backup entries: up to LSN: %s`,
							storage.PartitionID(3),
							storage.LSN(1),
							repo2.GetRelativePath(),
							storage.LSN(4)),
						"recovery status completed: 2 succeeded, 0 failed",
					},
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

			partitionFactoryOptions := []partition.FactoryOption{
				partition.WithCmdFactory(gitCmdFactory),
				partition.WithRepoFactory(localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache)),
				partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
				partition.WithRaftConfig(cfg.Raft),
			}

			storageMgr, err := storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(partitionFactoryOptions...),
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

			args := []string{"recovery", "-config", configPath, "status", "-storage", data.storageName}
			args = append(args, data.args...)
			cmd := exec.Command(cfg.BinaryPath("gitaly"), args...)
			output, err := cmd.CombinedOutput()
			testhelper.RequireGrpcError(t, data.expectedErr, err)

			for _, expectedOutput := range data.expectedOutputs {
				require.Contains(t, string(output), expectedOutput)
			}
		})
	}
}

func TestRecoveryCLI_replay(t *testing.T) {
	t.Parallel()

	testhelper.SkipWithRaft(t, "Raft must not be enabled during recovery")

	for _, tc := range []struct {
		desc  string
		setup func(tb testing.TB, ctx context.Context, opts setupOptions) setupData
	}{
		{
			desc: "unknown storage",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:     "pineapple",
					expectedErr:     errors.New("exit status 1"),
					expectedOutputs: []string{"get storage: storage name not found\n"},
					expectedLSN:     nil,
				}
			},
		},
		{
			desc: "partition 0",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName:     opts.cfg.Storages[0].Name,
					args:            []string{"-partition", storage.PartitionID(0).String()},
					expectedErr:     errors.New("exit status 1"),
					expectedOutputs: []string{fmt.Sprintf("invalid partition ID %s\n", storage.PartitionID(0))},
					expectedLSN:     nil,
				}
			},
		},
		{
			desc: "unknown partition",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					args:        []string{"-partition", storage.PartitionID(42).String()},
					// TODO: This currently will create arbitrary partitions.
					// It should return an error instead.
					// https://gitlab.com/gitlab-org/gitaly/-/issues/6478
					expectedOutputs: []string{
						"started processing partition 42",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s
recovery replay completed: 1 succeeded, 0 failed`,
							storage.PartitionID(42),
							storage.LSN(0),
							storage.LSN(0),
						),
					},
					expectedLSN: nil,
				}
			},
		},
		{
			desc: "success, no backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedOutputs: []string{
						"started processing partition 2",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s
recovery replay completed: 1 succeeded, 0 failed`,
							storage.PartitionID(2),
							storage.LSN(1),
							storage.LSN(1),
						),
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 1},
				}
			},
		},
		{
			desc: "success, contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(3)),
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedOutputs: []string{
						"started processing partition 2",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s
recovery replay completed: 1 succeeded, 0 failed`,
							storage.PartitionID(2),
							storage.LSN(1),
							storage.LSN(3),
						),
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 3},
				}
			},
		},
		{
			desc: "success using relative path, contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(3)),
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-repository", repo.GetRelativePath()},
					expectedOutputs: []string{
						"started processing partition 2",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s
recovery replay completed: 1 succeeded, 0 failed`,
							storage.PartitionID(2),
							storage.LSN(1),
							storage.LSN(3),
						),
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 3},
				}
			},
		},
		{
			desc: "non-contiguous backups",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(4).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(4)),
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedErr: errors.New("exit status 1"),
					expectedOutputs: []string{
						"started processing partition 2",
						"restore replay for partition 2 failed: there is discontinuity in the WAL entries. Expected LSN: 0000000000003, Got: 0000000000004",
						"recovery replay completed: 0 succeeded, 1 failed",
						"recovery replay failed for 1 out of 1 partition(s)",
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 2},
				}
			},
		},
		{
			desc: "fail to apply a log entry",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo, err := createRepository(t, ctx, opts)
				require.NoError(t, err)

				partitionPath := filepath.Join(repo.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createInvalidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo.GetRelativePath(), storage.LSN(3)),
				})

				return setupData{
					storageName: repo.GetStorageName(),
					args:        []string{"-partition", storage.PartitionID(2).String()},
					expectedErr: errors.New("exit status 1"),
					expectedOutputs: []string{
						"started processing partition 2",
						"restore replay for partition 2 failed: failed to apply latest log entry: transaction processing stopped",
						"recovery replay completed: 0 succeeded, 1 failed",
						`msg="partition failed" error="apply log entry: update: apply operations`,
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 1},
				}
			},
		},
		{
			desc: "success with all flag and multiple partitions",
			setup: func(tb testing.TB, ctx context.Context, opts setupOptions) setupData {
				repo1, err := createRepository(t, ctx, opts)
				require.NoError(t, err)
				partitionPath := filepath.Join(repo1.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(2)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(3)),
				})

				repo2, err := createRepository(t, ctx, opts)
				require.NoError(t, err)
				partitionPath = filepath.Join(repo2.GetStorageName(), fmt.Sprintf("%d", storage.PartitionID(3)))
				testhelper.WriteFiles(t, opts.backupRoot, map[string]any{
					filepath.Join(partitionPath, storage.LSN(1).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(1)),
					filepath.Join(partitionPath, storage.LSN(2).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(2)),
					filepath.Join(partitionPath, storage.LSN(3).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(3)),
					filepath.Join(partitionPath, storage.LSN(4).String()+".tar"): createValidLogEntryArchive(t, repo1.GetRelativePath(), storage.LSN(4)),
				})
				return setupData{
					storageName: opts.cfg.Storages[0].Name,
					args:        []string{"-all", "-parallel", "2"},
					expectedOutputs: []string{
						"started processing partition 2",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s`,
							storage.PartitionID(2),
							storage.LSN(1),
							storage.LSN(3),
						),
						"started processing partition 3",
						fmt.Sprintf(`---------------------------------------------
Partition ID: %s - Applied LSN: %s
Successfully processed log entries up to LSN: %s`,
							storage.PartitionID(3),
							storage.LSN(1),
							storage.LSN(4),
						),
						"recovery replay completed: 2 succeeded, 0 failed",
					},
					expectedLSN: map[storage.PartitionID]storage.LSN{2: 3, 3: 4},
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

			partitionFactoryOptions := []partition.FactoryOption{
				partition.WithCmdFactory(gitCmdFactory),
				partition.WithRepoFactory(localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache)),
				partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
				partition.WithRaftConfig(cfg.Raft),
			}

			storageMgr, err := storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(partitionFactoryOptions...),
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

			args := []string{"recovery", "-config", configPath, "replay", "-storage", data.storageName}
			args = append(args, data.args...)
			cmd := exec.Command(cfg.BinaryPath("gitaly"), args...)

			output, err := cmd.CombinedOutput()
			if err != nil && data.expectedErr == nil {
				t.Log(string(output))
			}
			testhelper.RequireGrpcError(t, data.expectedErr, err)
			for _, expectedOutput := range data.expectedOutputs {
				require.Contains(t, string(output), expectedOutput)
			}

			// Creating storage manager again as we had to close it previously to run the command in offline mode
			dbMgr, err = databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			storageMgr, err = storagemgr.NewStorageManager(
				logger,
				cfg.Storages[0].Name,
				cfg.Storages[0].Path,
				dbMgr,
				partition.NewFactory(partitionFactoryOptions...),
				1,
				storagemgr.NewMetrics(cfg.Prometheus),
			)
			require.NoError(t, err)
			defer storageMgr.Close()

			for partitionID, lsn := range data.expectedLSN {
				partition, err := storageMgr.GetPartition(ctx, partitionID)
				require.NoError(t, err)

				tr, err := partition.Begin(ctx, storage.BeginOptions{})
				require.NoError(t, err)
				appliedLSN := tr.SnapshotLSN()
				require.NoError(t, tr.Rollback(ctx))
				require.Equal(t, lsn, appliedLSN)
			}
		})
	}
}

func createValidLogEntryArchive(t *testing.T, repoRelativePath string, lsn storage.LSN) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// First create the directory entry
	err := tw.WriteHeader(&tar.Header{
		Name:     lsn.String() + "/", // Add trailing slash for directory
		Mode:     0o755,
		Typeflag: tar.TypeDir,
	})
	require.NoError(t, err)

	// Create a dummy MANIFEST file
	manifest := &gitalypb.LogEntry{
		RelativePath: repoRelativePath,
		Operations:   []*gitalypb.LogEntry_Operation{},
	}
	manifestBytes, err := proto.Marshal(manifest)
	require.NoError(t, err)

	err = tw.WriteHeader(&tar.Header{
		Name: lsn.String() + "/MANIFEST",
		Mode: 0o644,
		Size: int64(len(manifestBytes)),
	})
	require.NoError(t, err)
	_, err = tw.Write(manifestBytes)
	require.NoError(t, err)

	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func createInvalidLogEntryArchive(t *testing.T, repoRelativePath string, lsn storage.LSN) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// First create the directory entry
	err := tw.WriteHeader(&tar.Header{
		Name:     lsn.String() + "/", // Add trailing slash for directory
		Mode:     0o755,
		Typeflag: tar.TypeDir,
	})
	require.NoError(t, err)

	// Create a dummy MANIFEST file
	manifest := &gitalypb.LogEntry{
		RelativePath: repoRelativePath,
		Operations: []*gitalypb.LogEntry_Operation{
			{
				Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
					CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
						SourcePath:      []byte("please-do-not-exist"),
						DestinationPath: []byte("destination"),
					},
				},
			},
		},
	}
	manifestBytes, err := proto.Marshal(manifest)
	require.NoError(t, err)

	err = tw.WriteHeader(&tar.Header{
		Name: lsn.String() + "/MANIFEST",
		Mode: 0o644,
		Size: int64(len(manifestBytes)),
	})
	require.NoError(t, err)
	_, err = tw.Write(manifestBytes)
	require.NoError(t, err)

	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func createRepository(t *testing.T, ctx context.Context, opts setupOptions) (*gitalypb.Repository, error) {
	repo := &gitalypb.Repository{
		StorageName:  opts.cfg.Storages[0].Name,
		RelativePath: gittest.NewRepositoryName(t),
	}

	txn1, err := opts.storageMgr.Begin(ctx, storage.TransactionOptions{
		RelativePath: repo.GetRelativePath(),
		AllowPartitionAssignmentWithoutRepository: true,
	})
	if err != nil {
		return nil, err
	}

	err = repoutil.Create(
		storage.ContextWithTransaction(ctx, txn1),
		testhelper.SharedLogger(t),
		opts.locator,
		opts.gitCmdFactory,
		opts.catfileCache,
		transaction.NewTrackingManager(),
		counter.NewRepositoryCounter(opts.cfg.Storages),
		txn1.RewriteRepository(repo),
		func(repo *gitalypb.Repository) error {
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	_, err = txn1.Commit(ctx)
	return repo, err
}
