package gitalybackup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
)

func TestPartitionSubcommand_Create(t *testing.T) {
	if testhelper.IsPraefectEnabled() {
		t.Skip(`This command calls partition scoped RPCs and Praefect currently doesn't support routing the PARTITION scoped RPC messages.`)
	}

	tests := []struct {
		name               string
		serverOpts         func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt
		envSetup           func(ctx context.Context, cfg config.Cfg)
		commandArgs        []string
		expectedErrMessage string
	}{
		{
			name: "when gitaly server is not configured",
			serverOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
				}
			},
			envSetup:           func(ctx context.Context, cfg config.Cfg) {},
			commandArgs:        []string{"partition", "create", "--parallel", "2"},
			expectedErrMessage: "extract gitaly servers: empty metadata",
		},
		{
			name: "when gitaly server is empty",
			serverOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
				}
			},
			envSetup: func(ctx context.Context, cfg config.Cfg) {
				t.Setenv("GITALY_SERVERS", "")
			},
			commandArgs:        []string{"partition", "create", "--parallel", "2"},
			expectedErrMessage: "extract gitaly servers: empty metadata",
		},
		{
			name: "when gitaly server is correctly configured",
			serverOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
				}
			},
			envSetup: func(ctx context.Context, cfg config.Cfg) {
				ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
				require.NoError(t, err)
				t.Setenv("GITALY_SERVERS", metadata.GetValue(metadata.OutgoingToIncoming(ctx), "gitaly-servers"))
			},
			commandArgs:        []string{"partition", "create", "--parallel", "2"},
			expectedErrMessage: "",
		},
		{
			name: "when wrong timeout format is given",
			serverOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
				}
			},
			envSetup: func(ctx context.Context, cfg config.Cfg) {
				ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
				require.NoError(t, err)
				t.Setenv("GITALY_SERVERS", metadata.GetValue(metadata.OutgoingToIncoming(ctx), "gitaly-servers"))
			},
			commandArgs:        []string{"partition", "create", "--parallel", "2", "--timeout", "30"},
			expectedErrMessage: "parse timeout duration: time: missing unit in duration",
		},
		{
			name: "when correct timeout format is given",
			serverOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
				}
			},
			envSetup: func(ctx context.Context, cfg config.Cfg) {
				ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
				require.NoError(t, err)
				t.Setenv("GITALY_SERVERS", metadata.GetValue(metadata.OutgoingToIncoming(ctx), "gitaly-servers"))
			},
			commandArgs:        []string{"partition", "create", "--parallel", "2", "--timeout", "30s"},
			expectedErrMessage: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			path := testhelper.TempDir(t)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll, tc.serverOpts(ctx, path)...)

			tc.envSetup(ctx, cfg)

			repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipSnapshotInvalidation: true,
			})

			stdout, stderr, exitCode := runGitalyBackup(t, ctx, cfg, bytes.NewReader(nil), tc.commandArgs...)
			require.Empty(t, stderr)

			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() && tc.expectedErrMessage == "" {
				tc.expectedErrMessage = "partition create: list partitions: rpc error: code = Internal desc = transactions not enabled"
			}
			if tc.expectedErrMessage != "" {
				require.Contains(t, stdout, tc.expectedErrMessage)
				require.Equal(t, 1, exitCode)
				return
			}
			require.Zero(t, exitCode)

			testhelper.SkipWithRaft(t, `The test asserts the existence of backup files based on the latest
				LSN. When Raft is not enabled, the LSN is not static. The test should fetch the latest
				LSN instead https://gitlab.com/gitlab-org/gitaly/-/issues/6459`)

			lsn := storage.LSN(1)
			tarPath := filepath.Join(path, "partition-backups", cfg.Storages[0].Name, "2", lsn.String()) + ".tar"
			tar, err := os.Open(tarPath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, tar)

			testhelper.ContainsTarState(t, tar, testhelper.DirectoryState{
				"fs": {Mode: archive.DirectoryMode},
				filepath.Join("fs", repo.GetRelativePath()): {Mode: archive.DirectoryMode},
			})
		})
	}
}
