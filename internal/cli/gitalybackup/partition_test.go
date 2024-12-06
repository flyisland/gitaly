package gitalybackup

import (
	"context"
	"io"
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
			expectedErrMessage: "extract gitaly servers: empty gitaly-servers metadata",
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
			expectedErrMessage: "extract gitaly servers: empty gitaly-servers metadata",
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

			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			cmd := NewApp()
			cmd.Writer = io.Discard

			args := append([]string{progname, "partition"}, "create")
			args = append(args, "--parallel", "2")
			err := cmd.RunContext(ctx, args)

			// The test relies on the interceptor being configured in the test server. If WAL is not enabled, the interceptor won't be configured,
			// and as a result the transaction won't be initialized.
			if !testhelper.IsWALEnabled() && tc.expectedErrMessage != "extract gitaly servers: empty gitaly-servers metadata" {
				tc.expectedErrMessage = "partition create: list partitions: rpc error: code = Internal desc = transactions not enabled"
			}
			if tc.expectedErrMessage != "" {
				require.EqualError(t, err, tc.expectedErrMessage)
				return
			}
			require.NoError(t, err)

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
