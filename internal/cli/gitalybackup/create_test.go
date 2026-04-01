package gitalybackup

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestCreateSubcommand(t *testing.T) {
	tests := []struct {
		Name               string
		Flags              func(backupRoot string) []string
		ServerOpts         func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt
		ExpectedErrMessage string
	}{
		{
			Name: "when a local backup is created",
			Flags: func(backupRoot string) []string {
				return []string{"--path", backupRoot, "--id", "the-new-backup"}
			},
			ServerOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				return nil
			},
			ExpectedErrMessage: `create: pipeline: 1 failures encountered:\n - invalid: manager: could not dial source: invalid connection string: \"invalid\"\n`,
		},
		{
			Name: "when a server-side backup is created",
			Flags: func(path string) []string {
				return []string{"--server-side", "--id", "the-new-backup"}
			},
			ServerOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				backupLocator := backup.ResolveLocator(backupSink)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				}
			},
			ExpectedErrMessage: `create: pipeline: 1 failures encountered:\n - invalid: server-side create: could not dial source: invalid connection string: \"invalid\"\n`,
		},
		{
			Name: "when a server-side incremental backup is created",
			Flags: func(path string) []string {
				return []string{"--server-side", "--incremental", "--id", "the-new-backup"}
			},
			ServerOpts: func(ctx context.Context, backupRoot string) []testserver.GitalyServerOpt {
				backupSink, err := backup.ResolveSink(ctx, backupRoot)
				require.NoError(t, err)

				backupLocator := backup.ResolveLocator(backupSink)

				return []testserver.GitalyServerOpt{
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				}
			},
			ExpectedErrMessage: `create: pipeline: 1 failures encountered:\n - invalid: server-side create: could not dial source: invalid connection string: \"invalid\"\n`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			path := testhelper.TempDir(t)

			cfg := testcfg.Build(t)
			cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll, tc.ServerOpts(ctx, path)...)

			var repos []*gitalypb.Repository
			for i := 0; i < 5; i++ {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				repos = append(repos, repo)
			}

			var stdin bytes.Buffer
			encoder := json.NewEncoder(&stdin)

			for _, repo := range repos {
				require.NoError(t, encoder.Encode(map[string]string{
					"address":         cfg.SocketPath,
					"token":           cfg.Auth.Token,
					"storage_name":    repo.GetStorageName(),
					"relative_path":   repo.GetRelativePath(),
					"gl_project_path": repo.GetGlProjectPath(),
				}))
			}

			// Partial failure scenario
			require.NoError(t, encoder.Encode(map[string]string{
				"address":       "invalid",
				"token":         "invalid",
				"relative_path": "invalid",
			}))

			// server-side WriteBackupID resolves the Gitaly server from the
			// context, so GITALY_SERVERS must be available.
			injectCtx, err := storage.InjectGitalyServers(ctx, cfg.Storages[0].Name, cfg.SocketPath, cfg.Auth.Token)
			require.NoError(t, err)
			t.Setenv("GITALY_SERVERS", metadata.GetValue(metadata.OutgoingToIncoming(injectCtx), "gitaly-servers"))

			args := append([]string{"create"}, tc.Flags(path)...)

			stdout, stderr, exitCode := runGitalyBackup(t, ctx, cfg, &stdin, args...)
			require.Empty(t, stderr)
			require.Contains(t, stdout, tc.ExpectedErrMessage)
			require.Equal(t, 1, exitCode)

			// The marker is written regardless of partial pipeline failures so
			// that successfully backed-up repos remain discoverable for restore.
			require.FileExists(t, filepath.Join(path, "backup_ids", "the-new-backup"))

			for _, repo := range repos {
				bundlePath := filepath.Join(path, repo.GetStorageName(), repo.GetRelativePath(), "the-new-backup", "001.bundle")
				require.FileExists(t, bundlePath)
			}
		})
	}
}
