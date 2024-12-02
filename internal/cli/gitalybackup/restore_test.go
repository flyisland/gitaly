package gitalybackup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestRestoreSubcommand(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	conn := gittest.DialService(t, ctx, cfg)

	// This is an example of a "dangling" repository (one that was created after a backup was taken) that should be
	// removed after the backup is restored.
	existingRepo, existRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: "existing_repo",
	})
	gittest.WriteCommit(t, cfg, existRepoPath, gittest.WithBranch(git.DefaultBranch))

	// This pool is also dangling but should not be removed after the backup is restored.
	poolRepo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: gittest.NewObjectPoolName(t),
	})

	// The backupDir contains the artifacts that would've been created as part of a backup.
	backupDir := testhelper.TempDir(t)
	testhelper.WriteFiles(t, backupDir, map[string]any{
		filepath.Join(existingRepo.GetRelativePath() + ".bundle"): gittest.Exec(t, cfg, "-C", existRepoPath, "bundle", "create", "-", "--all"),
		filepath.Join(existingRepo.GetRelativePath() + ".refs"):   gittest.Exec(t, cfg, "-C", existRepoPath, "show-ref", "--head"),
	})

	// These repos are the ones being restored, and should exist after the restore.
	var repos []*gitalypb.Repository
	for i := 0; i < 2; i++ {
		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			RelativePath: fmt.Sprintf("repo-%d", i),
			Storage:      cfg.Storages[0],
		})

		testhelper.WriteFiles(t, backupDir, map[string]any{
			filepath.Join("manifests", repo.GetStorageName(), repo.GetRelativePath(), "+latest.toml"): fmt.Sprintf(`
object_format = '%[1]s'
head_reference = '%[3]s'

[[steps]]
bundle_path = '%[2]s.bundle'
ref_path = '%[2]s.refs'
custom_hooks_path = '%[2]s/custom_hooks.tar'
`, gittest.DefaultObjectHash.Format, existingRepo.GetRelativePath(), git.DefaultRef.String()),
		})

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

	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	args := []string{
		progname,
		"restore",
		"--path",
		backupDir,
		"--parallel",
		strconv.Itoa(runtime.NumCPU()),
		"--parallel-storage",
		"2",
		"--layout",
		"pointer",
		"--remove-all-repositories",
		existingRepo.GetStorageName(),
	}
	cmd := NewApp()
	cmd.Reader = &stdin
	cmd.Writer = io.Discard

	require.True(t, gittest.RepositoryExists(t, ctx, conn, existingRepo))
	require.True(t, gittest.RepositoryExists(t, ctx, conn, poolRepo))

	require.NoError(t, cmd.RunContext(ctx, args))

	require.False(t, gittest.RepositoryExists(t, ctx, conn, existingRepo))
	require.True(t, gittest.RepositoryExists(t, ctx, conn, poolRepo))

	// Ensure the repos were restored correctly.
	for _, repo := range repos {
		repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
		bundlePath := filepath.Join(backupDir, existingRepo.GetRelativePath()+".bundle")

		output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
		require.Contains(t, string(output), "The bundle records a complete history")
	}
}

func TestRestoreSubcommand_serverSide(t *testing.T) {
	ctx := testhelper.Context(t)

	backupDir := testhelper.TempDir(t)
	backupSink, err := backup.ResolveSink(ctx, backupDir)
	require.NoError(t, err)

	backupLocator, err := backup.ResolveLocator("pointer", backupSink)
	require.NoError(t, err)

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll,
		testserver.WithBackupSink(backupSink),
		testserver.WithBackupLocator(backupLocator),
	)
	conn := gittest.DialService(t, ctx, cfg)

	existingRepo, existRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: "existing_repo",
	})
	gittest.WriteCommit(t, cfg, existRepoPath, gittest.WithBranch(git.DefaultBranch))

	// This pool is dangling but should not be removed after the backup is restored.
	poolRepo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: gittest.NewObjectPoolName(t),
	})

	testhelper.WriteFiles(t, backupDir, map[string]any{
		filepath.Join(existingRepo.GetRelativePath() + ".bundle"): gittest.Exec(t, cfg, "-C", existRepoPath, "bundle", "create", "-", "--all"),
		filepath.Join(existingRepo.GetRelativePath() + ".refs"):   gittest.Exec(t, cfg, "-C", existRepoPath, "show-ref", "--head"),
	})

	var repos []*gitalypb.Repository
	for i := 0; i < 2; i++ {
		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			RelativePath: fmt.Sprintf("repo-%d", i),
			Storage:      cfg.Storages[0],
		})

		testhelper.WriteFiles(t, backupDir, map[string]any{
			filepath.Join("manifests", repo.GetStorageName(), repo.GetRelativePath(), "+latest.toml"): fmt.Sprintf(`
object_format = '%[1]s'
head_reference = '%[3]s'

[[steps]]
bundle_path = '%[2]s.bundle'
ref_path = '%[2]s.refs'
custom_hooks_path = '%[2]s/custom_hooks.tar'
`, gittest.DefaultObjectHash.Format, existingRepo.GetRelativePath(), git.DefaultRef.String()),
		})

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

	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	args := []string{
		progname,
		"restore",
		"--parallel",
		strconv.Itoa(runtime.NumCPU()),
		"--parallel-storage",
		"2",
		"--layout",
		"pointer",
		"--remove-all-repositories",
		existingRepo.GetStorageName(),
		"--server-side",
		"true",
	}
	cmd := NewApp()
	cmd.Reader = &stdin
	cmd.Writer = io.Discard

	require.True(t, gittest.RepositoryExists(t, ctx, conn, existingRepo))
	require.True(t, gittest.RepositoryExists(t, ctx, conn, poolRepo))

	require.NoError(t, cmd.RunContext(ctx, args))

	require.False(t, gittest.RepositoryExists(t, ctx, conn, existingRepo))
	require.True(t, gittest.RepositoryExists(t, ctx, conn, poolRepo))

	for _, repo := range repos {
		repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
		bundlePath := filepath.Join(backupDir, existingRepo.GetRelativePath()+".bundle")

		output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
		require.Contains(t, string(output), "The bundle records a complete history")
	}
}

func TestRemoveRepository(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	ctx := testhelper.MergeIncomingMetadata(testhelper.Context(t), testcfg.GitalyServersMetadataFromCfg(t, cfg))

	for _, tc := range []struct {
		desc            string
		repositorySetup func() *gitalypb.Repository
	}{
		{
			desc: "with valid repository",
			repositorySetup: func() *gitalypb.Repository {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				return repo
			},
		},
		{
			desc: "with non-existent repository",
			repositorySetup: func() *gitalypb.Repository {
				return &gitalypb.Repository{StorageName: "default", RelativePath: "nonexistent"}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			require.NoError(t, removeRepository(ctx, pool, tc.repositorySetup()))
		})
	}
}
