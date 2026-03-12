package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestRepo_FetchRemote(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.FetchRemoteProactiveAuth).Run(t, testRepoFetchRemote)
}

func testRepoFetchRemote(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg, gitcmd.WithSkipHooks())
	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()
	locator := config.NewLocator(cfg)
	logger := testhelper.NewLogger(t)

	_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))
	tagID := gittest.WriteTag(t, cfg, remoteRepoPath, "v1.0.0", commitID.Revision(), gittest.WriteTagConfig{
		Message: "annotated tag",
	})

	initBareWithRemote := func(t *testing.T, remote string) (*Repo, string) {
		t.Helper()

		clientRepo, clientRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		cmd := gittest.NewCommand(t, cfg, "-C", clientRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			require.NoError(t, err)
		}

		return New(logger, locator, gitCmdFactory, catfileCache, clientRepo), clientRepoPath
	}

	t.Run("invalid name", func(t *testing.T) {
		repo := New(logger, locator, gitCmdFactory, catfileCache, nil)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, gitcmd.ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		repo := New(logger, locator, gitCmdFactory, catfileCache, repoProto)
		var stderr bytes.Buffer
		err := repo.FetchRemote(ctx, "stub", FetchOpts{Stderr: &stderr})
		require.Error(t, err)
		require.Contains(t, stderr.String(), "'stub' does not appear to be a git repository")
	})

	t.Run("ok", func(t *testing.T) {
		repo, repoPath := initBareWithRemote(t, "origin")

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Stderr: &stderr}))

		require.Empty(t, stderr.String(), "it should not produce output as it is called with --quiet flag by default")

		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Contains(t, refs, git.Reference{Name: "refs/remotes/origin/main", Target: commitID.String()})
		require.Contains(t, refs, git.Reference{Name: "refs/tags/v1.0.0", Target: tagID.String()})

		fetchedCommitID, err := repo.ResolveRevision(ctx, git.Revision("refs/remotes/origin/main^{commit}"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, commitID, fetchedCommitID)

		require.NoFileExists(t, filepath.Join(repoPath, "FETCH_HEAD"))
	})

	t.Run("with env", func(t *testing.T) {
		testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		repo := New(logger, locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", remoteRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --atomic --end-of-options source")
	})

	t.Run("with disabled transactions", func(t *testing.T) {
		testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		repo := New(logger, locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", remoteRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{
			Stderr:              &stderr,
			Env:                 []string{"GIT_TRACE=1"},
			DisableTransactions: true,
		}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --end-of-options source")
	})

	t.Run("with globals", func(t *testing.T) {
		testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		repo := New(logger, locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", remoteRepoPath)

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		// Write a commit into the remote's reference namespace that doesn't exist in the
		// remote and that would thus be pruned.
		gittest.WriteCommit(t, cfg, testRepoPath, gittest.WithReference("refs/remotes/source/markdown"))

		require.NoError(t, repo.FetchRemote(
			ctx,
			"source",
			FetchOpts{
				CommandOptions: []gitcmd.CmdOpt{
					gitcmd.WithConfig(gitcmd.ConfigPair{Key: "fetch.prune", Value: "true"}),
				},
			}),
		)

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with prune", func(t *testing.T) {
		testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		repo := New(logger, locator, gitCmdFactory, catfileCache, testRepo)

		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", remoteRepoPath)
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))
		// Write a commit into the remote's reference namespace that doesn't exist in the
		// remote and that would thus be pruned.
		gittest.WriteCommit(t, cfg, testRepoPath, gittest.WithReference("refs/remotes/source/markdown"))

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Prune: true}))

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with dry-run", func(t *testing.T) {
		repo, _ := initBareWithRemote(t, "origin")

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Stderr: &stderr, DryRun: true}))

		require.Empty(t, stderr.String(), "it should not produce output as it is called with --quiet flag by default")

		// Prior to the fetch, the repository did not contain any references. Consequently, we do
		// not expect the repository to contain any references because the fetch performed was a
		// dry-run.
		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Len(t, refs, 0)
	})

	t.Run("with porcelain", func(t *testing.T) {
		repo, _ := initBareWithRemote(t, "origin")

		// The porcelain fetch option write output to stdout in an easy-to-parse format. By default,
		// output is suppressed by the --quiet flag. The Verbose option must also be enabled to
		// receive output.
		var stdout bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{
			Stdout:    &stdout,
			Porcelain: true,
			Verbose:   true,
		}))

		hash, err := repo.ObjectHash(ctx)
		require.NoError(t, err)
		scanner := gitcmd.NewFetchPorcelainScanner(&stdout, hash)

		// Scan the output for expected references.
		require.True(t, scanner.Scan())
		require.Equal(t, gitcmd.RefUpdateTypeFetched, scanner.StatusLine().Type)
		require.Equal(t, "refs/remotes/origin/main", scanner.StatusLine().Reference)
		require.True(t, scanner.Scan())
		require.Equal(t, gitcmd.RefUpdateTypeFetched, scanner.StatusLine().Type)
		require.Equal(t, "refs/tags/v1.0.0", scanner.StatusLine().Reference)

		// Since the remote only contains two references, there should be nothing left in the buffer
		// to scan.
		require.False(t, scanner.Scan())
		require.Nil(t, scanner.Err())
	})

	t.Run("with no tags", func(t *testing.T) {
		repo, testRepoPath := initBareWithRemote(t, "origin")

		tagsBefore := gittest.Exec(t, cfg, "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsBefore)

		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Tags: FetchOptsTagsNone, Force: true}))

		tagsAfter := gittest.Exec(t, cfg, "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsAfter)

		containsBranches, err := repo.HasRevision(ctx, git.Revision("'test'"))
		require.NoError(t, err)
		require.False(t, containsBranches)

		containsTags, err := repo.HasRevision(ctx, git.Revision("v1.1.0"))
		require.NoError(t, err)
		require.False(t, containsTags)
	})

	t.Run("with invalid remote", func(t *testing.T) {
		repo, _ := initBareWithRemote(t, "origin")

		err := repo.FetchRemote(ctx, "doesnotexist", FetchOpts{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "fatal: 'doesnotexist' does not appear to be a git repository")
		require.IsType(t, err, FetchFailedError{})
	})

	t.Run("generates reverse index", func(t *testing.T) {
		repo, repoPath := initBareWithRemote(t, "origin")

		// The repository has no objects yet, so there shouldn't be any packfile either.
		packfiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
		require.NoError(t, err)
		require.Empty(t, packfiles)

		// Same goes for reverse indices, naturally.
		reverseIndices, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.rev"))
		require.NoError(t, err)
		require.Empty(t, reverseIndices)

		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{}))

		// After the fetch we should end up with a single packfile.
		packfiles, err = filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
		require.NoError(t, err)
		require.Len(t, packfiles, 1)

		// And furthermore, that packfile should have a reverse index.
		reverseIndices, err = filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.rev"))
		require.NoError(t, err)
		require.Len(t, reverseIndices, 1)
	})
}

// captureGitSSHCommand creates a new intercepting command factory which captures the
// GIT_SSH_COMMAND environment variable. The returned function can be used to read the variable's
// value.
func captureGitSSHCommand(tb testing.TB, ctx context.Context, cfg config.Cfg) (gitcmd.CommandFactory, func() ([]byte, error)) {
	envPath := filepath.Join(testhelper.TempDir(tb), "GIT_SSH_PATH")

	gitCmdFactory := gittest.NewInterceptingCommandFactory(tb, ctx, cfg, func(execEnv gitcmd.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			if test -z "${GIT_SSH_COMMAND+x}"
			then
				rm -f %q
			else
				echo -n "$GIT_SSH_COMMAND" >%q
			fi
			%q "$@"
		`, envPath, envPath, execEnv.BinaryPath)
	})

	return gitCmdFactory, func() ([]byte, error) {
		return os.ReadFile(envPath)
	}
}

func TestRepo_Push(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	gitCmdFactory, readSSHCommand := captureGitSSHCommand(t, ctx, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	locator := config.NewLocator(cfg)
	logger := testhelper.NewLogger(t)

	sourceRepoProto, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	sourceRepo := New(logger, locator, gitCmdFactory, catfileCache, sourceRepoProto)
	gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("master"))
	gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("feature"))

	setupPushRepo := func(tb testing.TB) (*Repo, string, []gitcmd.ConfigPair) {
		repoProto, repopath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return New(logger, locator, gitCmdFactory, catfileCache, repoProto), repopath, nil
	}

	setupDivergedRepo := func(tb testing.TB) (*Repo, string, []gitcmd.ConfigPair) {
		repoProto, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := New(logger, locator, gitCmdFactory, catfileCache, repoProto)

		// set up master as a diverging ref in push repo
		sourceMaster, err := sourceRepo.GetReference(ctx, "refs/heads/master")
		require.NoError(tb, err)

		require.NoError(tb, sourceRepo.Push(ctx, repoPath, []string{"refs/*"}, PushOptions{}))
		divergedMaster := gittest.WriteCommit(tb, cfg, repoPath,
			gittest.WithBranch("master"),
			gittest.WithParents(git.ObjectID(sourceMaster.Target)),
		)

		master, err := repo.GetReference(ctx, "refs/heads/master")
		require.NoError(tb, err)
		require.Equal(tb, master.Target, divergedMaster.String())

		return repo, repoPath, nil
	}

	for _, tc := range []struct {
		desc           string
		setupPushRepo  func(testing.TB) (*Repo, string, []gitcmd.ConfigPair)
		config         []gitcmd.ConfigPair
		sshCommand     string
		force          bool
		refspecs       []string
		errorMessage   string
		expectedFilter []string
	}{
		{
			desc:          "refspecs must be specified",
			setupPushRepo: setupPushRepo,
			errorMessage:  "refspecs to push must be explicitly specified",
		},
		{
			desc:           "push two refs",
			setupPushRepo:  setupPushRepo,
			refspecs:       []string{"refs/heads/master", "refs/heads/feature"},
			expectedFilter: []string{"refs/heads/master", "refs/heads/feature"},
		},
		{
			desc:           "push with custom ssh command",
			setupPushRepo:  setupPushRepo,
			sshCommand:     "custom --ssh-command",
			refspecs:       []string{"refs/heads/master"},
			expectedFilter: []string{"refs/heads/master"},
		},
		{
			desc:          "doesn't force push over diverged refs with Force unset",
			refspecs:      []string{"refs/heads/master"},
			setupPushRepo: setupDivergedRepo,
			errorMessage:  "Updates were rejected because the remote contains work that you do",
		},
		{
			desc:          "force pushes over diverged refs with Force set",
			refspecs:      []string{"refs/heads/master"},
			force:         true,
			setupPushRepo: setupDivergedRepo,
		},
		{
			desc:          "push all refs",
			setupPushRepo: setupPushRepo,
			refspecs:      []string{"refs/*"},
		},
		{
			desc:          "push empty refspec",
			setupPushRepo: setupPushRepo,
			refspecs:      []string{""},
			errorMessage:  `git push: exit status 128, stderr: "fatal: invalid refspec ''\n"`,
		},
		{
			desc: "invalid remote",
			setupPushRepo: func(tb testing.TB) (*Repo, string, []gitcmd.ConfigPair) {
				repoProto, _ := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				return New(logger, locator, gitCmdFactory, catfileCache, repoProto), "", nil
			},
			refspecs:     []string{"refs/heads/master"},
			errorMessage: `git push: exit status 128, stderr: "fatal: bad repository ''\n"`,
		},
		{
			desc: "in-memory remote",
			setupPushRepo: func(tb testing.TB) (*Repo, string, []gitcmd.ConfigPair) {
				repoProto, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				return New(logger, locator, gitCmdFactory, catfileCache, repoProto), "inmemory", []gitcmd.ConfigPair{
					{Key: "remote.inmemory.url", Value: repoPath},
				}
			},
			refspecs:       []string{"refs/heads/master"},
			expectedFilter: []string{"refs/heads/master"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			pushRepo, remote, remoteConfig := tc.setupPushRepo(t)

			err := sourceRepo.Push(ctx, remote, tc.refspecs, PushOptions{
				SSHCommand: tc.sshCommand,
				Force:      tc.force,
				Config:     remoteConfig,
			})
			if tc.errorMessage != "" {
				require.Contains(t, err.Error(), tc.errorMessage)
				return
			}
			require.NoError(t, err)

			gitSSHCommand, err := readSSHCommand()
			if !os.IsNotExist(err) {
				require.NoError(t, err)
			}

			require.Equal(t, tc.sshCommand, string(gitSSHCommand))

			actual, err := pushRepo.GetReferences(ctx)
			require.NoError(t, err)

			expected, err := sourceRepo.GetReferences(ctx, tc.expectedFilter...)
			require.NoError(t, err)

			require.Equal(t, expected, actual)
		})
	}
}
