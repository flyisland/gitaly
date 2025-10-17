package commit

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const (
	rubyContent = "puts 'Hello world!'\n"

	javaContent = `public class HelloWorldFactory {
	public static void main(String[] args){
		IHelloWorldFactory factory = HelloWorldFactory().getInstance();
		IHelloWorld helloWorld = factory.getHelloWorld();
		IHelloWorldString helloWorldString = helloWorld.getHelloWorldString();
		IPrintStrategy printStrategy = helloWorld.getPrintStrategy();
		helloWorld.print(printStrategy, helloWorldString%);
	}
}
`

	cContent = `#include <stdio.h>

	int main(const char *argv[]) {
		puts("Hello, world!")
	}
`
)

func TestCommitLanguages(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.SocketPath = startTestServices(t, cfg)
	client := newCommitServiceClient(t, cfg.SocketPath)

	type setupData struct {
		request          *gitalypb.CommitLanguagesRequest
		expectedErr      error
		expectedResponse *gitalypb.CommitLanguagesResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "no repository provided",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: nil,
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte("--output=/meow"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "empty revision falls back to default branch",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   nil,
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "C", Color: "#555555", Share: 100, Bytes: uint64(len(cContent)), LanguageId: 41},
						},
					},
				}
			},
		},
		{
			desc: "ambiguous revision",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Write both a branch and a tag named "v1.0.0" with different contents so that we can
				// verify which of both references has precedence.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("v1.0.0"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1.0.0"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.rb", Mode: "100644", Content: rubyContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte("v1.0.0"),
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "C", Color: "#555555", Share: 100, Bytes: uint64(len(cContent)), LanguageId: 41},
						},
					},
				}
			},
		},
		{
			desc: "multiple languages",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.rb", Mode: "100644", Content: rubyContent},
					gittest.TreeEntry{Path: "file.java", Mode: "100644", Content: javaContent},
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte(commit),
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "Java", Color: "#b07219", Share: 79.67145538330078, Bytes: uint64(len(javaContent)), LanguageId: 181},
							{Name: "C", Color: "#555555", Share: 16.221765518188477, Bytes: uint64(len(cContent)), LanguageId: 41},
							{Name: "Ruby", Color: "#701516", Share: 4.106776237487793, Bytes: uint64(len(rubyContent)), LanguageId: 326},
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.CommitLanguages(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}

func TestConcurrentCommitLanguages(t *testing.T) {
	t.Parallel()

	if testhelper.IsPraefectEnabled() {
		t.Skip("usage of gittest.WriteCommit causes a race condition.")
	}

	ctx := testhelper.Context(t)
	logger := testhelper.NewLogger(t)

	t.Run("concurrent commit languages with transactions", func(t *testing.T) {
		cfg := testcfg.Build(t)
		cfg.SocketPath = startTestServices(t, cfg)
		client := newCommitServiceClient(t, cfg.SocketPath)

		// Create a repository with some content for language detection
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		_, dbMgr := dbSetup(t, ctx, cfg, testhelper.TempDir(t), cfg.Storages[0].Name, logger)

		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		cmdFactory := gittest.NewCommandFactory(t, cfg)
		localRepoFactory := localrepo.NewFactory(logger, config.NewLocator(cfg), cmdFactory, catfileCache)

		m := partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))
		raftNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory := raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)

		partitionFactoryOptions := []partition.FactoryOption{
			partition.WithCmdFactory(cmdFactory),
			partition.WithRepoFactory(localRepoFactory),
			partition.WithMetrics(m),
			partition.WithRaftConfig(cfg.Raft),
			partition.WithRaftFactory(raftFactory),
		}
		partitionFactory := partition.NewFactory(partitionFactoryOptions...)

		ptnMgr, err := node.NewManager(cfg.Storages, storagemgr.NewFactory(
			logger, dbMgr, partitionFactory, config.DefaultMaxInactivePartitions, storagemgr.NewMetrics(cfg.Prometheus),
		))
		require.NoError(t, err)
		defer ptnMgr.Close()

		commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "file.rb", Mode: "100644", Content: rubyContent},
			gittest.TreeEntry{Path: "file.java", Mode: "100644", Content: javaContent},
			gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
		))

		request := &gitalypb.CommitLanguagesRequest{
			Repository: repoProto,
			Revision:   []byte(commit),
		}

		expectedResponse := &gitalypb.CommitLanguagesResponse{
			Languages: []*gitalypb.CommitLanguagesResponse_Language{
				{Name: "Java", Color: "#b07219", Share: 79.67145538330078, Bytes: uint64(len(javaContent)), LanguageId: 181},
				{Name: "C", Color: "#555555", Share: 16.221765518188477, Bytes: uint64(len(cContent)), LanguageId: 41},
				{Name: "Ruby", Color: "#701516", Share: 4.106776237487793, Bytes: uint64(len(rubyContent)), LanguageId: 326},
			},
		}
		storageHandle, err := ptnMgr.GetStorage(cfg.Storages[0].Name)
		require.NoError(t, err)
		// Test concurrent operations on the same commit with transactions
		const numGoroutines = 3
		var wg sync.WaitGroup
		results := make(chan *gitalypb.CommitLanguagesResponse, numGoroutines)
		errors := make(chan error, numGoroutines)

		// Launch concurrent operations (mix of read-only and read-write transactions)
		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()

				tx, err := storageHandle.Begin(ctx, storage.TransactionOptions{
					RelativePath: repo.GetRelativePath(),
					ReadOnly:     false,
				})
				if err != nil {
					errors <- err
					return
				}
				ctxWithTx := storage.ContextWithTransaction(ctx, tx)
				var committed bool
				defer func() {
					if !committed {
						require.NoError(t, tx.Rollback(ctxWithTx))
					}
				}()

				response, err := client.CommitLanguages(ctxWithTx, request)
				if err != nil {
					errors <- err
					return
				}

				commitLSN, err := tx.Commit(ctxWithTx)
				if err != nil {
					errors <- err
					return
				}
				committed = true
				results <- response
				storage.LogTransactionCommit(ctxWithTx, logger, commitLSN, fmt.Sprintf("CommitLanguages for commitID: %s", commit.String()))
			}()
		}

		wg.Wait()
		close(results)
		close(errors)

		// Count errors and successes
		var errorCount int
		var successCount int

		// Count errors (expect transaction conflicts)
		for err := range errors {
			errorCount++
			require.Error(t, err)
		}

		// Count successful responses
		for response := range results {
			successCount++
			testhelper.ProtoEqual(t, expectedResponse, response)
		}

		// Assert that for transactions it is expected to cancel all but the first one
		if testhelper.IsWALEnabled() {
			require.Equal(t, 1, successCount, "expected exactly one successful operation total")
			require.Equal(t, numGoroutines-1, errorCount, "expected all but one operation to fail")

		} else {
			require.Equal(t, numGoroutines, successCount, "expected exactly 3 successful operation total")
			require.Equal(t, 0, errorCount, "expected no operations to fail")
		}
	})
}
