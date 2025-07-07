package ref

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestFindLocalBranches(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	type setupData struct {
		request          *gitalypb.FindLocalBranchesRequest
		expectedErr      error
		expectedBranches []*gitalypb.Branch
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "empty request",
			setup: func(t *testing.T) setupData {
				return setupData{
					request:     &gitalypb.FindLocalBranchesRequest{},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				relativePath := gittest.NewRepositoryName(t)

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: &gitalypb.Repository{
							StorageName:  cfg.Storages[0].Name,
							RelativePath: relativePath,
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(
						structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, relativePath)),
					),
				}
			},
		},
		{
			desc: "invalid storage",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "fake",
							RelativePath: "repo",
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("fake"),
					)),
				}
			},
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
				}
			},
		},
		{
			desc: "only non-branch references",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath)
				for _, ref := range []git.ReferenceName{
					"refs/keep-around/kept",
					"refs/remotes/origin/remote-branch",
					"refs/something",
					"refs/tags/v1.0.0",
				} {
					gittest.WriteRef(t, cfg, repoPath, ref, commitID)
				}

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
				}
			},
		},
		{
			desc: "single branch",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				_, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch"))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch"), TargetCommit: commit},
					},
				}
			},
		},
		{
			desc: "many branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo)

				var expectedBranches []*gitalypb.Branch
				for i := 0; i < 100; i++ {
					ref := fmt.Sprintf("refs/heads/branch-%03d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commitID)
					expectedBranches = append(expectedBranches, &gitalypb.Branch{
						Name: []byte(ref), TargetCommit: commit,
					})
				}

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
					expectedBranches: expectedBranches,
				}
			},
		},
		{
			desc: "mixed branch and non-branch references",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch"))
				gittest.WriteRef(t, cfg, repoPath, "refs/tags/v1.0.0", commitID)

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch"), TargetCommit: commit},
					},
				}
			},
		},
		{
			desc: "commit with huge committer name",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				_, commit := writeCommit(t, ctx, cfg, repo,
					gittest.WithBranch("branch"),
					gittest.WithCommitterName(strings.Repeat("A", 100000)),
				)

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch"), TargetCommit: commit},
					},
				}
			},
		},
		{
			desc: "with pagination limit",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				_, commitA := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithMessage("commit a"))
				_, commitB := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-b"), gittest.WithMessage("commit b"))
				writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-c"), gittest.WithMessage("commit c"))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 2,
						},
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-a"), TargetCommit: commitA},
						{Name: []byte("refs/heads/branch-b"), TargetCommit: commitB},
					},
				}
			},
		},
		{
			desc: "with pagination limit and page token",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithMessage("commit a"))
				_, commitB := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-b"), gittest.WithMessage("commit b"))
				writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-c"), gittest.WithMessage("commit c"))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit:     1,
							PageToken: "refs/heads/branch-a",
						},
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-b"), TargetCommit: commitB},
					},
				}
			},
		},
		{
			desc: "with pagination limit and many filtered branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithMessage("commit a"))

				updater, err := updateref.New(ctx, gittest.NewRepositoryPathExecutor(t, cfg, repoPath))
				require.NoError(t, err)

				// Create a bunch of local branches. When we only ask for a very small subset, this will
				// cause us to `Wait()` for git-for-each-ref(1) early while it is still enumerating the
				// branches. We thus close its stdout early and then wait for it to terminate, which can
				// lead to the process receiving the EPIPE signal if we didn't take proper care.
				//
				// So the more references we have, the more likely it is that we'll see the issue.
				require.NoError(t, updater.Start())
				for i := 0; i < 1000; i++ {
					require.NoError(t, updater.Create(git.ReferenceName(fmt.Sprintf("refs/heads/branch-%d", i)), commitID))
				}
				require.NoError(t, updater.Commit())

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 2,
						},
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-0"), TargetCommit: commit},
						{Name: []byte("refs/heads/branch-1"), TargetCommit: commit},
					},
				}
			},
		},
		{
			desc: "invalid page token",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithMessage("commit a"))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: "refs/heads/does-not-exist",
						},
					},
					expectedErr: structerr.NewInvalidArgument("invalid page token: sending lines: could not find page token"),
				}
			},
		},
		{
			desc: "with valid page token but without limit",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithMessage("commit a"))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: "refs/heads/branch-a",
						},
					},
					expectedErr: structerr.NewInvalidArgument("invalid page token: sending lines: could not find page token"),
				}
			},
		},
		{
			desc: "sort by ascending name",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				_, commitA := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithCommitterDate(time.Date(2020, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitB := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-b"), gittest.WithCommitterDate(time.Date(2010, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitC := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-c"), gittest.WithCommitterDate(time.Date(2030, 1, 1, 1, 1, 1, 1, time.UTC)))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						SortBy:     gitalypb.FindLocalBranchesRequest_NAME,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-a"), TargetCommit: commitA},
						{Name: []byte("refs/heads/branch-b"), TargetCommit: commitB},
						{Name: []byte("refs/heads/branch-c"), TargetCommit: commitC},
					},
				}
			},
		},
		{
			desc: "sort by ascending committer date",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				_, commitA := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithCommitterDate(time.Date(2020, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitB := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-b"), gittest.WithCommitterDate(time.Date(2010, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitC := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-c"), gittest.WithCommitterDate(time.Date(2030, 1, 1, 1, 1, 1, 1, time.UTC)))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						SortBy:     gitalypb.FindLocalBranchesRequest_UPDATED_ASC,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-b"), TargetCommit: commitB},
						{Name: []byte("refs/heads/branch-a"), TargetCommit: commitA},
						{Name: []byte("refs/heads/branch-c"), TargetCommit: commitC},
					},
				}
			},
		},
		{
			desc: "sort by descending committer date",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				_, commitA := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-a"), gittest.WithCommitterDate(time.Date(2020, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitB := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-b"), gittest.WithCommitterDate(time.Date(2010, 1, 1, 1, 1, 1, 1, time.UTC)))
				_, commitC := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("branch-c"), gittest.WithCommitterDate(time.Date(2030, 1, 1, 1, 1, 1, 1, time.UTC)))

				return setupData{
					request: &gitalypb.FindLocalBranchesRequest{
						Repository: repo,
						SortBy:     gitalypb.FindLocalBranchesRequest_UPDATED_DESC,
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/heads/branch-c"), TargetCommit: commitC},
						{Name: []byte("refs/heads/branch-a"), TargetCommit: commitA},
						{Name: []byte("refs/heads/branch-b"), TargetCommit: commitB},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := client.FindLocalBranches(ctx, setup.request)
			require.NoError(t, err)

			branches, err := testhelper.ReceiveAndFold(stream.Recv, func(
				result []*gitalypb.Branch,
				response *gitalypb.FindLocalBranchesResponse,
			) []*gitalypb.Branch {
				return append(result, response.GetLocalBranches()...)
			})
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedBranches, branches)
		})
	}
}
