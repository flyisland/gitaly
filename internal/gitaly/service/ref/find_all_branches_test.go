package ref

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestFindAllBranches(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	type setupData struct {
		request          *gitalypb.FindAllBranchesRequest
		expectedErr      error
		expectedBranches []*gitalypb.FindAllBranchesResponse_Branch
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "empty request",
			setup: func(t *testing.T) setupData {
				return setupData{
					request:     &gitalypb.FindAllBranchesRequest{},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "invalid storage",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
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
					request: &gitalypb.FindAllBranchesRequest{
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
					"refs/something",
					"refs/tags/v1.0.0",
				} {
					gittest.WriteRef(t, cfg, repoPath, ref, commitID)
				}

				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
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
					request: &gitalypb.FindAllBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.FindAllBranchesResponse_Branch{
						{Name: []byte("refs/heads/branch"), Target: commit},
					},
				}
			},
		},
		{
			desc: "many branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo)

				var expectedBranches []*gitalypb.FindAllBranchesResponse_Branch
				for i := 0; i < 100; i++ {
					ref := fmt.Sprintf("refs/heads/branch-%03d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commitID)
					expectedBranches = append(expectedBranches, &gitalypb.FindAllBranchesResponse_Branch{
						Name: []byte(ref), Target: commit,
					})
				}

				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
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
					request: &gitalypb.FindAllBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.FindAllBranchesResponse_Branch{
						{Name: []byte("refs/heads/branch"), Target: commit},
					},
				}
			},
		},
		{
			desc: "remote branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID, commit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("local-branch"))
				gittest.WriteRef(t, cfg, repoPath, "refs/remotes/origin/remote-branch", commitID)

				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
						Repository: repo,
					},
					expectedBranches: []*gitalypb.FindAllBranchesResponse_Branch{
						{Name: []byte("refs/heads/local-branch"), Target: commit},
						{Name: []byte("refs/remotes/origin/remote-branch"), Target: commit},
					},
				}
			},
		},
		{
			desc: "all merged branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				root := gittest.WriteCommit(t, cfg, repoPath)
				merged, mergedCommit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("merged"), gittest.WithParents(root), gittest.WithMessage("merged"))
				_, mainCommit := writeCommit(t, ctx, cfg, repo, gittest.WithBranch(git.DefaultBranch), gittest.WithParents(root, merged))

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unmerged"), gittest.WithParents(root), gittest.WithMessage("unmerged"))

				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
						Repository: repo,
						MergedOnly: true,
					},
					expectedBranches: []*gitalypb.FindAllBranchesResponse_Branch{
						{Name: []byte(git.DefaultRef), Target: mainCommit},
						{Name: []byte("refs/heads/merged"), Target: mergedCommit},
					},
				}
			},
		},
		{
			desc: "merged branches from a list of branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				root := gittest.WriteCommit(t, cfg, repoPath)
				mergedA, mergedCommitA := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("merged-a"), gittest.WithParents(root), gittest.WithMessage("merged A"))
				mergedB, _ := writeCommit(t, ctx, cfg, repo, gittest.WithBranch("merged-b"), gittest.WithParents(root), gittest.WithMessage("merged B"))
				writeCommit(t, ctx, cfg, repo, gittest.WithBranch(git.DefaultBranch), gittest.WithParents(root, mergedA, mergedB))

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unmerged"), gittest.WithParents(root), gittest.WithMessage("unmerged"))

				return setupData{
					request: &gitalypb.FindAllBranchesRequest{
						Repository: repo,
						MergedOnly: true,
						MergedBranches: [][]byte{
							[]byte("refs/heads/does-not-exist"),
							[]byte("refs/heads/merged-a"),
							[]byte("refs/heads/unmerged"),
						},
					},
					expectedBranches: []*gitalypb.FindAllBranchesResponse_Branch{
						{Name: []byte("refs/heads/merged-a"), Target: mergedCommitA},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := client.FindAllBranches(ctx, setup.request)
			require.NoError(t, err)

			branches, err := testhelper.ReceiveAndFold(stream.Recv, func(
				result []*gitalypb.FindAllBranchesResponse_Branch,
				response *gitalypb.FindAllBranchesResponse,
			) []*gitalypb.FindAllBranchesResponse_Branch {
				return append(result, response.GetBranches()...)
			})
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedBranches, branches)
		})
	}
}
