package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestFindAllRemoteBranches(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	type setupData struct {
		request          *gitalypb.FindAllRemoteBranchesRequest
		expectedErr      error
		expectedBranches []*gitalypb.Branch
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "unknown storage",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "fake",
							RelativePath: "repo",
						},
						RemoteName: "stub",
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("fake"),
					)),
				}
			},
		},
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						RemoteName: "myRemote",
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "unset remote name",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: repo,
					},
					expectedErr: structerr.NewInvalidArgument("empty RemoteName"),
				}
			},
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: repo,
						RemoteName: "origin",
					},
				}
			},
		},
		{
			desc: "no remote branches",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath)
				for _, ref := range []git.ReferenceName{
					"refs/heads/branch",
					"refs/tags/tag",
					"refs/something",
					"refs/keep-around/keep-around",
				} {
					gittest.WriteRef(t, cfg, repoPath, ref, commitID)
				}

				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: repo,
						RemoteName: "origin",
					},
				}
			},
		},
		{
			desc: "different remote",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/remotes/origin/branch"))

				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: repo,
						RemoteName: "does-not-exist",
					},
				}
			},
		},
		{
			desc: "mixed requested and unrequested remote references",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commit1ID, commit1 := writeCommit(t, ctx, cfg, repo, gittest.WithMessage("commit 1"))
				gittest.WriteRef(t, cfg, repoPath, "refs/remotes/origin/branch-1", commit1ID)
				gittest.WriteRef(t, cfg, repoPath, "refs/remotes/unrelated/branch-1", commit1ID)

				commit2ID, commit2 := writeCommit(t, ctx, cfg, repo, gittest.WithMessage("commit 2"))
				gittest.WriteRef(t, cfg, repoPath, "refs/remotes/origin/branch-2", commit2ID)
				gittest.WriteRef(t, cfg, repoPath, "refs/remotes/unrelated/branch-2", commit2ID)

				return setupData{
					request: &gitalypb.FindAllRemoteBranchesRequest{
						Repository: repo,
						RemoteName: "origin",
					},
					expectedBranches: []*gitalypb.Branch{
						{Name: []byte("refs/remotes/origin/branch-1"), TargetCommit: commit1},
						{Name: []byte("refs/remotes/origin/branch-2"), TargetCommit: commit2},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := client.FindAllRemoteBranches(ctx, setup.request)
			require.NoError(t, err)

			branches, err := testhelper.ReceiveAndFold(stream.Recv, func(
				result []*gitalypb.Branch,
				response *gitalypb.FindAllRemoteBranchesResponse,
			) []*gitalypb.Branch {
				return append(result, response.GetBranches()...)
			})
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedBranches, branches)
		})
	}
}
