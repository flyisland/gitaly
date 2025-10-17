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

func TestListTagNamesContainingCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitA := gittest.WriteCommit(t, cfg, repoPath)
	commitB := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commitA))
	commitC := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commitB), gittest.WithBranch(git.DefaultBranch))

	gittest.WriteTag(t, cfg, repoPath, "annotated", commitA.Revision(), gittest.WriteTagConfig{
		Message: "annotated",
	})
	gittest.WriteTag(t, cfg, repoPath, "lightweight", commitB.Revision())

	for _, tc := range []struct {
		desc         string
		request      *gitalypb.ListTagNamesContainingCommitRequest
		expectedErr  error
		expectedTags []string
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid commit ID",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "invalid",
			},
			expectedErr: structerr.NewInvalidArgument(
				`invalid object ID: "invalid", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen(),
			),
		},
		{
			desc: "no commit ID",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "",
			},
			expectedErr: structerr.NewInvalidArgument(
				`invalid object ID: "", expected length %d, got 0`, gittest.DefaultObjectHash.EncodedLen(),
			),
		},
		{
			desc: "commit not contained in any tag",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitC.String(),
			},
			expectedTags: nil,
		},
		{
			desc: "root commit",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitA.String(),
			},
			expectedTags: []string{"annotated", "lightweight"},
		},
		{
			desc: "root commit with limit",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitA.String(),
				Limit:      1,
			},
			expectedTags: []string{"annotated"},
		},
		{
			desc: "commit with single tag",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitB.String(),
			},
			expectedTags: []string{"lightweight"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListTagNamesContainingCommit(ctx, tc.request)
			require.NoError(t, err)

			tagNames, err := testhelper.ReceiveAndFold(stream.Recv, func(result []string, response *gitalypb.ListTagNamesContainingCommitResponse) []string {
				for _, tagName := range response.GetTagNames() {
					result = append(result, string(tagName))
				}
				return result
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.ElementsMatch(t, tc.expectedTags, tagNames)
		})
	}
}

func TestListBranchNamesContainingCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	rootCommitID := gittest.WriteCommit(t, cfg, repoPath)
	intermediateCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(rootCommitID))
	headCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(intermediateCommitID), gittest.WithBranch(git.DefaultBranch))
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(intermediateCommitID), gittest.WithBranch("branch"))

	ambiguousCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("ambiguous"), gittest.WithParents(intermediateCommitID))
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/heads/"+ambiguousCommitID.String()), ambiguousCommitID)

	unreferencedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreferenced"))

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.ListBranchNamesContainingCommitRequest
		expectedErr      error
		expectedBranches []string
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid commit",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "invalid",
			},
			expectedErr: structerr.NewInvalidArgument(
				`invalid object ID: "invalid", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen(),
			),
		},
		{
			desc: "no commit ID",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "",
			},
			expectedErr: structerr.NewInvalidArgument(
				`invalid object ID: "", expected length %v, got 0`, gittest.DefaultObjectHash.EncodedLen(),
			),
		},
		{
			desc: "current HEAD",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   headCommitID.String(),
			},
			expectedBranches: []string{git.DefaultBranch, "branch"},
		},
		{
			desc: "branch name is also commit id",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   ambiguousCommitID.String(),
			},
			expectedBranches: []string{ambiguousCommitID.String()},
		},
		{
			desc: "initial commit",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   rootCommitID.String(),
			},
			expectedBranches: []string{
				git.DefaultBranch,
				"branch",
				ambiguousCommitID.String(),
			},
		},
		{
			desc: "commit without references",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   unreferencedCommitID.String(),
			},
			expectedBranches: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListBranchNamesContainingCommit(ctx, tc.request)
			require.NoError(t, err)

			branchNames, err := testhelper.ReceiveAndFold(stream.Recv, func(result []string, response *gitalypb.ListBranchNamesContainingCommitResponse) []string {
				for _, branchName := range response.GetBranchNames() {
					result = append(result, string(branchName))
				}
				return result
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.ElementsMatch(t, tc.expectedBranches, branchNames)
		})
	}
}
