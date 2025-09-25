package repository

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestFindMergeBase(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// We set up the following history with a criss-cross merge so that the merge base becomes ambiguous:
	//
	//        l1  l2  l3
	//        o---o---o
	//       / \   \ /
	// base o   \   X
	//       \   \ / \
	//        o---o---o
	//        r1  r2  r3
	base := gittest.WriteCommit(t, cfg, repoPath)
	unrelated := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unrelated"))
	// Normal commits.
	l1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("l1"), gittest.WithParents(base), gittest.WithBranch("l1"))
	r1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("r1"), gittest.WithParents(base), gittest.WithBranch("r1"))
	l2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(l1))
	// Simple merge commit.
	r2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(r1, l1))
	// Criss-cross merges.
	l3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(l2, r2))
	r3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(r2, l2))

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.FindMergeBaseRequest
		expectedErr      error
		expectedResponse *gitalypb.FindMergeBaseResponse
	}{
		{
			desc: "no repository provided",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "not enough revisions",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions:  [][]byte{[]byte(l1)},
			},
			expectedErr: structerr.NewInvalidArgument("at least 2 revisions are required"),
		},
		{
			desc: "simple merge base with object IDs",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte(l1),
					[]byte(r1),
				},
			},
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: base.String(),
			},
		},
		{
			desc: "simple merge base with references",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte("l1"),
					[]byte("r1"),
				},
			},
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: base.String(),
			},
		},
		{
			desc: "non-existent merge base",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte(base),
					[]byte(unrelated),
				},
			},
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: "",
			},
		},
		{
			desc: "non-existent branch",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte("l1"),
					[]byte("a-branch-that-does-not-exist"),
				},
			},
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: "",
			},
		},
		{
			desc: "multiple revisions",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte(l1),
					[]byte(l2),
					[]byte(r1),
				},
			},
			// With more than two revisions all but the first commit will be treated as a virtual merge
			// base. That is, in this Git will treat l2 and r1 as a single merge commit of those two
			// references. Consequentially, given that l1 is an ancestor of the merge of l2 and r1, it
			// becomes the best common ancestor.
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: l1.String(),
			},
		},
		{
			desc: "ambiguous merge base due to criss-cross merge",
			request: &gitalypb.FindMergeBaseRequest{
				Repository: repo,
				Revisions: [][]byte{
					[]byte(l3),
					[]byte(r3),
				},
			},
			// We return the best merge base returned by git-merge-base(1) even though it is ambiguous in
			// the case of criss-cross merges.
			expectedResponse: &gitalypb.FindMergeBaseResponse{
				Base: l2.String(),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.FindMergeBase(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
