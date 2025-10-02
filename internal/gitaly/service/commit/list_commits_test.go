package commit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListCommits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	committerYear := func(year int) gittest.WriteCommitOption {
		return gittest.WithCommitterDate(time.Date(year, 1, 1, 1, 1, 1, 1, time.UTC))
	}

	// We have a rather simple commit history with a single mainline branch and a feature branch with a single
	// commit that gets merged back into it.
	main0ID, main0 := writeCommit(t, ctx, cfg, repo, gittest.WithMessage("main 0"), committerYear(2000))
	main1ID, main1 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main0ID), gittest.WithMessage("main 1"), committerYear(2001))
	main2ID, main2 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main1ID), gittest.WithMessage("main 2"), committerYear(2002))
	main3ID, main3 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main2ID), gittest.WithMessage("main 3"), committerYear(2003), gittest.WithAuthorName("Different author"))
	main4ID, main4 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main3ID), gittest.WithMessage("main 4"), committerYear(2004))
	branchingID, branching := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main4ID), gittest.WithMessage("branching"), committerYear(2020))
	featureID, feature := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main4ID), gittest.WithMessage("feature"), committerYear(2005))
	mergeID, merge := writeCommit(t, ctx, cfg, repo, gittest.WithParents(branchingID, featureID), gittest.WithMessage("merge"), committerYear(2006), gittest.WithBranch("branch"))

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.ListCommitsRequest
		expectedCommits []*gitalypb.GitCommit
		expectedErr     error
	}{
		{
			desc: "single revision",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main3ID.String(),
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main3, main2, main1, main0,
			},
		},
		{
			desc: "single revision with limit",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main3ID.String(),
				},
				PaginationParams: &gitalypb.PaginationParameter{
					Limit: 2,
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main3, main2,
			},
		},
		{
			desc: "single revision with page token",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main3ID.String(),
				},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: main1ID.String(),
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main0,
			},
		},
		{
			desc: "revision range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					"^" + main1ID.String(),
					main3ID.String(),
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main3, main2,
			},
		},
		{
			desc: "reverse revision range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					"^" + main1ID.String(),
					main3ID.String(),
				},
				Reverse: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				main2, main3,
			},
		},
		{
			desc: "revisions with sort topo order",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				Order: gitalypb.ListCommitsRequest_TOPO,
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge, feature, branching, main4, main3, main2, main1, main0,
			},
		},
		{
			desc: "revisions with sort date order",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				Order: gitalypb.ListCommitsRequest_DATE,
			},
			expectedCommits: []*gitalypb.GitCommit{
				// Note how branching and feature are swapped compared to the preceding testcase.
				merge, branching, feature, main4, main3, main2, main1, main0,
			},
		},
		{
			desc: "revision with pseudo-revisions",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
					"--not",
					"--all",
				},
			},
			expectedCommits: nil,
		},
		{
			desc: "only non-merge commits",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				MaxParents: 1,
			},
			expectedCommits: []*gitalypb.GitCommit{
				// The merge commit is missing in this list.
				branching, feature, main4, main3, main2, main1, main0,
			},
		},
		{
			desc: "disabled walk",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				DisableWalk: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge,
			},
		},
		{
			desc: "first-parent",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				FirstParent: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				// We do not see the commit on the feature branch.
				merge, branching, main4, main3, main2, main1, main0,
			},
		},
		{
			desc: "author",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				Author: []byte("Different author"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				main3,
			},
		},
		{
			desc: "time range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				After:  timestamppb.New(time.Date(2002, 1, 1, 1, 1, 1, 1, time.UTC)),
				Before: timestamppb.New(time.Date(2004, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			expectedCommits: []*gitalypb.GitCommit{
				main4, main3, main2,
			},
		},
		{
			desc: "revisions by multiple message patterns",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				CommitMessagePatterns: [][]byte{
					[]byte("main 3"),
					[]byte("feature"),
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				feature, main3,
			},
		},
		{
			desc: "revisions by case insensitive commit message",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					mergeID.String(),
				},
				IgnoreCase: true,
				CommitMessagePatterns: [][]byte{
					[]byte("MAIN 3"),
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main3,
			},
		},
		{
			desc: "skip commits",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main3ID.String(),
				},
				Skip: 2,
			},
			expectedCommits: []*gitalypb.GitCommit{
				// Skip two commits from main3
				main1, main0,
			},
		},
		{
			desc: "empty paths parameter behaves like no filtering",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main1ID.String(),
				},
				Paths: [][]byte{},
			},
			expectedCommits: []*gitalypb.GitCommit{
				main1, main0,
			},
		},
		{
			desc: "paths parameter with non-existent path returns no commits",
			request: &gitalypb.ListCommitsRequest{
				Repository: repoProto,
				Revisions: []string{
					main1ID.String(),
				},
				Paths: [][]byte{[]byte("non-existent-file.txt")},
			},
			expectedCommits: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListCommits(ctx, tc.request)
			require.NoError(t, err)

			commits, err := testhelper.ReceiveAndFold(stream.Recv, func(result []*gitalypb.GitCommit, response *gitalypb.ListCommitsResponse) []*gitalypb.GitCommit {
				return append(result, response.GetCommits()...)
			})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedCommits, commits)
		})
	}
}

func TestListCommits_verify(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.ListCommitsRequest
		expectedErr error
	}{
		{
			desc:        "no repository provided",
			req:         &gitalypb.ListCommitsRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:        "no revisions",
			req:         &gitalypb.ListCommitsRequest{Repository: repo},
			expectedErr: structerr.NewInvalidArgument("missing revisions"),
		},
		{
			desc: "invalid revision",
			req:  &gitalypb.ListCommitsRequest{Repository: repo, Revisions: []string{"asdf", "-invalid"}},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "-invalid",
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListCommits(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
