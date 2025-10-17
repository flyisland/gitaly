package commit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindAllCommits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// We have a rather simple commit history with a single mainline branch and a feature branch with a single
	// commit that gets merged back into it.
	main0ID, main0 := writeCommit(t, ctx, cfg, repo)
	main1ID, main1 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main0ID))
	main2ID, main2 := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main1ID))
	branchingID, branching := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main2ID), gittest.WithCommitterDate(time.Date(2020, 1, 1, 1, 1, 1, 1, time.UTC)))
	featureID, feature := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main2ID), gittest.WithCommitterDate(time.Date(2000, 1, 1, 1, 1, 1, 1, time.UTC)))
	_, merge := writeCommit(t, ctx, cfg, repo, gittest.WithParents(branchingID, featureID), gittest.WithBranch("branch"))
	_, different := writeCommit(t, ctx, cfg, repo, gittest.WithParents(main2ID), gittest.WithBranch("different"))

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.FindAllCommitsRequest
		expectedErr     error
		expectedCommits []*gitalypb.GitCommit
	}{
		{
			desc: "invalid repository",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "fake",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc:        "unset repository",
			request:     &gitalypb.FindAllCommitsRequest{},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Revision:   []byte("--output=/meow"),
			},
			expectedErr: status.Error(codes.InvalidArgument, "revision can't start with '-'"),
		},
		{
			desc: "all commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Revision:   []byte("branch"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge, branching, main2, main1, main0, feature,
			},
		},
		{
			desc: "maximum number of commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				MaxCount:   3,
				Revision:   []byte("branch"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge, branching, main2,
			},
		},
		{
			desc: "skipping number of commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Skip:       3,
				Revision:   []byte("branch"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				main1, main0, feature,
			},
		},
		{
			desc: "maximum number of commits of a revision plus skipping",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Skip:       2,
				MaxCount:   2,
				Revision:   []byte("branch"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				main2, main1,
			},
		},
		{
			desc: "all commits of a revision ordered by date",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Revision:   []byte("branch"),
				Order:      gitalypb.FindAllCommitsRequest_DATE,
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge, branching, feature, main2, main1, main0,
			},
		},
		{
			desc: "all commits of a revision ordered by topology",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Revision:   []byte("branch"),
				Order:      gitalypb.FindAllCommitsRequest_TOPO,
			},
			expectedCommits: []*gitalypb.GitCommit{
				// Note how feature and branching are ordered differently compared to the preceding
				// test.
				merge, feature, branching, main2, main1, main0,
			},
		},
		{
			desc: "all commits of all branches",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
			},
			expectedCommits: []*gitalypb.GitCommit{
				merge, branching, different, main2, main1, main0, feature,
			},
		},
		{
			desc: "non-existing revision",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repoProto,
				Revision:   []byte("i-do-not-exist"),
			},
			expectedCommits: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.FindAllCommits(ctx, tc.request)
			require.NoError(t, err)

			actualCommits, err := testhelper.ReceiveAndFold(stream.Recv, func(result []*gitalypb.GitCommit, response *gitalypb.FindAllCommitsResponse) []*gitalypb.GitCommit {
				return append(result, response.GetCommits()...)
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedCommits, actualCommits)
		})
	}
}
