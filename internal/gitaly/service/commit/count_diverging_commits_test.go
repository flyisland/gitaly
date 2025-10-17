package commit

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func createRepoWithDivergentBranches(t *testing.T, ctx context.Context, cfg config.Cfg, leftCommits, rightCommits int, leftBranchName, rightBranchName string) *gitalypb.Repository {
	/* create a branch structure as follows
	   	   a
	   	   |
	   	   b
	      / \
	     c   d
	     |   |
	     e   f
	     |   |
		 f   h
	*/

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	mainCommitOID := createCommits(t, cfg, repoPath, "main", 2, "")
	createCommits(t, cfg, repoPath, leftBranchName, leftCommits, mainCommitOID)
	createCommits(t, cfg, repoPath, rightBranchName, rightCommits, mainCommitOID)

	return repo
}

func TestSuccessfulCountDivergentCommitsRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	testRepo := createRepoWithDivergentBranches(t, ctx, cfg, 3, 3, "left", "right")

	testCases := []struct {
		leftRevision  string
		rightRevision string
		leftCount     int32
		rightCount    int32
	}{
		{
			leftRevision:  "left",
			rightRevision: "right",
			leftCount:     3,
			rightCount:    3,
		},
		{
			leftRevision:  "left^",
			rightRevision: "right",
			leftCount:     2,
			rightCount:    3,
		},
		{
			leftRevision:  "left",
			rightRevision: "right^",
			leftCount:     3,
			rightCount:    2,
		},
		{
			leftRevision:  "left^",
			rightRevision: "right^",
			leftCount:     2,
			rightCount:    2,
		},
		{
			leftRevision:  "main",
			rightRevision: "right",
			leftCount:     0,
			rightCount:    3,
		},
		{
			leftRevision:  "left",
			rightRevision: "main",
			leftCount:     3,
			rightCount:    0,
		},
		{
			leftRevision:  "main",
			rightRevision: "main",
			leftCount:     0,
			rightCount:    0,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%+v", testCase), func(t *testing.T) {
			request := &gitalypb.CountDivergingCommitsRequest{
				Repository: testRepo,
				From:       []byte(testCase.leftRevision),
				To:         []byte(testCase.rightRevision),
				MaxCount:   int32(1000),
			}
			response, err := client.CountDivergingCommits(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, testCase.leftCount, response.GetLeftCount())
			assert.Equal(t, testCase.rightCount, response.GetRightCount())
		})
	}
}

func TestSuccessfulCountDivergentCommitsRequestWithMaxCount(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	testRepo := createRepoWithDivergentBranches(t, ctx, cfg, 4, 4, "left", "right")

	testCases := []struct {
		leftRevision  string
		rightRevision string
		maxCount      int
	}{
		{
			leftRevision:  "left",
			rightRevision: "right",
			maxCount:      2,
		},
		{
			leftRevision:  "left",
			rightRevision: "right",
			maxCount:      3,
		},
		{
			leftRevision:  "left",
			rightRevision: "right",
			maxCount:      4,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%+v", testCase), func(t *testing.T) {
			request := &gitalypb.CountDivergingCommitsRequest{
				Repository: testRepo,
				From:       []byte(testCase.leftRevision),
				To:         []byte(testCase.rightRevision),
				MaxCount:   int32(testCase.maxCount),
			}
			response, err := client.CountDivergingCommits(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, testCase.maxCount, int(response.GetRightCount()+response.GetLeftCount()))
		})
	}
}

func TestFailedCountDivergentCommitsRequestDueToValidationError(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	revision := []byte(gittest.WriteCommit(t, cfg, repoPath))

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.CountDivergingCommitsRequest
		expectedErr error
	}{
		{
			desc:        "Repository not provided",
			req:         &gitalypb.CountDivergingCommitsRequest{Repository: nil, From: []byte("abcdef"), To: []byte("12345")},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "Repository doesn't exist",
			req:  &gitalypb.CountDivergingCommitsRequest{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, From: []byte("abcdef"), To: []byte("12345")},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc:        "From is empty",
			req:         &gitalypb.CountDivergingCommitsRequest{Repository: repo, From: nil, To: revision},
			expectedErr: status.Error(codes.InvalidArgument, "from and to are both required"),
		},
		{
			desc:        "To is empty",
			req:         &gitalypb.CountDivergingCommitsRequest{Repository: repo, From: revision, To: nil},
			expectedErr: status.Error(codes.InvalidArgument, "from and to are both required"),
		},
		{
			desc:        "From and To are empty",
			req:         &gitalypb.CountDivergingCommitsRequest{Repository: repo, From: nil, To: nil},
			expectedErr: status.Error(codes.InvalidArgument, "from and to are both required"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CountDivergingCommits(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
