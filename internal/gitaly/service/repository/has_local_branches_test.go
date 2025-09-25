package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestHasLocalBranches_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	populatedRepo, populatedRepoPath := gittest.CreateRepository(t, ctx, cfg)
	gittest.WriteCommit(t, cfg, populatedRepoPath, gittest.WithBranch("main"))

	emptyRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.HasLocalBranchesRequest
		expectedResponse *gitalypb.HasLocalBranchesResponse
	}{
		{
			desc: "repository has branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: populatedRepo,
			},
			expectedResponse: &gitalypb.HasLocalBranchesResponse{
				Value: true,
			},
		},
		{
			desc: "repository doesn't have branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: emptyRepo,
			},
			expectedResponse: &gitalypb.HasLocalBranchesResponse{
				Value: false,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.HasLocalBranches(ctx, tc.request)
			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestHasLocalBranches_failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)

	for _, tc := range []struct {
		desc        string
		repository  *gitalypb.Repository
		expectedErr error
	}{
		{
			desc:        "repository nil",
			repository:  nil,
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "repository doesn't exist",
			repository: &gitalypb.Repository{
				StorageName:  "fake",
				RelativePath: "path",
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.HasLocalBranchesRequest{Repository: tc.repository}
			_, err := client.HasLocalBranches(ctx, request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
