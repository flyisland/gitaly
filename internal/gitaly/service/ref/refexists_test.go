package ref

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestRefExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath)
	for _, ref := range []git.ReferenceName{"refs/heads/master", "refs/tags/v1.0.0", "refs/heads/'test'", "refs/heads/ʕ•ᴥ•ʔ"} {
		gittest.WriteRef(t, cfg, repoPath, ref, commitID)
	}

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.RefExistsRequest
		expectedResponse *gitalypb.RefExistsResponse
		expectedErr      error
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.RefExistsRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid ref name",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("in valid"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid refname"),
		},
		{
			desc: "master",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "v1.0.0",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/tags/v1.0.0"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "quoted",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/'test'"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "unicode exists",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/ʕ•ᴥ•ʔ"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "unicode missing",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/tags/अस्तित्वहीन"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "spaces",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/ /heads"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "haxxors",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/; rm -rf /tmp/*"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "dashes",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("--"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid refname"),
		},
		{
			desc: "blank",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte(""),
			},
			expectedErr: structerr.NewInvalidArgument("invalid refname"),
		},
		{
			desc: "not tags or branches",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/remotes/origin"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "wildcards",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/*"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "invalid repos",
			request: &gitalypb.RefExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "invalid",
					RelativePath: "/etc/",
				},
				Ref: []byte("refs/heads/master"),
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("invalid"),
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.RefExists(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
