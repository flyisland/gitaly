package commit

import (
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCountCommits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch-1"))
	commit0 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithCommitterDate(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		gittest.WithBranch("branch-2"))
	commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithCommitterDate(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)),
		gittest.WithBranch("branch-2"), gittest.WithParents(commit0))

	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Mode: "100644", Path: "foo", Content: "bar"},
	})
	commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{OID: treeID, Mode: "040000", Path: "files"},
	), gittest.WithParents(commit1))

	createCommits(t, cfg, repoPath, "branch-2", 10, commit2)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.CountCommitsRequest
		expectedErr      error
		expectedResponse *gitalypb.CountCommitsResponse
	}{
		{
			desc: "all commits",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				All:        true,
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 14},
		},
		{
			desc: "all master",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-1"),
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 1},
		},
		{
			desc: "only master",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 13},
		},
		{
			desc: "with max count",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				All:        true,
				MaxCount:   5,
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 5},
		},
		{
			desc: "with before",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Before: &timestamppb.Timestamp{
					Seconds: time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 2},
		},
		{
			desc: "with after",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				After: &timestamppb.Timestamp{
					Seconds: time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 11},
		},
		{
			desc: "with before and after",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Before: &timestamppb.Timestamp{
					Seconds: time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
				After: &timestamppb.Timestamp{
					Seconds: time.Date(2000, 2, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 1},
		},
		{
			desc: "with path",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Path:       []byte("files/"),
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 1},
		},
		{
			desc: "with path regex",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Path:       []byte("files/*"),
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 1},
		},
		{
			desc: "with path and literal path specs",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Path:       []byte("files/*"),
				GlobalOptions: &gitalypb.GlobalOptions{
					LiteralPathspecs: true,
				},
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 0},
		},
		{
			desc: "with path and before and after",
			request: &gitalypb.CountCommitsRequest{
				Repository: repo,
				Revision:   []byte("branch-2"),
				Path:       []byte("files/*"),
				After: &timestamppb.Timestamp{
					Seconds: time.Date(2000, 2, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
				Before: &timestamppb.Timestamp{
					Seconds: time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC).Unix(),
				},
			},
			expectedResponse: &gitalypb.CountCommitsResponse{Count: 1},
		},
		{
			desc:    "repository doesn't exist",
			request: &gitalypb.CountCommitsRequest{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: []byte("branch-2")},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc:        "repository is nil",
			request:     &gitalypb.CountCommitsRequest{Repository: nil, Revision: []byte("branch-2")},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:        "revision is empty and all is false",
			request:     &gitalypb.CountCommitsRequest{Repository: repo, Revision: nil, All: false},
			expectedErr: status.Error(codes.InvalidArgument, "empty Revision and false All"),
		},
		{
			desc:        "revision is invalid",
			request:     &gitalypb.CountCommitsRequest{Repository: repo, Revision: []byte("--output=/meow"), All: false},
			expectedErr: status.Error(codes.InvalidArgument, "revision can't start with '-'"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.CountCommits(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, resp)
		})
	}
}
