package diff

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestDiffStats_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	left := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "addition", Mode: "100644", Content: "a\nb\nc\n"},
		gittest.TreeEntry{Path: "deleted-file", Mode: "100644", Content: "deleted contents\n"},
		gittest.TreeEntry{Path: "deletion", Mode: "100644", Content: "a\nb\ndeleteme\nc\n"},
		gittest.TreeEntry{Path: "multiple-changes", Mode: "100644", Content: "a\nb\ncdelete\ndelete\ndelete\n\nd\ne\n"},
		gittest.TreeEntry{Path: "replace", Mode: "100644", Content: "a\nb\nreplaceme\nc\n"},
	))
	right := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "addition", Mode: "100644", Content: "a\nb\naddition\nc\n"},
		gittest.TreeEntry{Path: "created-file", Mode: "100644", Content: "created contents\n"},
		gittest.TreeEntry{Path: "deletion", Mode: "100644", Content: "a\nb\nc\n"},
		gittest.TreeEntry{Path: "multiple-changes", Mode: "100644", Content: "a\naddition\nb\nc\nd\neaddition\naddition\n"},
		gittest.TreeEntry{Path: "replace", Mode: "100644", Content: "a\nb\nreplaced\nc\n"},
	))

	stream, err := client.DiffStats(ctx, &gitalypb.DiffStatsRequest{
		Repository:    repo,
		LeftCommitId:  left.String(),
		RightCommitId: right.String(),
	})
	require.NoError(t, err)

	var actualStats []*gitalypb.DiffStats
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		actualStats = append(actualStats, response.GetStats()...)
	}

	testhelper.ProtoEqual(t, []*gitalypb.DiffStats{
		{
			Path:      []byte("addition"),
			Additions: 1,
		},
		{
			Path:      []byte("created-file"),
			Additions: 1,
		},
		{
			Path:      []byte("deleted-file"),
			Deletions: 1,
		},
		{
			Path:      []byte("deletion"),
			Deletions: 1,
		},
		{
			Path:      []byte("multiple-changes"),
			Additions: 4,
			Deletions: 5,
		},
		{
			Path:      []byte("replace"),
			Additions: 1,
			Deletions: 1,
		},
	}, actualStats)
}

func TestDiffStats_failures(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)
	missing := gittest.DefaultObjectHash.HashData([]byte("missing commit"))

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.DiffStatsRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.DiffStatsRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "repository not found",
			request: &gitalypb.DiffStatsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: "bar.git",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "bar.git")),
			),
		},
		{
			desc: "storage not found",
			request: &gitalypb.DiffStatsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "foo",
					RelativePath: "bar.git",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("foo"),
			)),
		},
		{
			desc: "left commit ID not set",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
		{
			desc: "right commit ID not set",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: "",
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
		{
			desc: "invalid left commit",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  "invalidinvalidinvalid",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewFailedPrecondition("exit status 128"),
		},
		{
			desc: "invalid right commit",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: "invalidinvalidinvalid",
			},
			expectedErr: structerr.NewFailedPrecondition("exit status 128"),
		},
		{
			desc: "left commit not found",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  missing.String(),
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewFailedPrecondition("exit status 128"),
		},
		{
			desc: "right commit not found",
			request: &gitalypb.DiffStatsRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: missing.String(),
			},
			expectedErr: structerr.NewFailedPrecondition("exit status 128"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.DiffStats(ctx, tc.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
