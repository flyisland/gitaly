package commit

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestCommitStatsSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	initialCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "a", Content: "1\n2\n3\n4\n5\n", Mode: "100644"},
		gittest.TreeEntry{Path: "b", Content: "1\n2\n3\n4\n5\n", Mode: "100644"},
		gittest.TreeEntry{Path: "binary", Content: "1\n2\n\000\n4\n5\n", Mode: "100644"},
	))

	multipleChanges := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(initialCommit),
		gittest.WithBranch("branch"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "a", Content: "1\n2\n3\n4\n4.a\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "b", Content: "1\n3\n4\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "binary", Content: "1\n2\n\000\n4\n5\n", Mode: "100644"},
		),
	)

	binaryChange := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(initialCommit),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "a", Content: "1\n2\n3\n4\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "b", Content: "1\n2\n3\n4\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "binary", Content: "a\n2\n\000\n4\nd\n", Mode: "100644"},
		))

	merge := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(multipleChanges, binaryChange),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "a", Content: "1\n2\n3\n4\n4.a\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "b", Content: "1\n3\n4\n5\n", Mode: "100644"},
			gittest.TreeEntry{Path: "binary", Content: "a\n2\n\000\n4\nd\n", Mode: "100644"},
		),
	)

	rogueMerge := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(multipleChanges, binaryChange),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "a", Content: "1\n2\n3\n4\n4.a\n5\n", Mode: "100644"},
			// We have a rogue merge here because "b" is different from both parents.
			gittest.TreeEntry{Path: "b", Content: "1\n3\n4\n6\n", Mode: "100644"},
			gittest.TreeEntry{Path: "binary", Content: "a\n2\n\000\n4\nd\n", Mode: "100644"},
		),
	)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.CommitStatsRequest
		expectedErr      error
		expectedResponse *gitalypb.CommitStatsResponse
	}{
		{
			desc: "no repository provided",
			request: &gitalypb.CommitStatsRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "repo not found",
			request: &gitalypb.CommitStatsRequest{
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
			request: &gitalypb.CommitStatsRequest{
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
			desc: "ref not found",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte("non/existing"),
			},
			expectedErr: structerr.NewInternal("object not found"),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte("--outpu=/meow"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "multiple changes, multiple files",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte("branch"),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       multipleChanges.String(),
				Additions: 1,
				Deletions: 1,
				Files:     2,
			},
		},
		{
			desc: "multiple changes, multiple files, reference by commit ID",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte(multipleChanges),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       multipleChanges.String(),
				Additions: 1,
				Deletions: 1,
				Files:     2,
			},
		},
		{
			desc: "merge",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte(merge),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       merge.String(),
				Additions: 0,
				Deletions: 0,
				Files:     1,
			},
		},
		{
			desc: "rogue merge",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte(rogueMerge),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       rogueMerge.String(),
				Additions: 1,
				Deletions: 1,
				Files:     2,
			},
		},
		{
			desc: "binary file modified",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte(binaryChange),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       binaryChange.String(),
				Additions: 0,
				Deletions: 0,
				Files:     1,
			},
		},
		{
			desc: "initial commit",
			request: &gitalypb.CommitStatsRequest{
				Repository: repo,
				Revision:   []byte(initialCommit),
			},
			expectedResponse: &gitalypb.CommitStatsResponse{
				Oid:       initialCommit.String(),
				Additions: 10,
				Deletions: 0,
				Files:     3,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.CommitStats(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
