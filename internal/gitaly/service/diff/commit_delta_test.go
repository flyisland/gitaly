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

func TestCommitDelta_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("old readme content\n"))
	newBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("new readme content\n"))
	uniqueBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("this is some unique content that we use in order to identify a rename\n"))

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "README.md", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "deleted-file", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "mode-change", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "mode-and-content-change", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "renamed-from", Mode: "100644", OID: uniqueBlob},
		gittest.TreeEntry{Path: "file\twith\nweird spaces", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "テスト.txt", Mode: "100644", OID: oldBlob},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "README.md", Mode: "100644", OID: newBlob},
		gittest.TreeEntry{Path: "mode-change", Mode: "100755", OID: oldBlob},
		gittest.TreeEntry{Path: "mode-and-content-change", Mode: "100755", OID: newBlob},
		gittest.TreeEntry{Path: "create-file", Mode: "100644", OID: newBlob},
		gittest.TreeEntry{Path: "renamed-to", Mode: "100644", OID: uniqueBlob},
		gittest.TreeEntry{Path: "file\twith\nweird spaces", Mode: "100644", OID: newBlob},
		gittest.TreeEntry{Path: "テスト.txt", Mode: "100644", OID: newBlob},
	))

	stream, err := client.CommitDelta(ctx, &gitalypb.CommitDeltaRequest{
		Repository:    repo,
		LeftCommitId:  leftCommit.String(),
		RightCommitId: rightCommit.String(),
	})
	require.NoError(t, err)

	expectedDeltas := []*gitalypb.CommitDelta{
		{
			FromId:   oldBlob.String(),
			ToId:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
		},
		{
			FromId:   gittest.DefaultObjectHash.ZeroOID.String(),
			ToId:     newBlob.String(),
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("create-file"),
			ToPath:   []byte("create-file"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     gittest.DefaultObjectHash.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("deleted-file"),
			ToPath:   []byte("deleted-file"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("file\twith\nweird spaces"),
			ToPath:   []byte("file\twith\nweird spaces"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("mode-and-content-change"),
			ToPath:   []byte("mode-and-content-change"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     oldBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("mode-change"),
			ToPath:   []byte("mode-change"),
		},
		{
			FromId:   uniqueBlob.String(),
			ToId:     uniqueBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("renamed-from"),
			ToPath:   []byte("renamed-to"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("テスト.txt"),
			ToPath:   []byte("テスト.txt"),
		},
	}

	assertExactReceivedDeltas(t, stream, expectedDeltas)
}

func TestCommitDelta_withPaths(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("old readme content\n"))
	newBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("new readme content\n"))

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "changed-but-excluded", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "content-change", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "deleted", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "mode-change", Mode: "100644", OID: oldBlob},
		gittest.TreeEntry{Path: "unchanged", Mode: "100644", OID: oldBlob},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "changed-but-excluded", Mode: "100644", OID: newBlob},
		gittest.TreeEntry{Path: "content-change", Mode: "100644", OID: newBlob},
		gittest.TreeEntry{Path: "mode-change", Mode: "100755", OID: oldBlob},
		gittest.TreeEntry{Path: "unchanged", Mode: "100644", OID: oldBlob},
	))

	stream, err := client.CommitDelta(ctx, &gitalypb.CommitDeltaRequest{
		Repository:    repo,
		LeftCommitId:  leftCommit.String(),
		RightCommitId: rightCommit.String(),
		Paths: [][]byte{
			// We include all changes except for "changed-but-excluded" here.
			[]byte("content-change"),
			[]byte("deleted"),
			[]byte("mode-change"),
			[]byte("unchanged"),
		},
	})
	require.NoError(t, err)

	expectedDeltas := []*gitalypb.CommitDelta{
		{
			FromId:   oldBlob.String(),
			ToId:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("content-change"),
			ToPath:   []byte("content-change"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     gittest.DefaultObjectHash.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("deleted"),
			ToPath:   []byte("deleted"),
		},
		{
			FromId:   oldBlob.String(),
			ToId:     oldBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("mode-change"),
			ToPath:   []byte("mode-change"),
		},
	}

	assertExactReceivedDeltas(t, stream, expectedDeltas)
}

func TestCommitDelta_validation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.CommitDeltaRequest
		expectedErr error
	}{
		{
			desc: "nonexistent repository",
			request: &gitalypb.CommitDeltaRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewStorageNotFoundError("fake")),
			),
		},
		{
			desc: "unset repository",
			request: &gitalypb.CommitDeltaRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "missing LeftCommitId",
			request: &gitalypb.CommitDeltaRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
		{
			desc: "missing RightCommitId",
			request: &gitalypb.CommitDeltaRequest{
				Repository:    repo,
				RightCommitId: "",
				LeftCommitId:  commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.CommitDelta(ctx, tc.request)
			require.NoError(t, err)
			testhelper.RequireGrpcError(t, tc.expectedErr, drainCommitDeltaResponse(stream))
		})
	}
}

func TestCommitDelta_nonexistentCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)
	nonExistentCommitID := gittest.DefaultObjectHash.HashData([]byte("some data"))

	stream, err := client.CommitDelta(ctx, &gitalypb.CommitDeltaRequest{
		Repository:    repo,
		LeftCommitId:  nonExistentCommitID.String(),
		RightCommitId: nonExistentCommitID.String(),
	})
	require.NoError(t, err)
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("exit status 128"), drainCommitDeltaResponse(stream))
}

func drainCommitDeltaResponse(c gitalypb.DiffService_CommitDeltaClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func assertExactReceivedDeltas(t *testing.T, client gitalypb.DiffService_CommitDeltaClient, expectedDeltas []*gitalypb.CommitDelta) {
	t.Helper()

	var actualDeltas []*gitalypb.CommitDelta
	for {
		fetchedDeltas, err := client.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		actualDeltas = append(actualDeltas, fetchedDeltas.GetDeltas()...)
	}

	testhelper.ProtoEqual(t, expectedDeltas, actualDeltas)
}
