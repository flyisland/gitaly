package diff

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

func TestRawDiff_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	oldBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("old\n"))
	newBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("new\n"))
	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: oldBlob},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: newBlob},
	))

	stream, err := client.RawDiff(ctx, &gitalypb.RawDiffRequest{
		Repository:    repoProto,
		LeftCommitId:  leftCommit.String(),
		RightCommitId: rightCommit.String(),
	})
	require.NoError(t, err)

	rawDiff, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	}))
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf(`diff --git a/file b/file
index %s..%s 100644
--- a/file
+++ b/file
@@ -1 +1 @@
-old
+new
`, oldBlob, newBlob), string(rawDiff))
}

func TestRawDiff_inputValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.RawDiffRequest
		expectedErr error
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: "",
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawDiffRequest{
				Repository:    nil,
				RightCommitId: commit.String(),
				LeftCommitId:  commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.RawDiff(ctx, tc.request)
			require.NoError(t, err)
			testhelper.RequireGrpcError(t, tc.expectedErr, drainRawDiffResponse(stream))
		})
	}
}

func TestRawPatch_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	oldBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("old\n"))
	newBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("new\n"))
	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: oldBlob},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(leftCommit), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: newBlob},
	))

	stream, err := client.RawPatch(ctx, &gitalypb.RawPatchRequest{
		Repository:    repoProto,
		LeftCommitId:  leftCommit.String(),
		RightCommitId: rightCommit.String(),
	})
	require.NoError(t, err)

	rawPatch, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	}))
	require.NoError(t, err)

	// Note that the date in From is actually static. This is actually hardcoded in Git itself, please refer to
	// git-format-patch(1).
	expectedPatch := fmt.Sprintf(
		`From %s Mon Sep 17 00:00:00 2001
From: %s <%s>
Date: %s
Subject: [PATCH] message

---
 file | 2 +-
 1 file changed, 1 insertion(+), 1 deletion(-)

diff --git a/file b/file
index %s..%s 100644
--- a/file
+++ b/file
@@ -1 +1 @@
-old
+new
--%c
GitLab

`, rightCommit, gittest.DefaultCommitterName, gittest.DefaultCommitterMail, gittest.DefaultCommitTime.Format("Mon, 2 Jan 2006 15:04:05 -0700"), oldBlob[:7], newBlob[:7], ' ')
	require.Equal(t, expectedPatch, string(rawPatch))
}

func TestRawPatch_inputValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.RawPatchRequest
		expectedErr error
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				RightCommitId: "",
				LeftCommitId:  commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawPatchRequest{
				Repository:    nil,
				RightCommitId: commit.String(),
				LeftCommitId:  commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, _ := client.RawPatch(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, drainRawPatchResponse(stream))
		})
	}
}

func drainRawDiffResponse(c gitalypb.DiffService_RawDiffClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}

func drainRawPatchResponse(c gitalypb.DiffService_RawPatchClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
