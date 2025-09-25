package diff

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestCommitDiff(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	type setupData struct {
		expectedErr  error
		request      *gitalypb.CommitDiffRequest
		expectedDiff []*diff.Diff
	}

	for _, tc := range []struct {
		setup func() setupData
		desc  string
	}{
		{
			desc: "diff in single file",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random of string text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random string of text\n+random of string text\n"),
						},
					},
				}
			},
		},
		{
			desc: "diff for file deleted",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     gittest.DefaultObjectHash.ZeroOID.String(),
							OldMode:  0o100644,
							NewMode:  0,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +0,0 @@\n-random string of text\n"),
						},
					},
				}
			},
		},
		{
			desc: "file with multiple chunks",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-before.txt"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-after.txt"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-diff.txt"),
						},
					},
				}
			},
		},
		{
			desc: "new file with pluses",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("+\n++\n+++\n++++\n+++++\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath)
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   gittest.DefaultObjectHash.ZeroOID.String(),
							ToID:     blob2.String(),
							OldMode:  0,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -0,0 +1,5 @@\n++\n+++\n++++\n+++++\n++++++\n"),
						},
					},
				}
			},
		},
		{
			desc: "no diff in file",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: nil,
				}
			},
		},
		{
			desc: "mode diff in single file",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random of string text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100755", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100755,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random string of text\n+random of string text\n"),
						},
					},
				}
			},
		},
		{
			desc: "binary file",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				// Git detects binary files by looking for null characters
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("\x000 hello world"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("\x000 world hello"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100755", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100755,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Binary:   true,
							Patch:    []byte("Binary files a/foo and b/foo differ\n"),
						},
					},
				}
			},
		},
		{
			desc: "single file renamed with mode changes",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "goo", Mode: "100755", OID: blob1},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob1.String(),
							OldMode:  0o100644,
							NewMode:  0o100755,
							FromPath: []byte("foo"),
							ToPath:   []byte("goo"),
						},
					},
				}
			},
		},
		{
			desc: "no newline at the end",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random of string text"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random string of text\n\\ No newline at end of file\n+random of string text\n\\ No newline at end of file\n"),
						},
					},
				}
			},
		},
		{
			desc: "filename with tabs, newline",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath)
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo\tbar\ngoo.txt", Mode: "100644", OID: blob1},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   gittest.DefaultObjectHash.ZeroOID.String(),
							ToID:     blob1.String(),
							OldMode:  0,
							NewMode:  0o100644,
							FromPath: []byte("foo\tbar\ngoo.txt"),
							ToPath:   []byte("foo\tbar\ngoo.txt"),
							Patch:    []byte("@@ -0,0 +1 @@\n+random string of text\n"),
						},
					},
				}
			},
		},
		{
			desc: "filename with unicode",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("こんにちは世界\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath)
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "テスト.txt", Mode: "100644", OID: blob1},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   gittest.DefaultObjectHash.ZeroOID.String(),
							ToID:     blob1.String(),
							OldMode:  0,
							NewMode:  0o100644,
							FromPath: []byte("テスト.txt"),
							ToPath:   []byte("テスト.txt"),
							Patch:    []byte("@@ -0,0 +1 @@\n+こんにちは世界\n"),
						},
					},
				}
			},
		},
		{
			desc: "diff.noprefix set to true",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.Exec(t, cfg, "-C", repoPath, "config", "diff.noprefix", "true")

				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random string of text\n"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random of string text\n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random string of text\n+random of string text\n"),
						},
					},
				}
			},
		},
		{
			desc: "whitespace_changes: dont_ignore",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random text of string\n"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random text of string \n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:        repoProto,
						LeftCommitId:      commit1.String(),
						RightCommitId:     commit2.String(),
						WhitespaceChanges: gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_UNSPECIFIED,
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random text of string\n+random text of string \n"),
						},
					},
				}
			},
		},
		{
			desc: "whitespace_changes: ignore",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random text of string\n"))
				// prefix space is not ignored
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte(" random text of string \n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:        repoProto,
						LeftCommitId:      commit1.String(),
						RightCommitId:     commit2.String(),
						WhitespaceChanges: gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE,
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
							Patch:    []byte("@@ -1 +1 @@\n-random text of string\n+ random text of string \n"),
						},
					},
				}
			},
		},
		{
			desc: "whitespace_changes: ignore_all",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("random text of string\n"))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("random text of string \n"))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "foo", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:        repoProto,
						LeftCommitId:      commit1.String(),
						RightCommitId:     commit2.String(),
						WhitespaceChanges: gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE_ALL,
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("foo"),
							ToPath:   []byte("foo"),
						},
					},
				}
			},
		},
		{
			desc: "blobs exceeding core.bigFileThreshold",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// The blobs are crafted such that the huge common data will not be in the context of
				// the diff anymore to make this a bit more efficient.
				data1 := strings.Repeat("1", 50*1024*1024) + "\n\n\n\na\n"
				data2 := strings.Repeat("1", 50*1024*1024) + "\n\n\n\nb\n"

				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte(data1))
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte(data2))
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "huge", Mode: "100644", OID: blob1},
				))
				commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "huge", Mode: "100644", OID: blob2},
				))

				return setupData{
					request: &gitalypb.CommitDiffRequest{
						Repository:    repoProto,
						LeftCommitId:  commit1.String(),
						RightCommitId: commit2.String(),
					},
					expectedDiff: []*diff.Diff{
						{
							FromID:   blob1.String(),
							ToID:     blob2.String(),
							OldMode:  0o100644,
							NewMode:  0o100644,
							FromPath: []byte("huge"),
							ToPath:   []byte("huge"),
							Binary:   true,
							Patch:    []byte("Binary files a/huge and b/huge differ\n"),
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup()
			stream, err := client.CommitDiff(ctx, data.request)
			require.Equal(t, err, data.expectedErr)
			if err != nil {
				return
			}

			assertExactReceivedDiffs(t, stream, tc.setup().expectedDiff)
		})
	}
}

func TestCommitDiff_withPaths(t *testing.T) {
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

	stream, err := client.CommitDiff(ctx, &gitalypb.CommitDiffRequest{
		Repository:    repo,
		RightCommitId: rightCommit.String(),
		LeftCommitId:  leftCommit.String(),
		Paths: [][]byte{
			[]byte("content-change"),
			[]byte("deleted"),
			[]byte("mode-change"),
			[]byte("unchanged"),
		},
	})
	require.NoError(t, err)

	expectedDiffs := []*diff.Diff{
		{
			FromID:   oldBlob.String(),
			ToID:     newBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("content-change"),
			ToPath:   []byte("content-change"),
			Binary:   false,
			Patch:    []byte("@@ -1 +1 @@\n-old readme content\n+new readme content\n"),
		},
		{
			FromID:   oldBlob.String(),
			ToID:     gittest.DefaultObjectHash.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("deleted"),
			ToPath:   []byte("deleted"),
			Binary:   false,
			Patch:    []byte("@@ -1 +0,0 @@\n-old readme content\n"),
		},
		{
			FromID:   oldBlob.String(),
			ToID:     oldBlob.String(),
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("mode-change"),
			ToPath:   []byte("mode-change"),
			Binary:   false,
			Patch:    nil,
		},
	}

	assertExactReceivedDiffs(t, stream, expectedDiffs)
}

func TestCommitDiff_typeChange(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldSymlink := gittest.WriteBlob(t, cfg, repoPath, []byte("/etc/passwd\n"))
	newRegular := gittest.WriteBlob(t, cfg, repoPath, []byte("regular content\n"))

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "type-change", Mode: "120000", OID: oldSymlink},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "type-change", Mode: "100644", OID: newRegular},
	))

	stream, err := client.CommitDiff(ctx, &gitalypb.CommitDiffRequest{
		Repository:    repo,
		RightCommitId: rightCommit.String(),
		LeftCommitId:  leftCommit.String(),
	})
	require.NoError(t, err)

	expectedDiffs := []*diff.Diff{
		{
			FromID:   oldSymlink.String(),
			ToID:     gittest.DefaultObjectHash.ZeroOID.String(),
			OldMode:  0o120000,
			NewMode:  0,
			FromPath: []byte("type-change"),
			ToPath:   []byte("type-change"),
			Binary:   false,
			Patch:    []byte("@@ -1 +0,0 @@\n-/etc/passwd\n"),
		},
		{
			FromID:   gittest.DefaultObjectHash.ZeroOID.String(),
			ToID:     newRegular.String(),
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("type-change"),
			ToPath:   []byte("type-change"),
			Binary:   false,
			Patch:    []byte("@@ -0,0 +1 @@\n+regular content\n"),
		},
	}

	assertExactReceivedDiffs(t, stream, expectedDiffs)
}

func TestCommitDiff_ignoreWhitespaceChange(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldContent := gittest.WriteBlob(t, cfg, repoPath, []byte("\tfoo bar\n"))
	normalChange := gittest.WriteBlob(t, cfg, repoPath, []byte("\tbar foo\n"))
	whitespaceChange := gittest.WriteBlob(t, cfg, repoPath, []byte("    foo bar\n"))

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "normal-change", Mode: "100644", OID: oldContent},
		gittest.TreeEntry{Path: "whitespace-change", Mode: "100644", OID: oldContent},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "normal-change", Mode: "100644", OID: normalChange},
		gittest.TreeEntry{Path: "whitespace-change", Mode: "100644", OID: whitespaceChange},
	))

	expectedWhitespaceDiff := &diff.Diff{
		FromID:   oldContent.String(),
		ToID:     whitespaceChange.String(),
		OldMode:  0o100644,
		NewMode:  0o100644,
		FromPath: []byte("whitespace-change"),
		ToPath:   []byte("whitespace-change"),
		Binary:   false,
	}
	expectedNormalDiff := &diff.Diff{
		FromID:   oldContent.String(),
		ToID:     normalChange.String(),
		OldMode:  0o100644,
		NewMode:  0o100644,
		FromPath: []byte("normal-change"),
		ToPath:   []byte("normal-change"),
		Binary:   false,
		Patch:    []byte("@@ -1 +1 @@\n-\tfoo bar\n+\tbar foo\n"),
	}

	for _, tc := range []struct {
		desc          string
		paths         [][]byte
		expectedDiffs []*diff.Diff
	}{
		{
			desc:          "whitespace paths",
			paths:         [][]byte{[]byte("whitespace-change")},
			expectedDiffs: []*diff.Diff{expectedWhitespaceDiff},
		},
		{
			desc:          "whitespace paths and normal paths",
			paths:         [][]byte{[]byte("normal-change"), []byte("whitespace-change")},
			expectedDiffs: []*diff.Diff{expectedNormalDiff, expectedWhitespaceDiff},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.CommitDiff(ctx, &gitalypb.CommitDiffRequest{
				Repository:        repo,
				LeftCommitId:      leftCommit.String(),
				RightCommitId:     rightCommit.String(),
				WhitespaceChanges: gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE,
				Paths:             tc.paths,
			})
			require.NoError(t, err)

			assertExactReceivedDiffs(t, stream, tc.expectedDiffs)
		})
	}
}

func TestCommitDiff_wordDiff(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldContent := gittest.WriteBlob(t, cfg, repoPath, []byte("file with multiple words\n"))
	newContent := gittest.WriteBlob(t, cfg, repoPath, []byte("file with changed words\n"))

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "word-diff", Mode: "100644", OID: oldContent},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "word-diff", Mode: "100644", OID: newContent},
	))

	for _, tc := range []struct {
		desc           string
		noPrefixConfig string
	}{
		{
			desc:           "Git config diff.noprefix set to false",
			noPrefixConfig: "false",
		},
		{
			desc:           "Git config diff.noprefix set to true",
			noPrefixConfig: "true",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			gittest.Exec(t, cfg, "-C", repoPath, "config", "diff.noprefix", tc.noPrefixConfig)

			stream, err := client.CommitDiff(ctx, &gitalypb.CommitDiffRequest{
				Repository:    repo,
				LeftCommitId:  leftCommit.String(),
				RightCommitId: rightCommit.String(),
				DiffMode:      gitalypb.CommitDiffRequest_WORDDIFF,
			})
			require.NoError(t, err)

			assertExactReceivedDiffs(t, stream, []*diff.Diff{
				{
					FromID:   oldContent.String(),
					ToID:     newContent.String(),
					OldMode:  0o100644,
					NewMode:  0o100644,
					FromPath: []byte("word-diff"),
					ToPath:   []byte("word-diff"),
					Binary:   false,
					Patch:    []byte("@@ -1 +1 @@\n file with \n-multiple\n+changed\n  words\n~\n"),
				},
			})
		})
	}
}

func TestCommitDiff_limits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "a.aa", Mode: "100644", Content: "a\na\na\na\na\na\na\n"},
		gittest.TreeEntry{Path: "b.bb", Mode: "100644", Content: "b\nb\nb\n\nthis line is a bit longer and will change\nb\nb\nb\n"},
		gittest.TreeEntry{Path: "c.cc", Mode: "100644", Content: "will change up front\nc\nc\nc\nc\nc\nwill change in the end\n"},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "a.aa", Mode: "100644", Content: "a\na\na\nb\na\na\na\n"},
		gittest.TreeEntry{Path: "b.bb", Mode: "100644", Content: "b\nb\nb\n\nmultiple\nlines\nare\ninserted\ninto\nthe\nmiddle\nb\nb\nb\n"},
		gittest.TreeEntry{Path: "c.cc", Mode: "100644", Content: "some changes up front\nc\nc\nc\nc\nc\nsome changes in the end\n"},
	))

	type diffAttributes struct {
		path                                string
		collapsed, overflowMarker, tooLarge bool
	}

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.CommitDiffRequest
		expectedResult []diffAttributes
	}{
		{
			desc: "no enforcement",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: false,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{path: "c.cc"},
			},
		},
		{
			desc: "max file count enforcement",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      2,
				MaxLines:      9000,
				MaxBytes:      9000,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{overflowMarker: true},
			},
		},
		{
			desc: "max file count enforcement with collect all paths",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits:   true,
				MaxFiles:        2,
				MaxLines:        9000,
				MaxBytes:        9000,
				MaxPatchBytes:   9000,
				CollectAllPaths: true,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{path: "c.cc", overflowMarker: true},
			},
		},
		{
			desc: "max line count enforcement",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      9000,
				MaxLines:      30,
				MaxBytes:      9000,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{overflowMarker: true},
			},
		},
		{
			desc: "max line count enforcement with collect all paths",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits:   true,
				MaxFiles:        9000,
				MaxLines:        30,
				MaxBytes:        9000,
				MaxPatchBytes:   9000,
				CollectAllPaths: true,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{path: "c.cc", overflowMarker: true},
			},
		},
		{
			desc: "max byte count enforcement",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      9000,
				MaxLines:      9000,
				MaxBytes:      150,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{overflowMarker: true},
			},
		},
		{
			desc: "max byte count enforcement with collect all paths",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits:   true,
				MaxFiles:        9000,
				MaxLines:        9000,
				MaxBytes:        150,
				MaxPatchBytes:   9000,
				CollectAllPaths: true,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb"},
				{path: "c.cc", overflowMarker: true},
			},
		},
		{
			desc: "max patch bytes for file type",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits:                 true,
				MaxFiles:                      9000,
				MaxLines:                      9000,
				MaxBytes:                      9000,
				MaxPatchBytes:                 10,
				MaxPatchBytesForFileExtension: map[string]int32{".bb": 9000},
			},
			expectedResult: []diffAttributes{
				{path: "a.aa", tooLarge: true},
				{path: "b.bb", tooLarge: false},
				{path: "c.cc", tooLarge: true},
			},
		},
		{
			desc: "collapse after safe max file count is exceeded",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      9000,
				MaxLines:      9000,
				MaxBytes:      9000,
				SafeMaxFiles:  1,
				SafeMaxLines:  9000,
				SafeMaxBytes:  9000,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb", collapsed: true},
				{path: "c.cc", collapsed: true},
			},
		},
		{
			desc: "collapse after safe max line count is exceeded",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      9000,
				MaxLines:      9000,
				MaxBytes:      9000,
				SafeMaxFiles:  9000,
				SafeMaxLines:  20,
				SafeMaxBytes:  9000,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb", collapsed: true},
				{path: "c.cc"},
			},
		},
		{
			desc: "collapse after safe max byte count is exceeded",
			request: &gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      9000,
				MaxLines:      9000,
				MaxBytes:      9000,
				SafeMaxFiles:  9000,
				SafeMaxLines:  9000,
				SafeMaxBytes:  120,
				MaxPatchBytes: 9000,
			},
			expectedResult: []diffAttributes{
				{path: "a.aa"},
				{path: "b.bb", collapsed: true},
				{path: "c.cc"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			request := tc.request
			request.Repository = repo
			request.LeftCommitId = leftCommit.String()
			request.RightCommitId = rightCommit.String()

			stream, err := client.CommitDiff(ctx, request)
			require.NoError(t, err)

			receivedDiffs := getDiffsFromCommitDiffClient(t, stream)
			require.Equal(t, len(tc.expectedResult), len(receivedDiffs), "number of diffs received")

			for i, diff := range receivedDiffs {
				expectedDiff := tc.expectedResult[i]

				require.Equal(t, expectedDiff.overflowMarker, diff.OverflowMarker, "%s overflow marker", diff.FromPath)
				require.Equal(t, expectedDiff.tooLarge, diff.TooLarge, "%s too large", diff.FromPath)
				require.Equal(t, expectedDiff.path, string(diff.FromPath), "%s path", diff.FromPath)
				require.Equal(t, expectedDiff.collapsed, diff.Collapsed, "%s collapsed", diff.FromPath)

				if expectedDiff.collapsed {
					require.Empty(t, diff.Patch, "patch")
				}
			}
		})
	}
}

func TestCommitDiff_validation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.CommitDiffRequest
		expectedErr error
	}{
		{
			desc: "nonexistent repository",
			request: &gitalypb.CommitDiffRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "unset repository",
			request: &gitalypb.CommitDiffRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "RightCommitId is empty",
			request: &gitalypb.CommitDiffRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: "",
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
		{
			desc: "LeftCommitId is empty",
			request: &gitalypb.CommitDiffRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.CommitDiff(ctx, tc.request)
			require.NoError(t, err)
			testhelper.RequireGrpcError(t, tc.expectedErr, drainCommitDiffResponse(stream))
		})
	}
}

func TestCommitDiff_nonexistentCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)
	nonExistentCommitID := gittest.DefaultObjectHash.HashData([]byte("nonexistent commit"))

	stream, err := client.CommitDiff(ctx, &gitalypb.CommitDiffRequest{
		Repository:    repo,
		LeftCommitId:  nonExistentCommitID.String(),
		RightCommitId: nonExistentCommitID.String(),
	})
	require.NoError(t, err)

	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("eachDiff: exit status 128"), drainCommitDiffResponse(stream))
}

func getDiffsFromCommitDiffClient(t *testing.T, client gitalypb.DiffService_CommitDiffClient) []*diff.Diff {
	var diffs []*diff.Diff
	var currentDiff *diff.Diff

	for {
		fetchedDiff, err := client.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		if currentDiff == nil {
			currentDiff = &diff.Diff{
				FromID:         fetchedDiff.GetFromId(),
				ToID:           fetchedDiff.GetToId(),
				OldMode:        fetchedDiff.GetOldMode(),
				NewMode:        fetchedDiff.GetNewMode(),
				FromPath:       fetchedDiff.GetFromPath(),
				ToPath:         fetchedDiff.GetToPath(),
				Binary:         fetchedDiff.GetBinary(),
				Collapsed:      fetchedDiff.GetCollapsed(),
				OverflowMarker: fetchedDiff.GetOverflowMarker(),
				Patch:          fetchedDiff.GetRawPatchData(),
				TooLarge:       fetchedDiff.GetTooLarge(),
			}
		} else {
			currentDiff.Patch = append(currentDiff.Patch, fetchedDiff.GetRawPatchData()...)
		}

		if fetchedDiff.GetEndOfPatch() {
			diffs = append(diffs, currentDiff)
			currentDiff = nil
		}
	}

	return diffs
}

func drainCommitDiffResponse(c gitalypb.DiffService_CommitDiffClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func assertExactReceivedDiffs(t *testing.T, client gitalypb.DiffService_CommitDiffClient, expectedDiffs []*diff.Diff) {
	t.Helper()
	require.Equal(t, expectedDiffs, getDiffsFromCommitDiffClient(t, client))
}
