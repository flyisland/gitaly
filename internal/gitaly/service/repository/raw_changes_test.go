package repository

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestGetRawChanges(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	type setupData struct {
		request         *gitalypb.GetRawChangesRequest
		expectedErr     error
		expectedChanges []*gitalypb.GetRawChangesResponse_RawChange
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "missing from-revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: "",
						ToRevision:   gittest.DefaultObjectHash.HashData([]byte("irrelevant")).String(),
					},
					expectedErr: structerr.NewInvalidArgument("invalid 'from' revision: %q", ""),
				}
			},
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request:     &gitalypb.GetRawChangesRequest{},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "missing commit",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				missingCommitID := gittest.DefaultObjectHash.HashData([]byte("nonexistent commit"))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: missingCommitID.String(),
						ToRevision:   missingCommitID.String(),
					},
					expectedErr: structerr.NewInvalidArgument("invalid 'from' revision: %q", missingCommitID),
				}
			},
		},
		{
			desc: "simple change",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "unchanged\n"},
				))

				toData := []byte("changed\n")
				toBlob := gittest.WriteBlob(t, cfg, repoPath, toData)

				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", OID: toBlob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       toBlob.String(),
							Size:         int64(len(toData)),
							OldPathBytes: []byte("path"),
							NewPathBytes: []byte("path"),
							Operation:    gitalypb.GetRawChangesResponse_RawChange_MODIFIED,
							OldMode:      0o100644,
							NewMode:      0o100644,
						},
					},
				}
			},
		},
		{
			desc: "comparison with zero OID",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blob := gittest.WriteBlob(t, cfg, repoPath, []byte("blob data\n"))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", OID: blob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: gittest.DefaultObjectHash.ZeroOID.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       blob.String(),
							Size:         10,
							NewPathBytes: []byte("path"),
							NewMode:      0o100644,
							Operation:    gitalypb.GetRawChangesResponse_RawChange_ADDED,
						},
					},
				}
			},
		},
		{
			desc: "mode change",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blob := gittest.WriteBlob(t, cfg, repoPath, []byte("unmodified\n"))
				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", OID: blob},
				))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100755", OID: blob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       blob.String(),
							Size:         11,
							OldPathBytes: []byte("path"),
							NewPathBytes: []byte("path"),
							OldMode:      0o100644,
							NewMode:      0o100755,
							Operation:    gitalypb.GetRawChangesResponse_RawChange_MODIFIED,
						},
					},
				}
			},
		},
		{
			desc: "special characters in path",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// We know that 'git diff --raw' sometimes quotes "special characters" in paths, and
				// that this can result in incorrect results from the GetRawChanges RPC, see
				// https://gitlab.com/gitlab-org/gitaly/issues/1444. The definition of "special" is
				// under core.quotePath in `git help config`.
				//
				// This test verifies that we correctly handle these special characters.
				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "テスト.txt", Mode: "100644", Content: "unchanged\n"},
				))

				toBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("changed\n"))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "テスト.txt", Mode: "100644", OID: toBlob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       toBlob.String(),
							Size:         8,
							OldPathBytes: []byte("テスト.txt"),
							NewPathBytes: []byte("テスト.txt"),
							OldMode:      0o100644,
							NewMode:      0o100644,
							Operation:    gitalypb.GetRawChangesResponse_RawChange_MODIFIED,
						},
					},
				}
			},
		},
		{
			desc: "invalid UTF-8 in path",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				nonUTF8Filename := "hello\x80world"
				require.False(t, utf8.ValidString(nonUTF8Filename))

				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: nonUTF8Filename, Mode: "100644", Content: "unchanged\n"},
				))
				toBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("changed\n"))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: nonUTF8Filename, Mode: "100644", OID: toBlob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       toBlob.String(),
							Size:         8,
							OldPathBytes: []byte(nonUTF8Filename),
							NewPathBytes: []byte(nonUTF8Filename),
							OldMode:      0o100644,
							NewMode:      0o100644,
							Operation:    gitalypb.GetRawChangesResponse_RawChange_MODIFIED,
						},
					},
				}
			},
		},
		{
			desc: "many files",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				fromBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("from\n"))
				toBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("to\n"))

				fromTreeEntries := make([]gittest.TreeEntry, 0, 1024)
				toTreeEntries := make([]gittest.TreeEntry, 0, 1024)
				expectedChanges := make([]*gitalypb.GetRawChangesResponse_RawChange, 0, 1024)
				for i := 0; i < 1024; i++ {
					path := fmt.Sprintf("path-%4d", i)

					fromTreeEntries = append(fromTreeEntries, gittest.TreeEntry{Path: path, Mode: "100644", OID: fromBlob})
					toTreeEntries = append(toTreeEntries, gittest.TreeEntry{Path: path, Mode: "100644", OID: toBlob})
					expectedChanges = append(expectedChanges, &gitalypb.GetRawChangesResponse_RawChange{
						BlobId:       toBlob.String(),
						Size:         3,
						OldPathBytes: []byte(path),
						NewPathBytes: []byte(path),
						OldMode:      0o100644,
						NewMode:      0o100644,
						Operation:    gitalypb.GetRawChangesResponse_RawChange_MODIFIED,
					})
				}

				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(fromTreeEntries...))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(toTreeEntries...))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: expectedChanges,
				}
			},
		},
		{
			desc: "rename",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blob := gittest.WriteBlob(t, cfg, repoPath, []byte("blob that is to be renamed\n"))
				from := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "from-path", Mode: "100644", OID: blob},
				))
				to := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "to-path", Mode: "100644", OID: blob},
				))

				return setupData{
					request: &gitalypb.GetRawChangesRequest{
						Repository:   repo,
						FromRevision: from.String(),
						ToRevision:   to.String(),
					},
					expectedChanges: []*gitalypb.GetRawChangesResponse_RawChange{
						{
							BlobId:       blob.String(),
							Size:         27,
							OldPathBytes: []byte("from-path"),
							NewPathBytes: []byte("to-path"),
							OldMode:      0o100644,
							NewMode:      0o100644,
							Operation:    gitalypb.GetRawChangesResponse_RawChange_RENAMED,
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := client.GetRawChanges(ctx, setup.request)
			require.NoError(t, err)

			var changes []*gitalypb.GetRawChangesResponse_RawChange
			for {
				var response *gitalypb.GetRawChangesResponse
				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}

					break
				}

				changes = append(changes, response.GetRawChanges()...)
			}

			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedChanges, changes)
		})
	}
}
