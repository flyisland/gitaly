package diff

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestDiffBlobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffService(t)

	cmdFactory, clean, err := gitcmd.NewExecCommandFactory(cfg, testhelper.SharedLogger(t))
	require.NoError(t, err)
	defer clean()

	gitVersion, err := cmdFactory.GitVersion(ctx)
	require.NoError(t, err)

	type setupData struct {
		request           *gitalypb.DiffBlobsRequest
		expectedResponses []*gitalypb.DiffBlobsResponse
		expectedErr       error
	}

	for _, tc := range []struct {
		setup func() setupData
		desc  string
	}{
		{
			desc: "invalid repository in request",
			setup: func() setupData {
				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: nil,
					},
					expectedErr: structerr.NewInvalidArgument("repository not set"),
				}
			},
		},
		{
			desc: "invalid blob pair in request",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID),
								RightBlob: nil,
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"getting right blob info: validating blob ID: invalid object ID: \"\", expected length %d, got 0",
						gittest.DefaultObjectHash.EncodedLen()).WithMetadata("revision", ""),
					),
				}
			},
		},
		{
			desc: "rawInfo entry missing old blob ID",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Status:    gitalypb.ChangedPaths_RENAMED,
								OldBlobId: "",
								NewBlobId: blobID.String(),
								Path:      []byte("foo"),
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("raw info entry missing blob IDs"),
				}
			},
		},
		{
			desc: "rawInfo entry missing new blob ID",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Status:    gitalypb.ChangedPaths_RENAMED,
								OldBlobId: blobID.String(),
								NewBlobId: "",
								Path:      []byte("foo"),
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("raw info entry missing blob IDs"),
				}
			},
		},
		{
			desc: "rawInfo rename entry missing path",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Status:    gitalypb.ChangedPaths_RENAMED,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
								Path:      nil,
								OldPath:   []byte("bar"),
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("raw info entry missing path"),
				}
			},
		},
		{
			desc: "rawInfo rename entry missing old path",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Status:    gitalypb.ChangedPaths_RENAMED,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
								Path:      []byte("foo"),
								OldPath:   nil,
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("rename/copy raw info entry missing old path"),
				}
			},
		},
		{
			desc: "commit ID in request",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				commitID := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID),
								RightBlob: []byte(commitID),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"getting right blob info: revision is not blob").WithMetadata("revision", string(commitID)),
					),
				}
			},
		},
		{
			desc: "not found path scoped blob revision in request",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte("HEAD:foo"),
								RightBlob: []byte(blobID),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"getting left blob info: getting revision info: object not found").WithMetadata("revision", "HEAD:foo"),
					),
				}
			},
		},
		{
			desc: "path scoped blob revision in request",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{
						OID:  blobID1,
						Mode: "100644",
						Path: "foo",
					},
				))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(fmt.Sprintf("%s:foo", commitID.String())),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "path scoped blob revision in request with attributes applied",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						OID:  blobID1,
						Mode: "100644",
						Path: "foo",
					},
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitattributes",
						Content: "foo binary",
					},
				))

				expectedResponse := []*gitalypb.DiffBlobsResponse{
					{
						LeftBlobId:  blobID1.String(),
						RightBlobId: blobID2.String(),
						Patch:       []byte(fmt.Sprintf("Binary files a/foo and b/%s differ\n", blobID2.String())),
						PatchSize: gittest.ObjectHashDependent(t, map[string]int32{
							"sha1":   73,
							"sha256": 97,
						}),
						Status: gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						Binary: true,
					},
				}

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte("HEAD:foo"),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: expectedResponse,
				}
			},
		},
		{
			desc: "single blob pair diffed",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "multiple blob pairs diffed",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
							{
								LeftBlob:  []byte(blobID2),
								RightBlob: []byte(blobID1),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
						{
							LeftBlobId:  blobID2.String(),
							RightBlobId: blobID1.String(),
							Patch:       []byte("@@ -1 +1 @@\n-bar\n+foo\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "single blob pair diff chunked across responses",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create large blobs that when diffed will span across response messages. The 14
				// byte offset here nicely aligns the chunks to make validation easier.
				data1 := strings.Repeat("f", msgSizeThreshold-14) + "\n"
				data2 := strings.Repeat("b", msgSizeThreshold-14) + "\n"

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte(data1))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte(data2))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte(fmt.Sprintf("@@ -1 +1 @@\n-%s", data1)),
							PatchSize:   10228,
						},
						{
							Patch:  []byte(fmt.Sprintf("+%s", data2)),
							Status: gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "binary blob pair diffed",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("\x000 foo"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("\x000 bar"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch: []byte(fmt.Sprintf("Binary files a/%s and b/%s differ\n",
								[]byte(blobID1),
								[]byte(blobID2),
							)),
							PatchSize: gittest.ObjectHashDependent(t, map[string]int32{
								"sha1":   110,
								"sha256": 158,
							}),
							Binary: true,
							Status: gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "word diff computed",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo bar baz\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo bob baz\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						DiffMode:   gitalypb.DiffBlobsRequest_DIFF_MODE_WORD,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n foo \n-bar\n+bob\n  baz\n~\n"),
							PatchSize:   36,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "whitespace_changes: dont_ignore",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo \n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+foo \n"),
							PatchSize:   23,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "whitespace_changes: ignore",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo \n"))
				// Prefix space is not ignored.
				blobID3 := gittest.WriteBlob(t, cfg, repoPath, []byte(" foo \n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository:        repoProto,
						WhitespaceChanges: gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID3),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(
						structerr.NewInvalidArgument("whitespace changes cannot be ignored when blob pairs are provided"),
					),
				}
			},
		},
		{
			desc: "whitespace_changes: ignore_all",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo \n"))
				blobID3 := gittest.WriteBlob(t, cfg, repoPath, []byte(" foo \n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository:        repoProto,
						WhitespaceChanges: gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE_ALL,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID3),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(
						structerr.NewInvalidArgument("whitespace changes cannot be ignored when blob pairs are provided"),
					),
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

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte(data1))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte(data2))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch: []byte(fmt.Sprintf("Binary files a/%s and b/%s differ\n",
								[]byte(blobID1),
								[]byte(blobID2),
							)),
							PatchSize: gittest.ObjectHashDependent(t, map[string]int32{
								"sha1":   110,
								"sha256": 158,
							}),
							Status: gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
							Binary: true,
						},
					},
				}
			},
		},
		{
			desc: "no newline at the end",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n\\ No newline at end of file\n+bar\n\\ No newline at end of file\n"),
							PatchSize:   78,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "null left blob ID",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.DefaultObjectHash.ZeroOID
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -0,0 +1 @@\n+bar\n"),
							PatchSize:   19,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "null right blob ID",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.DefaultObjectHash.ZeroOID

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +0,0 @@\n-foo\n"),
							PatchSize:   19,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "null left and right blob ID",
			setup: func() setupData {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.DefaultObjectHash.ZeroOID
				blobID2 := gittest.DefaultObjectHash.ZeroOID

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"left and right blob revisions resolve to same OID").WithMetadataItems(
						structerr.MetadataItem{Key: "left_revision", Value: blobID1.String()},
						structerr.MetadataItem{Key: "right_revision", Value: blobID2.String()},
					)),
				}
			},
		},
		{
			desc: "matching blob IDs",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID),
								RightBlob: []byte(blobID),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"left and right blob revisions resolve to same OID").WithMetadataItems(
						structerr.MetadataItem{Key: "left_revision", Value: blobID.String()},
						structerr.MetadataItem{Key: "right_revision", Value: blobID.String()},
					)),
				}
			},
		},
		{
			desc: "left and right revisions resolve to same OID",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{
						OID:  blobID1,
						Mode: "100644",
						Path: "foo",
					},
				))
				revision := fmt.Sprintf("%s:foo", commitID.String())

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(fmt.Sprintf("%s:foo", commitID.String())),
							},
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"left and right blob revisions resolve to same OID").WithMetadataItems(
						structerr.MetadataItem{Key: "left_revision", Value: blobID1.String()},
						structerr.MetadataItem{Key: "right_revision", Value: revision},
					)),
				}
			},
		},
		{
			desc: "empty file added",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Gitaly rewrites NULL OIDs to an empty blob ID. This allows addition/deletion
				// diffs to be generated through git-diff(1). If the added/deleted blob is also
				// empty, there is no diff according to Git because the pre-image and post-image
				// are identical.
				blobID1 := gittest.DefaultObjectHash.ZeroOID
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte(""))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "empty blob",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte(""))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -0,0 +1 @@\n+bar\n"),
							PatchSize:   19,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "diff limit exceeded",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
						PatchBytesLimit: 1,
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:          blobID1.String(),
							RightBlobId:         blobID2.String(),
							Status:              gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
							OverPatchBytesLimit: true,
							PatchSize:           22,
						},
					},
				}
			},
		},
		{
			desc: "single diff limit exceeded in batch",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))
				blobID3 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo bar baz\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID3),
							},
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
						PatchBytesLimit: 23,
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:          blobID1.String(),
							RightBlobId:         blobID3.String(),
							Status:              gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
							OverPatchBytesLimit: true,
							PatchSize:           30,
						},
						{
							LeftBlobId:          blobID1.String(),
							RightBlobId:         blobID2.String(),
							Patch:               []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							Status:              gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
							OverPatchBytesLimit: false,
							PatchSize:           22,
						},
					},
				}
			},
		},
		{
			desc: "blob info and raw info provided",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						BlobPairs: []*gitalypb.DiffBlobsRequest_BlobPair{
							{
								LeftBlob:  []byte(blobID1),
								RightBlob: []byte(blobID2),
							},
						},
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("file"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("blob pairs and raw info both used in request"),
				}
			},
		},
		{
			desc: "diff-pairs single diff",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("file"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "diff-pairs rename and copy",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("bar"),
								OldPath:   []byte("foo"),
								Status:    gitalypb.ChangedPaths_RENAMED,
								Score:     50,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
							{
								Path:      []byte("bar"),
								OldPath:   []byte("foo"),
								Status:    gitalypb.ChangedPaths_COPIED,
								Score:     100,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "diff-pairs multiple diffed",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar\n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository: repoProto,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("file1"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
							{
								Path:      []byte("file2"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+bar\n"),
							PatchSize:   22,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "diff-pairs whitespace_changes: ignore",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo \n"))
				// Prefix space is not ignored.
				blobID3 := gittest.WriteBlob(t, cfg, repoPath, []byte(" foo \n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository:        repoProto,
						WhitespaceChanges: gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("file1"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
							{
								Path:      []byte("file2"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID3.String(),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID3.String(),
							Patch:       []byte("@@ -1 +1 @@\n-foo\n+ foo \n"),
							PatchSize:   24,
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
		{
			desc: "diff-pairs whitespace_changes: ignore_all",
			setup: func() setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				blobID1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo\n"))
				blobID2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo \n"))
				blobID3 := gittest.WriteBlob(t, cfg, repoPath, []byte(" foo \n"))

				return setupData{
					request: &gitalypb.DiffBlobsRequest{
						Repository:        repoProto,
						WhitespaceChanges: gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE_ALL,
						RawInfo: []*gitalypb.ChangedPaths{
							{
								Path:      []byte("file1"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID2.String(),
							},
							{
								Path:      []byte("file2"),
								Status:    gitalypb.ChangedPaths_MODIFIED,
								OldMode:   0o100644,
								NewMode:   0o100644,
								OldBlobId: blobID1.String(),
								NewBlobId: blobID3.String(),
							},
						},
					},
					expectedResponses: []*gitalypb.DiffBlobsResponse{
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID2.String(),
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
						{
							LeftBlobId:  blobID1.String(),
							RightBlobId: blobID3.String(),
							Status:      gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH,
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup()

			if len(data.request.GetRawInfo()) > 0 && len(data.request.GetBlobPairs()) == 0 {
				if gittest.IsGitVersionLessThan(t, ctx, cfg, git.NewVersion(2, 49, 0, 1)) {
					data.expectedErr = structerr.NewInvalidArgument("git version: %s, doesn't support git-diff-pairs(1)", gitVersion)
					data.expectedResponses = nil
				}
			}

			stream, err := client.DiffBlobs(ctx, data.request)
			require.NoError(t, err)

			var actualResp []*gitalypb.DiffBlobsResponse
			for {
				var resp *gitalypb.DiffBlobsResponse

				resp, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					break
				}

				actualResp = append(actualResp, resp)
			}

			testhelper.RequireGrpcError(t, data.expectedErr, err)
			testhelper.ProtoEqual(t, data.expectedResponses, actualResp)
		})
	}
}

func BenchmarkDiffBlobs(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg, client := setupDiffService(b)
	repoProto, repoPath := gittest.CreateRepository(b, ctx, cfg)

	gittest.SkipIfGitVersionLessThan(b, ctx, cfg, git.NewVersion(2, 49, 0, 1),
		"git-diff-pairs(1) required")

	data1 := strings.Repeat("1\n", 1024) + "\n\n\n\na\n"
	data2 := strings.Repeat("2\n", 1024) + "\n\n\n\nb\n"

	blobID1 := gittest.WriteBlob(b, cfg, repoPath, []byte(data1))
	blobID2 := gittest.WriteBlob(b, cfg, repoPath, []byte(data2))

	var blobPairs []*gitalypb.DiffBlobsRequest_BlobPair
	var changedPaths []*gitalypb.ChangedPaths
	for i := 0; i < 1000; i++ {
		blobPairs = append(blobPairs, &gitalypb.DiffBlobsRequest_BlobPair{
			LeftBlob:  []byte(blobID1),
			RightBlob: []byte(blobID2),
		})

		changedPaths = append(changedPaths, &gitalypb.ChangedPaths{
			Path:      []byte("file"),
			Status:    gitalypb.ChangedPaths_MODIFIED,
			OldMode:   0o100644,
			NewMode:   0o100644,
			OldBlobId: blobID1.String(),
			NewBlobId: blobID2.String(),
		})
	}

	diffRequest := &gitalypb.DiffBlobsRequest{
		Repository: repoProto,
		BlobPairs:  blobPairs,
	}

	pairsRequest := &gitalypb.DiffBlobsRequest{
		Repository: repoProto,
		RawInfo:    changedPaths,
	}

	b.Run("DiffBlobs using git-diff(1)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			stream, err := client.DiffBlobs(ctx, diffRequest)
			require.NoError(b, err)

			for {
				_, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(b, err)
			}
		}
	})

	b.Run("DiffBlobs using git-diff-pairs(1)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			stream, err := client.DiffBlobs(ctx, pairsRequest)
			require.NoError(b, err)

			for {
				_, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(b, err)
			}
		}
	})
}
