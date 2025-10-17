package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestTreeEntry(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	blobContent := []byte("1234567890")
	blobID := gittest.WriteBlob(t, cfg, repoPath, blobContent)

	blobWithSpacesContent := []byte("space")
	blobWithSpacesID := gittest.WriteBlob(t, cfg, repoPath, blobWithSpacesContent)

	submoduleCommit := gittest.DefaultObjectHash.HashData([]byte("submodule commit"))

	subfileContent := []byte("subfile content")
	subfileID := gittest.WriteBlob(t, cfg, repoPath, subfileContent)
	subtreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subfile", Mode: "100644", OID: subfileID},
	})
	subtreeSize := gittest.ObjectSize(t, cfg, repoPath, subtreeID)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "blob", Mode: "100644", OID: blobID},
		gittest.TreeEntry{Path: "blob with spaces", Mode: "100644", OID: blobWithSpacesID},
		gittest.TreeEntry{Path: "submodule", Mode: "160000", OID: submoduleCommit},
		gittest.TreeEntry{Path: "subtree", Mode: "040000", OID: subtreeID},
	))

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.TreeEntryRequest
		expectedErr       error
		expectedResponses []*gitalypb.TreeEntryResponse
	}{
		{
			desc: "missing repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Revision: []byte(commitID),
				Path:     []byte("blob"),
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "unset repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: nil,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   nil,
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("empty revision"),
		},
		{
			desc: "empty path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
			},
			expectedErr: structerr.NewInvalidArgument("empty Path"),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("--output=/meow"),
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "negative limit",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative Limit"),
		},
		{
			desc: "negative maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative MaxSize"),
		},
		{
			desc: "object size exceeds maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    5,
			},
			expectedErr: structerr.NewFailedPrecondition("object size (10) is bigger than the maximum allowed size (5)"),
		},
		{
			desc: "path escapes repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				// Git blows up on paths like this
				Path: []byte("../bar/.gitkeep"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "../bar/.gitkeep"),
		},
		{
			desc: "missing file with space in path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("with space/README.md"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "with space/README.md"),
		},
		{
			desc: "missing file",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("missing.rb"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "missing.rb"),
		},
		{
			desc: "without limits",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "with limit exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      20,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "with maximum size exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    20,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "path with space",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob with spaces"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobWithSpacesID.String(),
					Size: int64(len(blobWithSpacesContent)),
					Mode: 0o100644,
					Data: blobWithSpacesContent,
				},
			},
		},
		{
			desc: "with limit that truncates the response",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      5,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent[:5],
				},
			},
		},
		{
			desc: "with submodule",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("submodule"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_COMMIT,
					Oid:  submoduleCommit.String(),
					Mode: 0o160000,
				},
			},
		},
		{
			desc: "subdirectory without trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  subtreeID.String(),
					Size: subtreeSize,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "subdirectory with trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree/"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  subtreeID.String(),
					Size: subtreeSize,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "blob in subdirectory",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree/subfile"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  subfileID.String(),
					Size: int64(len(subfileContent)),
					Mode: 0o100644,
					Data: subfileContent,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.TreeEntry(ctx, tc.request)
			require.NoError(t, err)

			responses, err := testhelper.Receive(stream.Recv)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponses, responses)
		})
	}
}
