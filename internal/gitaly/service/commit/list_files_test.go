package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListFiles(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", Content: "content"},
		gittest.TreeEntry{Path: "binary", Mode: "100644", Content: "\000something"},
		gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "subfile", Mode: "100644", Content: "subcontent"},
		})},
	))
	defaultPaths := [][]byte{
		[]byte("file"),
		[]byte("binary"),
		[]byte("subdir/subfile"),
	}

	differentCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "different", Mode: "100644", Content: "different"},
	))

	emptyRepoProto, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc          string
		request       *gitalypb.ListFilesRequest
		expectedPaths [][]byte
		expectedErr   error
	}{
		{
			desc: "nil repo",
			request: &gitalypb.ListFilesRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty repo object",
			request: &gitalypb.ListFilesRequest{
				Repository: &gitalypb.Repository{},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "non-existing repo",
			request: &gitalypb.ListFilesRequest{
				Repository: &gitalypb.Repository{StorageName: "foo", RelativePath: "bar"},
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("foo"),
			)),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("--output=/meow"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "valid object ID",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(commitID),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "HEAD",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("HEAD"),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "valid branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(git.DefaultBranch),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "valid fully qualified branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(git.DefaultRef),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "non-default branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(differentCommitID),
			},
			expectedPaths: [][]byte{[]byte("different")},
		},
		{
			desc: "missing object ID uses default branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte{},
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "invalid object ID",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("1234123412341234"),
			},
			expectedPaths: [][]byte{},
		},
		{
			desc: "invalid branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("non/existing"),
			},
			expectedPaths: [][]byte{},
		},
		{
			desc: "nonexisting fully qualified branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("refs/heads/foobar"),
			},
			expectedPaths: [][]byte{},
		},
		{
			desc: "empty repository with unborn branch",
			request: &gitalypb.ListFilesRequest{
				Repository: emptyRepoProto,
				Revision:   []byte(git.DefaultRef),
			},
		},
		{
			desc: "empty repository with missing revision",
			request: &gitalypb.ListFilesRequest{
				Repository: emptyRepoProto,
				Revision:   []byte{},
			},
			expectedErr: structerr.NewFailedPrecondition("repository does not have a default branch"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListFiles(ctx, tc.request)
			require.NoError(t, err)

			paths, err := testhelper.ReceiveAndFold(stream.Recv, func(result [][]byte, response *gitalypb.ListFilesResponse) [][]byte {
				return append(result, response.GetPaths()...)
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.ElementsMatch(t, paths, tc.expectedPaths)
		})
	}
}
