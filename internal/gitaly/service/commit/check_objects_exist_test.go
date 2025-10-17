package commit

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckObjectsExist(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitID1 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("master"), gittest.WithMessage("commit-1"),
	)
	commitID2 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("feature"), gittest.WithMessage("commit-2"), gittest.WithParents(commitID1),
	)
	commitID3 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("commit-3"), gittest.WithParents(commitID1),
	)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("foo"), gittest.WithTreeEntries(
		gittest.TreeEntry{OID: blobID, Path: "bar", Mode: "100644"},
		gittest.TreeEntry{OID: blobID, Path: "~bar", Mode: "100644"},
		gittest.TreeEntry{OID: blobID, Path: "@bar", Mode: "100644"},
		gittest.TreeEntry{OID: blobID, Path: "@", Mode: "100644"},
		gittest.TreeEntry{OID: blobID, Path: "bar:none", Mode: "100644"},
	))

	for _, tc := range []struct {
		desc            string
		requests        []*gitalypb.CheckObjectsExistRequest
		expectedResults map[string]bool
		expectedErr     error
	}{
		{
			desc:        "no repository provided",
			requests:    []*gitalypb.CheckObjectsExistRequest{{Repository: nil}},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:     "no requests",
			requests: []*gitalypb.CheckObjectsExistRequest{},
			// Ideally, we'd return an invalid-argument error in case there aren't any
			// requests. We can't do this though as this would diverge from Praefect's
			// behaviour, which always returns `io.EOF`.
			expectedErr: status.Error(codes.Internal, io.EOF.Error()),
		},
		{
			desc: "missing repository",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Revisions: [][]byte{},
				},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "request without revisions",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
				},
			},
		},
		{
			desc: "commit ids and refs that exist",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
						[]byte("master"),
						[]byte(commitID2),
						[]byte(commitID3),
						[]byte("feature"),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String(): true,
				"master":           true,
				commitID2.String(): true,
				commitID3.String(): true,
				"feature":          true,
			},
		},
		{
			desc: "ref and objects missing",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
						[]byte("master"),
						[]byte(commitID2),
						[]byte(commitID3),
						[]byte("feature"),
						[]byte("refs/does/not/exist"),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String():    true,
				"master":              true,
				commitID2.String():    true,
				commitID3.String():    true,
				"feature":             true,
				"refs/does/not/exist": false,
			},
		},
		{
			desc: "chunked input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
					},
				},
				{
					Revisions: [][]byte{
						[]byte(commitID2),
					},
				},
				{
					Revisions: [][]byte{
						[]byte("refs/does/not/exist"),
					},
				},
				{
					Revisions: [][]byte{
						[]byte(commitID3),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String():    true,
				commitID2.String():    true,
				commitID3.String():    true,
				"refs/does/not/exist": false,
			},
		},
		{
			desc: "invalid input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte("-not-a-rev"),
					},
				},
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "-not-a-rev"),
		},
		{
			desc: "input with whitespace",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(fmt.Sprintf("%s\n%s", commitID1, commitID2)),
					},
				},
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't contain whitespace"),
				"revision", fmt.Sprintf("%s\n%s", commitID1, commitID2)),
		},
		{
			desc: "chunked invalid input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
					},
				},
				{
					Revisions: [][]byte{
						[]byte("-not-a-rev"),
					},
				},
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
				"revision", "-not-a-rev"),
		},
		{
			desc: "path scoped revisions",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte("foo:bar"),
						[]byte("bar:foo"),
					},
				},
			},
			expectedResults: map[string]bool{
				"foo:bar": true,
				"bar:foo": false,
			},
		},
		{
			desc: "path scoped revisions",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte("foo:bar"),
						[]byte("bar:foo"),
						[]byte("bar:foo\nfoo"),
						[]byte("foo:~bar"),
						[]byte("foo:@bar"),
						[]byte("foo:@"),
						[]byte("foo:bar:none"),
					},
				},
			},
			expectedResults: map[string]bool{
				"foo:bar":      true,
				"bar:foo":      false,
				"bar:foo\nfoo": false,
				"foo:~bar":     true,
				"foo:@bar":     true,
				"foo:@":        true,
				"foo:bar:none": true,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			client, err := client.CheckObjectsExist(ctx)
			require.NoError(t, err)

			for _, request := range tc.requests {
				require.NoError(t, client.Send(request))
			}
			require.NoError(t, client.CloseSend())

			var results map[string]bool
			for {
				var response *gitalypb.CheckObjectsExistResponse
				response, err = client.Recv()
				if err != nil {
					break
				}

				for _, revision := range response.GetRevisions() {
					if results == nil {
						results = map[string]bool{}
					}
					results[string(revision.GetName())] = revision.GetExists()
				}
			}

			if tc.expectedErr == nil {
				tc.expectedErr = io.EOF
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResults, results)
		})
	}
}
