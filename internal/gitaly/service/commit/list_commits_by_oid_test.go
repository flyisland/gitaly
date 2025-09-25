package commit

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListCommitsByOid(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	var commitIDs []string
	var treeIDs []string
	for i := 0; i < 20; i++ {
		treeID := gittest.WriteTree(t, cfg, repoPath,
			[]gittest.TreeEntry{{Mode: "100644", Path: "foo", Content: fmt.Sprintf("foo %d", i)}})
		commitIDs = append(commitIDs, gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithTree(treeID), gittest.WithMessage(fmt.Sprintf("Commit: %d", i))).String())
		treeIDs = append(treeIDs, treeID.String())
	}

	expectedCommits := func(from, to int) []*gitalypb.GitCommit {
		commits := []*gitalypb.GitCommit{}

		for i := from; i < to; i++ {
			commits = append(commits, &gitalypb.GitCommit{
				Id:        commitIDs[i],
				Body:      []byte(fmt.Sprintf("Commit: %d", i)),
				BodySize:  int64(len(fmt.Sprintf("Commit: %d", i))),
				Subject:   []byte(fmt.Sprintf("Commit: %d", i)),
				Author:    gittest.DefaultCommitAuthor,
				Committer: gittest.DefaultCommitAuthor,
				TreeId:    treeIDs[i],
			})
		}

		return commits
	}

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.ListCommitsByOidRequest
		expectedErr     error
		expectedCommits []*gitalypb.GitCommit
	}{
		{
			desc:            "single commit",
			request:         &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: []string{commitIDs[0]}},
			expectedCommits: expectedCommits(0, 1),
		},
		{
			desc:            "multiple commits",
			request:         &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: []string{commitIDs[0], commitIDs[1]}},
			expectedCommits: expectedCommits(0, 2),
		},
		{
			desc:            "partial oids",
			request:         &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: []string{commitIDs[0][:10], commitIDs[1][:8]}},
			expectedCommits: expectedCommits(0, 2),
		},
		{
			desc:            "large request",
			request:         &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: commitIDs},
			expectedCommits: expectedCommits(0, 20),
		},
		{
			desc:    "no query",
			request: &gitalypb.ListCommitsByOidRequest{Repository: repo},
		},
		{
			desc:    "empty query",
			request: &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: []string{""}},
		},
		{
			desc:    "unknown oids",
			request: &gitalypb.ListCommitsByOidRequest{Repository: repo, Oid: []string{"foobar", gittest.DefaultObjectHash.EmptyTreeOID.String()}},
		},
		{
			desc:        "repository not provided",
			request:     &gitalypb.ListCommitsByOidRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			c, err := client.ListCommitsByOid(ctx, tc.request)
			require.NoError(t, err)

			receivedCommits := getAllCommits(t, func() (gitCommitsGetter, error) {
				getter, err := c.Recv()

				if tc.expectedErr != nil || err != nil && !errors.Is(err, io.EOF) {
					testhelper.RequireGrpcError(t, tc.expectedErr, err)
					return getter, io.EOF
				}
				return getter, err
			})
			testhelper.ProtoEqual(t, tc.expectedCommits, receivedCommits)
		})
	}
}
