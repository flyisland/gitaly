package ref

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_ListRefs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	oldCommitID := gittest.WriteCommit(t, cfg, repoPath)
	newCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(oldCommitID),
		gittest.WithAuthorDate(time.Date(2011, 2, 16, 14, 1, 0, 0, time.FixedZone("UTC+1", +1*60*60))),
	)

	for _, cmd := range [][]string{
		{"update-ref", "refs/heads/main", newCommitID.String()},
		{"tag", "lightweight-tag", newCommitID.String()},
		{"tag", "old-commit-tag", oldCommitID.String()},
		{"tag", "-m", "tag message", "annotated-tag", "refs/heads/main"},
		{"symbolic-ref", "refs/heads/symbolic", "refs/heads/main"},
		{"update-ref", "refs/remote/remote-name/remote-branch", newCommitID.String()},
		{"symbolic-ref", "HEAD", "refs/heads/main"},
		{"update-ref", "refs/heads/old", oldCommitID.String()},
	} {
		gittest.Exec(t, cfg, append([]string{"-C", repoPath}, cmd...)...)
	}

	annotatedTagOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "annotated-tag"))

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.ListRefsRequest
		expectedGrpcError codes.Code
		expectedError     string
		expected          []*gitalypb.ListRefsResponse_Reference
	}{
		{
			desc: "no repo",
			request: &gitalypb.ListRefsRequest{
				Patterns: [][]byte{[]byte("refs/")},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "", // Ideally we would test the message but it changes when running through praefect
		},
		{
			desc: "no patterns",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = patterns must have at least one entry",
		},
		{
			desc: "bad sorting key",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Key: gitalypb.ListRefsRequest_SortBy_Key(100),
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     `rpc error: code = InvalidArgument desc = sorting key "100" is not supported`,
		},
		{
			desc: "bad sorting direction",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Direction: gitalypb.SortDirection(100),
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = sorting direction is not supported",
		},
		{
			desc: "not found",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("this-pattern-does-not-match-anything")},
			},
		},
		{
			desc: "not found and main",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns: [][]byte{
					[]byte("this-pattern-does-not-match-anything"),
					[]byte("refs/heads/main"),
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: newCommitID.String()},
			},
		},
		{
			desc: "all",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: newCommitID.String()},
				{Name: []byte("refs/heads/old"), Target: oldCommitID.String()},
				{Name: []byte("refs/heads/symbolic"), Target: newCommitID.String()},
				{Name: []byte("refs/remote/remote-name/remote-branch"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "sort by authordate desc",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/heads")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Direction: gitalypb.SortDirection_DESCENDING,
					Key:       gitalypb.ListRefsRequest_SortBy_AUTHORDATE,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/old"), Target: oldCommitID.String()},
				{Name: []byte("refs/heads/main"), Target: newCommitID.String()},
				{Name: []byte("refs/heads/symbolic"), Target: newCommitID.String()},
			},
		},
		{
			desc: "branches and tags only",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/heads/*"), []byte("refs/tags/*")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: newCommitID.String()},
				{Name: []byte("refs/heads/old"), Target: oldCommitID.String()},
				{Name: []byte("refs/heads/symbolic"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "head and branches and tags only",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Head:       true,
				Patterns:   [][]byte{[]byte("refs/heads/*"), []byte("refs/tags/*")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("HEAD"), Target: newCommitID.String()},
				{Name: []byte("refs/heads/main"), Target: newCommitID.String()},
				{Name: []byte("refs/heads/old"), Target: oldCommitID.String()},
				{Name: []byte("refs/heads/symbolic"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "tags filtered by one OID",
			request: &gitalypb.ListRefsRequest{
				Repository:     repo,
				Head:           false,
				Patterns:       [][]byte{[]byte("refs/tags/*")},
				PointingAtOids: [][]byte{[]byte(oldCommitID.String())},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "tags filtered by multiple OIDs",
			request: &gitalypb.ListRefsRequest{
				Repository:     repo,
				Head:           false,
				Patterns:       [][]byte{[]byte("refs/tags/*")},
				PointingAtOids: [][]byte{[]byte(oldCommitID.String()), []byte(newCommitID.String())},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "with PeelTags option",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Head:       false,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PeelTags:   true,
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID, PeeledTarget: newCommitID.String()},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "pagination with limit exceeding refs count",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PaginationParams: &gitalypb.PaginationParameter{
					Limit: 100,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "pagination with limit smaller than refs count",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PaginationParams: &gitalypb.PaginationParameter{
					Limit: 2,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
			},
		},
		{
			desc: "pagination with page token and no limit",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: "refs/tags/annotated-tag",
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = invalid page token: sending lines: could not find page token",
		},
		{
			desc: "pagination with page token and limit",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: "refs/tags/annotated-tag",
					Limit:     2,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/old-commit-tag"), Target: oldCommitID.String()},
			},
		},
		{
			desc: "pagination with page token and reversed sorting",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/tags/*")},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: "refs/tags/old-commit-tag",
					Limit:     2,
				},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Direction: gitalypb.SortDirection_DESCENDING,
					Key:       gitalypb.ListRefsRequest_SortBy_AUTHORDATE,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/tags/lightweight-tag"), Target: newCommitID.String()},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
			},
		},
		{
			desc: "pagination with invalid page token",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: "refs/tags/missing_tag",
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = invalid page token: sending lines: could not find page token",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			c, err := client.ListRefs(ctx, tc.request)
			require.NoError(t, err)

			var refs []*gitalypb.ListRefsResponse_Reference
			for {
				r, err := c.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if tc.expectedError == "" && tc.expectedGrpcError == 0 {
					require.NoError(t, err)
				} else {
					if tc.expectedError != "" {
						require.EqualError(t, err, tc.expectedError)
					}

					if tc.expectedGrpcError != 0 {
						testhelper.RequireGrpcCode(t, err, tc.expectedGrpcError)
					}

					return
				}

				refs = append(refs, r.GetReferences()...)
			}

			testhelper.ProtoEqual(t, tc.expected, refs)
		})
	}
}

func TestListRefs_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefService(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.ListRefsRequest
		expectedErr error
	}{
		{
			desc:        "repository not provided",
			req:         &gitalypb.ListRefsRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:        "no Patterns",
			req:         &gitalypb.ListRefsRequest{Repository: repo, Patterns: nil},
			expectedErr: status.Error(codes.InvalidArgument, "patterns must have at least one entry"),
		},
		{
			desc: "bad sort key",
			req: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{{}},
				SortBy:     &gitalypb.ListRefsRequest_SortBy{Key: gitalypb.ListRefsRequest_SortBy_Key(-1)},
			},
			expectedErr: status.Error(codes.InvalidArgument, `sorting key "-1" is not supported`),
		},
		{
			desc: "bad sort direction",
			req: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{{}},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Key:       gitalypb.ListRefsRequest_SortBy_REFNAME,
					Direction: gitalypb.SortDirection(-2),
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "sorting direction is not supported"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListRefs(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
