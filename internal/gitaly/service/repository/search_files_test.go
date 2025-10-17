package repository

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestSearchFilesByContent(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", Content: "a-1\na-2\na-3\na-4\na-5\na-6\n"},
		{Path: "b", Mode: "100644", Content: "b-1\nb-2\nb-3\nb-4\nb-5\nb-6\n"},
		{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "c", Mode: "100644", Content: "c-1\nc-2\nc-3\nc-4\nc-5\nc-6\n"},
		})},
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTree(treeID))

	largeFilesCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "huge-file", Mode: "100644", Content: strings.Repeat("abcdefghi\n", 210000)},
		gittest.TreeEntry{Path: "huge-utf8", Mode: "100644", Content: strings.Repeat("你今天吃了什么东西?\n", 70000)},
	))

	largeLineCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "huge-line-file", Mode: "100644", Content: strings.Repeat("abcdefghi", 1024*64) + "\n"},
	))

	dashedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "-dashed", Mode: "100644", Content: "-dashed\n"},
	))

	generateMatch := func(ref, file string, from, to int, generateLine func(line int) string) string {
		var builder strings.Builder
		for i := from; i <= to; i++ {
			_, err := builder.WriteString(fmt.Sprintf("%s:%s\x00%d\x00%s\n", ref, file, i, generateLine(i)))
			require.NoError(t, err)
		}
		return builder.String()
	}
	prefixedLine := func(prefix string) func(int) string {
		return func(line int) string { return fmt.Sprintf("%s-%d", prefix, line) }
	}
	staticLine := func(content string) func(int) string {
		return func(int) string { return content }
	}

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.SearchFilesByContentRequest
		expectedErr     error
		expectedMatches []string
	}{
		{
			desc: "no repo",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty request",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("no query given"),
		},
		{
			desc: "only query given",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "foo",
			},
			expectedErr: structerr.NewInvalidArgument("no ref given"),
		},
		{
			desc: "invalid ref argument",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      ".",
				Ref:        []byte("--no-index"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid ref argument"),
		},
		{
			desc: "single matching file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "a-6",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "multiple matching files",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "6",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "multiple matching files with multiple matching lines",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "[ac]-[16]",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
				generateMatch("branch", "dir/c", 1, 6, prefixedLine("c")),
			},
		},
		{
			desc: "no results",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "这个应该没有结果",
				Ref:        []byte("branch"),
			},
			expectedMatches: nil,
		},
		{
			desc: "with regexp limiter only recognized by pcre",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "(*LIMIT_MATCH=1)a",
				Ref:        []byte("branch"),
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
			},
		},
		{
			desc: "large file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "abcdefg",
				Ref:        []byte(largeFilesCommit),
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-file", 1, 210000, staticLine("abcdefghi")),
			},
		},
		{
			desc: "large file with unicode",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "什么东西",
				Ref:        []byte(largeFilesCommit),
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-utf8", 1, 70000, staticLine("你今天吃了什么东西?")),
			},
		},
		{
			desc: "file with large line",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "abcdefg",
				Ref:        []byte(largeLineCommit),
			},
			expectedMatches: []string{
				generateMatch(largeLineCommit.String(), "huge-line-file", 1, 1, staticLine(strings.Repeat("abcdefghi", 1024*64))),
			},
		},
		{
			desc: "query with leading dash",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "-dashed",
				Ref:        []byte(dashedCommit),
			},
			expectedMatches: []string{
				generateMatch(dashedCommit.String(), "-dashed", 1, 1, staticLine("-dashed")),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SearchFilesByContent(ctx, tc.request)
			require.NoError(t, err)

			matches, err := consumeFilenameByContentChunked(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedMatches, matches)
		})
	}
}

func TestSearchFilesByName(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", Content: "a"},
		{Path: "b", Mode: "100644", Content: "b"},
		{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "c.svg", Mode: "100644", Content: "c"},
			{Path: "d.txt", Mode: "100644", Content: "d"},
		})},
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTree(treeID))

	unusualNamesCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "\"file with quote.txt", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: ".vimrc", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: "cuộc đời là những chuyến đi.md", Mode: "100644", Content: "something"},
		gittest.TreeEntry{Path: "编码 'foo'.md", Mode: "100644", Content: "something"},
	))

	paginationCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file1.md", Mode: "100644", Content: "file1"},
		gittest.TreeEntry{Path: "file2.md", Mode: "100644", Content: "file2"},
		gittest.TreeEntry{Path: "file3.md", Mode: "100644", Content: "file3"},
		gittest.TreeEntry{Path: "file4.md", Mode: "100644", Content: "file4"},
		gittest.TreeEntry{Path: "file5.md", Mode: "100644", Content: "file5"},
		gittest.TreeEntry{Path: "new_file1.md", Mode: "100644", Content: "new_file1"},
		gittest.TreeEntry{Path: "new_file2.md", Mode: "100644", Content: "new_file2"},
		gittest.TreeEntry{Path: "new_file3.md", Mode: "100644", Content: "new_file3"},
	))

	dashedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "-dashed", Mode: "100644", Content: "-dashed\n"},
	))

	for _, tc := range []struct {
		desc          string
		request       *gitalypb.SearchFilesByNameRequest
		expectedErr   error
		expectedFiles []string
	}{
		{
			desc: "repository not initialized",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: &gitalypb.Repository{},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty request",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("no query given"),
		},
		{
			desc: "only query given",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
			},
			expectedErr: structerr.NewInvalidArgument("no ref given"),
		},
		{
			desc: "no repo",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: nil,
				Query:      "foo",
				Ref:        []byte("branch"),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
				Ref:        []byte("branch"),
				Filter:     "+*.",
			},
			expectedErr: structerr.NewInvalidArgument("filter did not compile: error parsing regexp: missing argument to repetition operator: `+`"),
		},
		{
			desc: "filter longer than max",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      "foo",
				Ref:        []byte("branch"),
				Filter:     strings.Repeat(".", searchFilesFilterMaxLength+1),
			},
			expectedErr: structerr.NewInvalidArgument("filter exceeds maximum length"),
		},
		{
			desc: "one file",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir/c.svg",
			},
			expectedFiles: []string{
				"dir/c.svg",
			},
		},
		{
			desc: "many files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir",
			},
			expectedFiles: []string{
				"dir/c.svg",
				"dir/d.txt",
			},
		},
		{
			desc: "filtered",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte("branch"),
				Query:      "dir",
				Filter:     `\.svg$`,
			},
			expectedFiles: []string{
				"dir/c.svg",
			},
		},
		{
			desc: "non-existent ref",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Query:      ".",
				Ref:        []byte("non_existent_ref"),
			},
		},
		{
			desc: "file with quote",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "\"file with quote.txt",
			},
			expectedFiles: []string{"\"file with quote.txt"},
		},
		{
			desc: "dotfiles",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".vimrc",
			},
			expectedFiles: []string{".vimrc"},
		},
		{
			desc: "latin-base language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "cuộc đời là những chuyến đi.md",
			},
			expectedFiles: []string{"cuộc đời là những chuyến đi.md"},
		},
		{
			desc: "non-latin language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      "编码 'foo'.md",
			},
			expectedFiles: []string{"编码 'foo'.md"},
		},
		{
			desc: "filter file with quote",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "^\"file.*",
			},
			expectedFiles: []string{"\"file with quote.txt"},
		},
		{
			desc: "filter dotfiles",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "^\\..*",
			},
			expectedFiles: []string{".vimrc"},
		},
		{
			desc: "filter latin-base language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "cuộc đời .*\\.md",
			},
			expectedFiles: []string{"cuộc đời là những chuyến đi.md"},
		},
		{
			desc: "filter non-latin language",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     "编码 'foo'\\.(md|txt|rdoc)",
			},
			expectedFiles: []string{"编码 'foo'.md"},
		},
		{
			desc: "wildcard filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(unusualNamesCommit),
				Query:      ".",
				Filter:     ".*",
			},
			expectedFiles: []string{
				"\"file with quote.txt",
				".vimrc",
				"cuộc đời là những chuyến đi.md",
				"编码 'foo'.md",
			},
		},
		{
			desc: "only limit is set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Limit:      3,
			},
			expectedFiles: []string{"file1.md", "file2.md", "file3.md"},
		},
		{
			desc: "only offset is set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     2,
			},
			expectedFiles: []string{"file3.md", "file4.md", "file5.md", "new_file1.md", "new_file2.md", "new_file3.md"},
		},
		{
			desc: "both limit and offset are set",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     2,
				Limit:      2,
			},
			expectedFiles: []string{"file3.md", "file4.md"},
		},
		{
			desc: "offset exceeds the total files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     8,
			},
			expectedFiles: nil,
		},
		{
			desc: "offset + limit exceeds the total files",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Offset:     6,
				Limit:      5,
			},
			expectedFiles: []string{"new_file2.md", "new_file3.md"},
		},
		{
			desc: "limit and offset combine with filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Filter:     "new.*",
				Offset:     1,
				Limit:      2,
			},
			expectedFiles: []string{"new_file2.md", "new_file3.md"},
		},
		{
			desc: "limit and offset combine with unmatched filter",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      ".",
				Filter:     "not_matched.*",
				Offset:     1,
				Limit:      2,
			},
			expectedFiles: nil,
		},
		{
			desc: "limit and offset combine with matched query",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(paginationCommit),
				Query:      "new_file2.md",
				Offset:     0,
				Limit:      2,
			},
			expectedFiles: []string{"new_file2.md"},
		},
		{
			desc: "query with leading dash",
			request: &gitalypb.SearchFilesByNameRequest{
				Repository: repoProto,
				Ref:        []byte(dashedCommit),
				Query:      "-dashed",
			},
			expectedFiles: []string{"-dashed"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SearchFilesByName(ctx, tc.request)
			require.NoError(t, err)

			files, err := consumeFilenameByName(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedFiles, files)
		})
	}
}

func consumeFilenameByContentChunked(stream gitalypb.RepositoryService_SearchFilesByContentClient) ([]string, error) {
	var matches []string
	var match []byte

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		match = append(match, resp.GetMatchData()...)
		if resp.GetEndOfMatch() {
			matches = append(matches, string(match))
			match = nil
		}
	}

	return matches, nil
}

func consumeFilenameByName(stream gitalypb.RepositoryService_SearchFilesByNameClient) ([]string, error) {
	var filenames []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, file := range resp.GetFiles() {
			filenames = append(filenames, string(file))
		}
	}

	return filenames, nil
}
