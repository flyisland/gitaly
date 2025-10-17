package gitattributes

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestCheckAttr(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, finish, err := CheckAttr(ctx, nil, "HEAD", nil)
	require.Equal(t, structerr.NewInvalidArgument("empty list of attribute names"), err)
	require.Nil(t, finish)
}

func TestCheckAttrCmd_Check(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc         string
		treeOID      git.ObjectID
		attrContent  string
		path         string
		expectedAttr Attributes
	}{
		{
			desc:         "no attributes",
			path:         "README.md",
			treeOID:      gittest.DefaultObjectHash.EmptyTreeOID,
			expectedAttr: Attributes{},
		},
		{
			desc:         "empty attributes",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "attribute on other file",
			attrContent:  "*.go foo",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "comments are ignored",
			attrContent:  "# *.md -foo",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "single attribute",
			attrContent:  "*.md foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "double star pattern",
			attrContent:  "doc/** foo",
			path:         "doc/development/readme.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "comment and single attribute",
			attrContent:  "# these are the gitattributes\n*.md foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "quoted pattern",
			attrContent:  `"*.\155\144" foo`,
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:        "multiple attributes",
			attrContent: "*.md foo bar",
			path:        "README.md",
			expectedAttr: Attributes{
				Attribute{Name: "foo", State: "set"},
				Attribute{Name: "bar", State: "set"},
			},
		},
		{
			desc:        "multiple attributes on multiple lines",
			attrContent: "*.md foo\n*.md bar",
			path:        "README.md",
			expectedAttr: Attributes{
				Attribute{Name: "foo", State: "set"},
				Attribute{Name: "bar", State: "set"},
			},
		},
		{
			desc:         "attributes on multiple files",
			attrContent:  "*.md foo\n*.go bar",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "attributes with state",
			attrContent:  "*.md foo=bar",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "bar"}},
		},
		{
			desc:         "attributes with unset",
			attrContent:  "*.md -foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "unset"}},
		},
		{
			desc:         "attributes with unset overriding",
			attrContent:  "*.md foo\ndoc/*.md -foo",
			path:         "doc/README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "unset"}},
		},
		{
			desc:         "attributes with state overridden",
			attrContent:  "*.md foo=bar\n*.md foo=buzz",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "buzz"}},
		},
		{
			desc:         "attributes overridden for subdir",
			attrContent:  "*.md foo=bar\ndoc/*.md foo=buzz",
			path:         "doc/README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "buzz"}},
		},
		{
			desc:         "attributes overridden for other subdir",
			attrContent:  "*.md foo=bar\ndoc/*.md foo=buzz",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "bar"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			treeOID := tc.treeOID
			if treeOID == "" {
				treeOID = gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Path: ".gitattributes", Mode: "100644", Content: tc.attrContent},
				})
			} else {
				require.Empty(t, tc.attrContent, "cannot specify both attrContent & treeOID")
			}

			commitID := gittest.WriteCommit(t, cfg, repoPath,
				gittest.WithMessage(tc.desc),
				gittest.WithTree(treeOID))

			checkCmd, finish, err := CheckAttr(ctx, repo, commitID.Revision(), []string{"foo", "bar"})
			require.NoError(t, err)
			t.Cleanup(finish)

			actual, err := checkCmd.Check(tc.path)
			require.NoError(t, err)
			require.Equal(t, tc.expectedAttr, actual)
		})
	}
}
