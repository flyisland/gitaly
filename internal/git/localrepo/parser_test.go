package localrepo

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestParser(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitignoreBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("gitignore"))
	gitmodulesBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("gitmodules"))
	submoduleCommitID := gittest.WriteCommit(t, cfg, repoPath)

	regularEntriesTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: ".gitignore", Mode: "100644", OID: gitignoreBlobID},
		{Path: ".gitmodules", Mode: "100644", OID: gitmodulesBlobID},
		{Path: "entry with space", Mode: "040000", OID: gittest.DefaultObjectHash.EmptyTreeOID},
		{Path: "gitlab-shell", Mode: "160000", OID: submoduleCommitID},
	})

	for _, tc := range []struct {
		desc            string
		treeID          git.ObjectID
		expectedEntries Entries
	}{
		{
			desc:   "regular entries",
			treeID: regularEntriesTreeID,
			expectedEntries: Entries{
				{
					Mode: "100644",
					Type: Blob,
					OID:  gitignoreBlobID,
					Path: ".gitignore",
				},
				{
					Mode: "100644",
					Type: Blob,
					OID:  gitmodulesBlobID,
					Path: ".gitmodules",
				},
				{
					Mode: "040000",
					Type: Tree,
					OID:  gittest.DefaultObjectHash.EmptyTreeOID,
					Path: "entry with space",
				},
				{
					Mode: "160000",
					Type: Submodule,
					OID:  submoduleCommitID,
					Path: "gitlab-shell",
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			treeData := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-z", tc.treeID.String())

			parser := NewParser(bytes.NewReader(treeData), gittest.DefaultObjectHash)
			parsedEntries := Entries{}
			for {
				entry, err := parser.NextEntry()
				if errors.Is(err, io.EOF) {
					break
				}

				require.NoError(t, err)
				parsedEntries = append(parsedEntries, *entry)
			}

			require.Equal(t, tc.expectedEntries, parsedEntries)
		})
	}
}

func TestParserReadEntryPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	regularEntriesTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: ".gitignore", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("gitignore"))},
		{Path: ".gitmodules", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("gitmodules"))},
		{Path: "entry with space", Mode: "040000", OID: gittest.DefaultObjectHash.EmptyTreeOID},
		{Path: "gitlab-shell", Mode: "160000", OID: gittest.WriteCommit(t, cfg, repoPath)},
		{Path: "\"file with quote.txt", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("file with quotes"))},
		{Path: "cuộc đời là những chuyến đi.md", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("file with non-ascii file name"))},
		{Path: "编码 'foo'.md", Mode: "100644", OID: gittest.WriteBlob(t, cfg, repoPath, []byte("file with non-ascii file name"))},
	})
	for _, tc := range []struct {
		desc          string
		treeID        git.ObjectID
		expectedPaths [][]byte
	}{
		{
			desc:   "regular entries",
			treeID: regularEntriesTreeID,
			expectedPaths: [][]byte{
				[]byte("\"file with quote.txt"),
				[]byte(".gitignore"),
				[]byte(".gitmodules"),
				[]byte("cuộc đời là những chuyến đi.md"),
				[]byte("entry with space"),
				[]byte("gitlab-shell"),
				[]byte("编码 'foo'.md"),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			treeData := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "--name-only", "-z", tc.treeID.String())

			parser := NewParser(bytes.NewReader(treeData), gittest.DefaultObjectHash)
			parsedPaths := [][]byte{}
			for {
				path, err := parser.NextEntryPath()
				if errors.Is(err, io.EOF) {
					break
				}

				require.NoError(t, err)
				parsedPaths = append(parsedPaths, path)
			}

			require.Equal(t, tc.expectedPaths, parsedPaths)
		})
	}
}
