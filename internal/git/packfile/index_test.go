package packfile_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/packfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestReadIndexWithGitCmdFactory(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.SharedLogger(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	repo, expectedOIDs := setUpRepo(t, ctx, cfg)

	repoPath, err := repo.Path(ctx)
	require.NoError(t, err)
	repoPackDir := filepath.Join(repoPath, "objects", "pack")

	entries, err := os.ReadDir(repoPackDir)
	require.NoError(t, err)
	require.Equal(t, 4, len(entries))

	// Find the idx file
	var idxFile string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".idx" {
			idxFile = entry.Name()
			break
		}
	}
	require.NotEmpty(t, idxFile)
	// Verify if the index file and associated packfile is valid.
	packfileIndex := filepath.Join(repoPackDir, idxFile)

	for _, tc := range []struct {
		desc          string
		gitCmdFactory gitcmd.CommandFactory
		repo          *localrepo.Repo
		expectedOIDs  []git.ObjectID
		expectedError error
	}{
		{
			desc:          "Read objects from index file",
			gitCmdFactory: gitCmdFactory,
			repo:          repo,
			expectedOIDs:  expectedOIDs,
			expectedError: nil,
		},
		{
			desc:          "gitCmdFactory with nil repo ",
			gitCmdFactory: gitCmdFactory,
			repo:          nil,
			expectedOIDs:  nil,
			expectedError: fmt.Errorf("must use Git command factory without repo"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			index, err := packfile.ReadIndexWithGitCmdFactory(tc.gitCmdFactory, tc.repo, logger, packfileIndex)
			require.Equal(t, tc.expectedError, err)

			if tc.expectedError == nil {
				var actualOIDs []git.ObjectID
				for _, obj := range index.Objects {
					actualOIDs = append(actualOIDs, git.ObjectID(obj.OID))
				}
				require.ElementsMatch(t, expectedOIDs, actualOIDs)
			}
		})
	}
}

func setUpRepo(t *testing.T, ctx context.Context, cfg config.Cfg) (
	repo *localrepo.Repo,
	objects []git.ObjectID,
) {
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
		gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
	repo = localrepo.NewTestRepo(t, cfg, repoProto)
	require.DirExists(t, repoPath)

	// We set up the repository with the following object structure:
	// - Two blobs
	// - Two trees
	// - One commit
	blobs := gittest.WriteBlobs(t, cfg, repoPath, 2)
	objects = append(objects, blobs...)
	subTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subfile", Mode: "100644", OID: blobs[0]},
	})
	objects = append(objects, subTree)
	commitTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "mockfile", Mode: "100644", OID: blobs[1]},
		{Path: "subdir", Mode: "040000", OID: subTree},
	})
	objects = append(objects, commitTree)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTree(commitTree))
	objects = append(objects, commitID)

	// Pack the objects in the repository.
	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	return repo, objects
}
