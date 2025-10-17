package housekeeping

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/packfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestSetOffloadingGitConfig(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		url            string
		filter         string
		expectedErr    error
		expectedConfig []string
	}{
		{
			desc:        "can set offload remote",
			url:         "fake://offload/promisor/url",
			filter:      "blob:none",
			expectedErr: nil,
			expectedConfig: []string{
				"remote.offload.fetch +refs/heads/*:refs/remotes/offload/*",
				"remote.offload.partialclonefilter blob:none",
				"remote.offload.url fake://offload/promisor/url",
				"remote.offload.promisor true",
			},
		},
		{
			desc:           "fail when remote url is empty",
			url:            "",
			filter:         "blob:none",
			expectedErr:    fmt.Errorf("set offloading config: promisor remote url missing"),
			expectedConfig: []string{},
		},
		{
			desc:           "fail when filter is empty",
			url:            "fake://offload/promisor/url",
			filter:         "",
			expectedErr:    fmt.Errorf("set offloading config: filter missing"),
			expectedConfig: []string{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			require.Equal(t, tc.expectedErr,
				SetOffloadingGitConfig(ctx, repo, tc.url, tc.filter,
					&transaction.MockManager{}))

			if tc.expectedErr == nil {
				regex := fmt.Sprintf("remote.%s", OffloadingPromisorRemote)
				output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--get-regexp", regex)
				require.ElementsMatch(t,
					tc.expectedConfig,
					strings.Split(text.ChompBytes(output), "\n"),
				)
			}
		})
	}
}

func TestResetOffloadingGitConfig(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("only remove offloading promisor Git configurations", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
			gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Get a snapshot of repository configuration before applying configuration changes.
		output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		beforeConfigs := strings.Split(text.ChompBytes(output), "\n")

		// Apply an offload configuration.
		url := "fake://offload/promisor/url"
		filter := "blob:none"
		require.NoError(t, SetOffloadingGitConfig(ctx, repo, url, filter, &transaction.MockManager{}))
		require.NoError(t, SetOffloadingGitConfig(ctx, repo, url, filter, &transaction.MockManager{}))

		// Ensure that the configuration changes have been successfully applied.
		output = gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		modifiedConfigs := strings.Split(text.ChompBytes(output), "\n")
		require.ElementsMatch(t, append([]string{
			fmt.Sprintf("remote.%s.fetch=+refs/heads/*:refs/remotes/%s/*", OffloadingPromisorRemote, OffloadingPromisorRemote),
			fmt.Sprintf("remote.%s.partialclonefilter=%s", OffloadingPromisorRemote, filter),
			fmt.Sprintf("remote.%s.url=%s", OffloadingPromisorRemote, url),
			fmt.Sprintf("remote.%s.promisor=true", OffloadingPromisorRemote),
		}, beforeConfigs...), modifiedConfigs)

		// Execute the reset operation to roll back the configuration.
		require.NoError(t, ResetOffloadingGitConfig(ctx, repo, &transaction.MockManager{}))

		// Other unrelated configurations remain unaffected.
		// The modified configurations are restored to their original state.
		output = gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
		afterConfigs := strings.Split(text.ChompBytes(output), "\n")
		require.ElementsMatch(t, beforeConfigs, afterConfigs)
	})
}

func TestPerformRepackingForOffloading(t *testing.T) {
	t.Parallel()

	t.Run("error when loose objects", func(t *testing.T) {
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		repo, _, _, _ := setUpRepoForOffloading(t, ctx, cfg)

		filter := "blob:none"
		filterTo := testhelper.TempDir(t)
		err := PerformRepackingForOffloading(ctx, repo, filter, filterTo)
		require.Equal(t, err, fmt.Errorf("loose objects when performing repack for offloading"))
	})

	t.Run("repack without loose objects", func(t *testing.T) {
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		repo, commits, trees, blobs := setUpRepoForOffloading(t, ctx, cfg)
		repoPath, _ := repo.Path(ctx)
		// Remove loose objects to ensure they do not interfere with the repack process.
		gittest.Exec(t, cfg, "-C", repoPath, "gc")

		filter := "blob:none"
		filterTo := testhelper.TempDir(t)
		err := PerformRepackingForOffloading(ctx, repo, filter, filterTo)
		require.NoError(t, err)
		repoPackDir := filepath.Join(repoPath, "objects", "pack")

		// Assert one and only one promisor file is created
		entries, err := os.ReadDir(repoPackDir)
		require.NoError(t, err)
		var promisorFileCount int
		var midxFileCount int
		for _, entry := range entries {
			entryName := entry.Name()
			if strings.HasPrefix(entryName, "pack-") && strings.HasSuffix(entryName, ".promisor") {
				promisorFileCount++
			}
			if strings.EqualFold(entryName, "multi-pack-index") {
				midxFileCount++
			}
		}
		require.Equal(t, promisorFileCount, 1)
		require.Equal(t, midxFileCount, 1)

		// Assert actual objects left-over packfile
		var expectedLeftOverObjects []git.ObjectID
		expectedLeftOverObjects = append(expectedLeftOverObjects, commits...)
		expectedLeftOverObjects = append(expectedLeftOverObjects, trees...)
		actualLeftOverObjects := readObjectsFromPackFile(t, ctx, cfg, repo, repoPackDir)
		require.ElementsMatch(t, expectedLeftOverObjects, actualLeftOverObjects)

		// Assert actual objects in new packfile
		expectedMovedAwayObjects := blobs
		actualMovedAwayObjects := readObjectsFromPackFile(t, ctx, cfg, repo, filterTo)
		require.ElementsMatch(t, expectedMovedAwayObjects, actualMovedAwayObjects)

		// Assert logical view point of Git. After repacking with the filter blob:none, all blob objects are
		// missing because they are moved to the "filterTo" path. As a result, their object hashes start with a
		// "?" while the commit and tree objects remain in the repository.
		output := gittest.Exec(t, cfg, "-C", repoPath, "rev-list", "--objects", "--all", "--missing=print", "--no-object-names")
		var expectObjectHashList []string
		for _, blob := range actualLeftOverObjects {
			expectObjectHashList = append(expectObjectHashList, string(blob))
		}
		for _, blob := range expectedMovedAwayObjects {
			// Blobs are filtered, hence missing. Their hash are prefixed with "?".
			expectObjectHashList = append(expectObjectHashList, "?"+string(blob))
		}

		var actualObjectHashList []string
		actualObjectHashList = append(actualObjectHashList, strings.Split(text.ChompBytes(output), "\n")...)
		require.ElementsMatch(t, expectObjectHashList, actualObjectHashList)
	})
}

func TestAddCacheAlternateEntry(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cacheRoot := testhelper.TempDir(t)
	cacheDir := filepath.Join(cacheRoot, "offloading_cache", "my_cache_dir")

	for _, tc := range []struct {
		desc             string
		alternateEntries []string
		expectedErr      error
	}{
		{
			desc:             "alternate file not exist",
			alternateEntries: []string{},
			expectedErr:      nil,
		},
		{
			desc:             "alternate file exists without offloading_cache",
			alternateEntries: []string{filepath.Join(cacheRoot, "old", "alt")},
			expectedErr:      nil,
		},
		{
			desc: "alternate file exists with offloading_cache",
			alternateEntries: []string{
				filepath.Join(cacheRoot, "old", "alt"),
				filepath.Join(cacheRoot, offloadingCacheDir, "existing_cache"),
			},
			expectedErr: fmt.Errorf("offloading cache entry is already added"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, os.MkdirAll(cacheDir, mode.Directory))
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			require.DirExists(t, repoPath)

			if len(tc.alternateEntries) > 0 {
				addAlternateToRepo(t, ctx, tc.alternateEntries, repo)
			}

			cacheEntry, err := filepath.Rel(filepath.Join(repoPath, "objects"), cacheDir)
			require.NoError(t, err)
			alternateFile, err := repo.InfoAlternatesPath(ctx)
			require.NoError(t, err)

			err = AddCacheAlternateEntry(alternateFile, cacheEntry)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				// Use `git count-objects -v` to verify alternate object databases.
				// This command outputs absolute paths of alternates, allowing us to confirm
				// if the repository correctly recognizes them.
				// Ref: https://git-scm.com/docs/git-count-objects
				output := gittest.Exec(t, cfg, "-C", repoPath, "count-objects", "-v")
				for _, entry := range tc.alternateEntries {
					require.Contains(t, text.ChompBytes(output), entry)
				}
				require.Contains(t, text.ChompBytes(output), cacheDir)
			} else {
				require.Equal(t, err, tc.expectedErr)
			}
		})
	}
}

func TestRemoveCacheAlternateEntry(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cacheRoot := testhelper.TempDir(t)
	cacheDir := filepath.Join(cacheRoot, "offloading_cache", "my_repo")

	for _, tc := range []struct {
		desc             string
		alternateEntries []string
	}{
		{
			desc:             "alternate file does not exist",
			alternateEntries: []string{},
		},
		{
			desc: "alternate file exists with cache entry",
			alternateEntries: []string{
				cacheDir, // always put cache dir first, since we have logic to compare other entries.
				filepath.Join(cacheRoot, "old", "alt1"),
				filepath.Join(cacheRoot, "old", "alt2"),
			},
		},
		{
			desc: "alternate file exists without cache entry",
			alternateEntries: []string{
				filepath.Join(cacheRoot, "old", "alt1"),
				filepath.Join(cacheRoot, "old", "alt2"),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			require.DirExists(t, repoPath)

			alternateFile, err := repo.InfoAlternatesPath(ctx)
			require.NoError(t, err)

			if len(tc.alternateEntries) == 0 {
				err = RemoveCacheAlternateEntry(alternateFile)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {

				addAlternateToRepo(t, ctx, tc.alternateEntries, repo)
				err = RemoveCacheAlternateEntry(alternateFile)
				require.NoError(t, err)

				output := gittest.Exec(t, cfg, "-C", repoPath, "count-objects", "-v")

				// Use `git count-objects -v` to verify alternate object databases.
				// This command outputs absolute paths of alternates, allowing us to confirm
				// if the repository correctly recognizes them.
				// Ref: https://git-scm.com/docs/git-count-objects
				for _, entry := range tc.alternateEntries[1:] {
					require.Contains(t, text.ChompBytes(output), entry)
				}

				// Ensure no offloading_cache entry remains after RemoveCacheAlternateEntry execution,
				// regardless of the initial state.
				require.NotContains(t, text.ChompBytes(output), "offloading_cache")

			}
		})
	}
}

func setUpRepoForOffloading(t *testing.T, ctx context.Context, cfg config.Cfg) (
	repo *localrepo.Repo,
	commits []git.ObjectID,
	trees []git.ObjectID,
	blobs []git.ObjectID,
) {
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
		gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
	repo = localrepo.NewTestRepo(t, cfg, repoProto)
	require.DirExists(t, repoPath)
	// We set up the repository with the following object structure:
	// - Four blobs
	// - Three trees
	// - One commit
	blobs = gittest.WriteBlobs(t, cfg, repoPath, 4)
	subsubTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subsubfile", Mode: "100644", OID: blobs[0]},
	})
	trees = append(trees, subsubTree)
	subTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subfile", Mode: "100644", OID: blobs[1]},
		{Path: "subsubdir", Mode: "040000", OID: subsubTree},
	})
	trees = append(trees, subTree)
	commitTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "LICENSE", Mode: "100644", OID: blobs[2]},
		{Path: "README.md", Mode: "100644", OID: blobs[3]},
		{Path: "subdir", Mode: "040000", OID: subTree},
	})
	trees = append(trees, commitTree)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTree(commitTree))
	commits = append(commits, commitID)

	return repo, commits, trees, blobs
}

func readObjectsFromPackFile(t *testing.T, ctx context.Context, cfg config.Cfg, repo *localrepo.Repo, packfileDir string) []git.ObjectID {
	entries, err := os.ReadDir(packfileDir)
	require.NoError(t, err)

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
	packfileIndex := filepath.Join(packfileDir, idxFile)
	repoPath, _ := repo.Path(ctx)
	gittest.Exec(t, cfg, "-C", repoPath, "verify-pack", packfileIndex)

	// Read all objects from the packfile
	cmdFactory := gittest.NewCommandFactory(t, cfg)
	index, err := packfile.ReadIndexWithGitCmdFactory(cmdFactory, repo, testhelper.SharedLogger(t), packfileIndex)
	require.NoError(t, err)

	var objectOIDs []git.ObjectID
	for _, obj := range index.Objects {
		objectOIDs = append(objectOIDs, git.ObjectID(obj.OID))
	}
	return objectOIDs
}

func addAlternateToRepo(t *testing.T, ctx context.Context, alternatePaths []string, repo *localrepo.Repo) {
	repoPath, _ := repo.Path(ctx)
	alternateFile, err := repo.InfoAlternatesPath(ctx)
	require.NoError(t, err)
	altFile, err := os.Create(alternateFile)
	require.NoError(t, err)
	defer func() { require.NoError(t, altFile.Close()) }()
	for _, path := range alternatePaths {
		require.NoError(t, os.MkdirAll(path, mode.Directory))
		alternateEntry, err := filepath.Rel(filepath.Join(repoPath, "objects"), path)
		require.NoError(t, err)
		_, err = altFile.WriteString(alternateEntry + "\n")
		require.NoError(t, err)
	}
}
