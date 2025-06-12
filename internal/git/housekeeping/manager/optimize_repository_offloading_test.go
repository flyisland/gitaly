package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/offloading"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gocloud.dev/blob"
)

func TestOffloadRepository_HappyPath(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	localBucketDir := testhelper.TempDir(t)
	cacheRoot := testhelper.TempDir(t)
	sinkURL := fmt.Sprintf("file://%s", localBucketDir)
	sink, bucket := getOffloadingStorageSink(t, ctx, sinkURL)
	t.Cleanup(func() { _ = bucket.Close() })

	cfg.Offloading = gitalycfg.Offloading{
		Enabled:    true,
		CacheRoot:  cacheRoot,
		GoCloudURL: sinkURL,
	}

	// Setup repo, node and housekeeping manager.
	repoSetup := setupRepoForOffloading(t, ctx, cfg)
	repo := repoSetup.repo
	blobs := repoSetup.blobs
	commits := repoSetup.commits
	trees := repoSetup.trees
	repoPath := repoSetup.repoPath
	node := setupNodeForTransaction(t, ctx, cfg, sink)
	defer node.Close()
	housekeepingManager := New(cfg.Prometheus, testhelper.SharedLogger(t), nil, node)

	// Execute offloading in repository.
	offloadingCfg := housekeepingcfg.OffloadingConfig{
		CacheRoot:   cacheRoot,
		SinkBaseURL: cfg.Offloading.GoCloudURL,
	}

	require.NoError(t, housekeepingManager.OffloadRepository(ctx, repo, offloadingCfg))
	applyWAL(t, ctx, repo, node)

	// First, verify blobs are properly offloaded and no longer present in the repository
	assertOffloadedRepoObjects(t, false, "", blobs, trees, commits, repoPath, cfg)

	// Then, verify that downloading offloaded files to the cache resolves missing objects, which confirms:
	// 1. Offloaded files contain the expected blob content
	// 2. After injecting the cache path into GIT_ALTERNATE_OBJECT_DIRECTORIES, the downloaded files are
	//    recognized as part of the repository
	cachePathObjectsDir := filepath.Join(cfg.Offloading.CacheRoot, repo.GetRelativePath(), "objects")
	cachePathPackDir := filepath.Join(cachePathObjectsDir, "pack")
	downloadFilesToCache(t, ctx, repoPath, cachePathPackDir, sink, repo, cfg)
	assertOffloadedRepoObjects(t, true, cachePathObjectsDir, blobs, trees, commits, repoPath, cfg)
}

type offloadingRepoSetup struct {
	repoPath              string
	repo                  *localrepo.Repo
	blobs, trees, commits []git.ObjectID
	refs                  map[git.ReferenceName]git.ObjectID
}

func setupRepoForOffloading(t *testing.T, ctx context.Context, cfg gitalycfg.Cfg) offloadingRepoSetup {
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           gittest.NewRepositoryName(t),
	})

	blobs := make([]git.ObjectID, 4)
	for i := range len(blobs) {
		blobs[i] = gittest.WriteBlob(t, cfg, repoPath, []byte(strconv.Itoa(i)))
	}

	subsubTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subsubfile", Mode: "100644", OID: blobs[0]},
	})
	firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(subsubTree), gittest.WithParents())
	subTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subfile", Mode: "100644", OID: blobs[1]},
		{Path: "subsubdir", Mode: "040000", OID: subsubTree},
	})
	secondCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(subTree), gittest.WithParents(firstCommit))
	commitTree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "LICENSE", Mode: "100644", OID: blobs[2]},
		{Path: "README.md", Mode: "100644", OID: blobs[3]},
		{Path: "subdir", Mode: "040000", OID: subTree},
	})
	thirdCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(commitTree), gittest.WithParents(secondCommit))

	trees := []git.ObjectID{subsubTree, subTree, commitTree}
	commits := []git.ObjectID{firstCommit, secondCommit, thirdCommit}

	refs := make(map[git.ReferenceName]git.ObjectID)
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/first", firstCommit)
	refs["refs/heads/first"] = firstCommit
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/second", secondCommit)
	refs["refs/heads/second"] = secondCommit
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/main", thirdCommit)
	refs["refs/heads/main"] = thirdCommit

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	repo := localrepo.New(testhelper.NewLogger(t), gitalycfg.NewLocator(cfg), cmdFactory, nil, repoProto)
	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	return offloadingRepoSetup{
		repoPath: repoPath,
		repo:     repo,
		blobs:    blobs,
		trees:    trees,
		commits:  commits,
		refs:     refs,
	}
}

func getOffloadingStorageSink(t *testing.T, ctx context.Context, localBucketURL string) (*offloading.Sink, offloading.Bucket) {
	bucket, err := blob.OpenBucket(ctx, localBucketURL)
	require.NoError(t, err)
	sink, err := offloading.NewSink(bucket)
	require.NoError(t, err)
	return sink, bucket
}

func setupNodeForTransaction(t *testing.T, ctx context.Context, cfg gitalycfg.Cfg, sink *offloading.Sink) *nodeimpl.Manager {
	logger := testhelper.SharedLogger(t)
	cmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, gitalycfg.NewLocator(cfg), cmdFactory, catfileCache)

	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		cfg.Storages,
		keyvalue.NewBadgerStore,
		helper.NewNullTickerFactory(),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	raftNode, err := raftmgr.NewNode(cfg, logger, dbMgr, nil)
	require.NoError(t, err)

	raftFactory := raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)

	partitionFactoryOptions := []partition.FactoryOption{
		partition.WithCmdFactory(cmdFactory),
		partition.WithRepoFactory(localRepoFactory),
		partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
		partition.WithRaftConfig(cfg.Raft),
		partition.WithRaftFactory(raftFactory),
		partition.WithOffloadingSink(sink),
	}

	node, err := nodeimpl.NewManager(
		cfg.Storages,
		storagemgr.NewFactory(
			logger,
			dbMgr,
			partition.NewFactory(partitionFactoryOptions...),
			gitalycfg.DefaultMaxInactivePartitions,
			storagemgr.NewMetrics(cfg.Prometheus),
		),
	)
	require.NoError(t, err)
	return node
}

func applyWAL(t *testing.T, ctx context.Context, repo *localrepo.Repo, node storage.Node) {
	nodeStorage, err := node.GetStorage(repo.GetStorageName())
	require.NoError(t, err)

	// Start a transaction to ensure the WAL is fully applied. This test is still
	// accessing the repository directly in the storage.
	tx, err := nodeStorage.Begin(ctx, storage.TransactionOptions{
		ReadOnly:     true,
		RelativePath: repo.GetRelativePath(),
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tx.Rollback(ctx))
	}()
}

func downloadFilesToCache(t *testing.T, ctx context.Context, repoPath, cachePath string, sink *offloading.Sink,
	repo *localrepo.Repo, cfg gitalycfg.Cfg,
) {
	offloadRemoteURL := gittest.Exec(t, cfg, "-C", repoPath, "config", "get", "remote.offload.url")
	prefix := filepath.Join(repo.GetRelativePath(), filepath.Base(strings.TrimSpace(string(offloadRemoteURL))))

	filesOnSink, err := sink.List(ctx, prefix)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(cachePath, mode.Directory))
	for _, name := range filesOnSink {
		key := filepath.Join(prefix, name)
		require.NoError(t, sink.Download(ctx, key, filepath.Join(cachePath, name)))
	}
}

func assertOffloadedRepoObjects(t *testing.T, downloadToCache bool, cachePath string, blobs, trees, commits []git.ObjectID, repoPath string, cfg gitalycfg.Cfg) {
	expectedObjects := make([]git.ObjectID, 0, len(trees)+len(commits))
	expectedObjects = append(expectedObjects, commits...)
	expectedObjects = append(expectedObjects, trees...)
	expectedObjectHashes := make([]string, 0, len(blobs)+len(trees)+len(commits))
	for _, obj := range expectedObjects {
		expectedObjectHashes = append(expectedObjectHashes, string(obj))
	}
	for _, b := range blobs {

		hashPrefix := ""
		if !downloadToCache {
			// If we choose not to download offloaded files, it means
			// blobs are offloaded, hence missing from the repo. Therefore, their hash are prefixed with "?".
			hashPrefix = "?"
		}
		expectedObjectHashes = append(expectedObjectHashes, hashPrefix+string(b))
	}

	actualObjectHashes := make([]string, 0, len(trees)+len(commits)+len(blobs))
	execOpt := gittest.ExecConfig{
		Env: []string{fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", cachePath)},
	}
	output := gittest.ExecOpts(t, cfg, execOpt, "-C", repoPath, "rev-list", "--objects", "--all", "--missing=print", "--no-object-names")
	actualObjectHashes = append(actualObjectHashes, strings.Split(text.ChompBytes(output), "\n")...)
	require.ElementsMatch(t, expectedObjectHashes, actualObjectHashes)
}
