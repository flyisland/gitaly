package partition

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict/fshistory"
	"gitlab.com/gitlab-org/gitaly/v16/internal/offloading"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gocloud.dev/blob"
)

func generateOffloadingTests(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) []transactionTestCase {
	sink, sinkURL, bucket := setupEmptyLocalBucket(t, testhelper.TempDir(t), true)
	unstableSink, unstableSinkURL, defectedBucket := setupEmptyLocalBucket(t, testhelper.TempDir(t), false)
	t.Cleanup(func() {
		_ = bucket.Close()
		_ = defectedBucket.Close()
	})

	cacheRoot := filepath.Join(testhelper.TempDir(t), "offloading_cache")

	// Run setupOffloadingRepo once to gather object information (blobs, trees, etc.) needed for test expectations.
	// This information becomes inaccessible after customSetup() is called within transactionTestCase.
	preRunSetup, blobs, trees, commits, refs, originalAlternatesFileContent := setupOffloadingRepo(t, ctx, testPartitionID, relativePath)

	noneBlobObjects := append(trees, commits...)
	allObjects := append(blobs, noneBlobObjects...)

	customSetup := func(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) testTransactionSetup {
		// Reuse the existing repo setup instead of creating a new one with setupOffloadingRepo().
		// Creating a new setup would generate different commits due to different timestamps,
		// making it difficult to predict and verify the expected repository state.
		setup, _, _, _, _, _ := setupOffloadingRepo(t, ctx, testPartitionID, relativePath)
		return setup
	}

	absCachePath := filepath.Join(cacheRoot, relativePath, objectsDir)
	err := os.MkdirAll(absCachePath, mode.Directory)
	require.NoError(t, err)

	// The alternate file contains a relative path from the repository's objects directory to the cache directory
	relCachePath, err := filepath.Rel(filepath.Join(preRunSetup.RepositoryPath, objectsDir), absCachePath)
	require.NoError(t, err)
	afterOffloadingAlternatesFileContent := fmt.Sprintf("%s\n%s\n", originalAlternatesFileContent, relCachePath)

	// pathPrefixUuid is a unique path prefix for when uploading
	pathPrefixUUID := uuid.New().String()
	anotherTxnPathPrefixUUID := uuid.New().String()

	return []transactionTestCase{
		{
			desc:        "offload a repository",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: sink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 1,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
						Prefix:      filepath.Join(relativePath, pathPrefixUUID),
					},
				},
				Commit{
					TransactionID: 1,
					ExpectedError: nil,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     afterOffloadingAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: noneBlobObjects,
					},
				},
				OffloadingStorage: OffloadingStorageStates{
					filepath.Join(relativePath, pathPrefixUUID): OffloadingStorageState{
						Sink:      sink,
						Kind:      packFile,
						FileTotal: 3,
						Objects:   blobs,
					},
				},
			},
		},
		{
			desc:        "cannot offload a repository with loose objects",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					Sink: sink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 1,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
					},
				},
				Commit{
					TransactionID: 1,
					ExpectedError: errOffloadingOnRepacking,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     originalAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: nil,
								LooseReferences:  refs,
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 3,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
									{
										MinIndex: 4,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: allObjects,
					},
				},
			},
		},
		{
			desc:        "when upload having an error",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: unstableSink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 1,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: unstableSinkURL,
					},
				},
				Commit{
					TransactionID: 1,
					ExpectedError: errOffloadingObjectUpload,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     originalAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: allObjects,
					},
				},
				OffloadingStorage: OffloadingStorageStates{
					filepath.Join(relativePath, pathPrefixUUID): OffloadingStorageState{
						Sink:      unstableSink,
						Kind:      packFile,
						FileTotal: 0,
					},
				},
			},
		},
		{
			desc:        "conflict when there is a committed offloading already",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: sink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 1,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
						Prefix:      filepath.Join(relativePath, pathPrefixUUID),
					},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 2,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
						Prefix:      filepath.Join(relativePath, anotherTxnPathPrefixUUID),
					},
				},
				Commit{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
					ExpectedError: errHousekeepingConflictConcurrent,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     afterOffloadingAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: noneBlobObjects,
					},
				},
				OffloadingStorage: OffloadingStorageStates{
					filepath.Join(relativePath, pathPrefixUUID): OffloadingStorageState{
						Sink:      sink,
						Kind:      packFile,
						FileTotal: 3,
						Objects:   blobs,
					},
				},
			},
		},
		{
			desc:        "conflict when there is a committed housekeeping job already",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: sink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				RunRepack{
					TransactionID: 1,
					Config: housekeepingcfg.RepackObjectsConfig{
						Strategy:            housekeepingcfg.RepackObjectsStrategyFullWithUnreachable,
						WriteBitmap:         true,
						WriteMultiPackIndex: true,
					},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 2,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
					},
				},
				Commit{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
					ExpectedError: errHousekeepingConflictConcurrent,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     originalAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: allObjects,
					},
				},
			},
		},
		{
			desc:        "conflict when there is committed repository deletion already",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: sink,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 2,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
						Prefix:      filepath.Join(relativePath, pathPrefixUUID),
					},
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{relativePath},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 3,
				},
				Commit{
					TransactionID: 3,
				},
				Commit{
					TransactionID: 2,
					ExpectedError: errConflictRepositoryDeletion,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                               storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey(relativePath)): "",
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								LooseReferences: map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 1,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
										},
									},
								},
							},
						}),
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc:        "conflict when there is a committed change on the alternate file",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")

						// Removes the alternates file, later in the test file
						// we will recreate one
						require.NoError(t, os.Remove(filepath.Join(stats.AlternatesFilePath(repoPath))))

						gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
							SkipCreationViaService: true,
							RelativePath:           "fake_pool",
						})
					},
					Sink: sink,
				},

				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath, "fake_pool"},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 2,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
					},
				},

				Commit{
					TransactionID:   1,
					UpdateAlternate: &alternateUpdate{RelativePath: "fake_pool"},
				},
				Commit{
					TransactionID: 2,
					ExpectedError: fshistory.NewReadWriteConflictError(
						filepath.Join(stats.AlternatesFilePath(relativePath)),
						0, 1),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					"fake_pool": {
						Objects: []git.ObjectID{},
					},
					relativePath: {
						Alternate:     "../../../../../fake_pool/objects",
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: allObjects,
					},
				},
			},
		},
		{
			desc:        "conflict when committed changes on the gitconfig file",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					Sink: sink,
				},

				Begin{
					TransactionID: 1,
					RelativePaths: []string{relativePath},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{relativePath},
				},
				RunOffloading{
					TransactionID: 2,
					Config: housekeepingcfg.OffloadingConfig{
						CacheRoot:   cacheRoot,
						SinkBaseURL: sinkURL,
					},
				},
				Commit{
					TransactionID: 1,
					UpdateGitConfig: map[string]string{
						"user.name": "John Doe the Offloading tester",
					},
				},
				Commit{
					TransactionID: 2,
					ExpectedError: fshistory.NewReadWriteConflictError(
						filepath.Join(filepath.Join(relativePath, "config")),
						0, 1),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     originalAlternatesFileContent,
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(&ReferencesState{
							FilesBackend: &FilesBackendState{
								PackedReferences: refs,
								LooseReferences:  map[git.ReferenceName]git.ObjectID{},
							},
						}, &ReferencesState{
							ReftableBackend: &ReftableBackendState{
								Tables: []ReftableTable{
									{
										MinIndex: 1,
										MaxIndex: 4,
										References: []git.Reference{
											{
												Name:       "HEAD",
												Target:     "refs/heads/main",
												IsSymbolic: true,
											},
											{
												Name:       "refs/heads/first",
												Target:     refs["refs/heads/first"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/main",
												Target:     refs["refs/heads/main"].String(),
												IsSymbolic: false,
											},
											{
												Name:       "refs/heads/second",
												Target:     refs["refs/heads/second"].String(),
												IsSymbolic: false,
											},
										},
									},
								},
							},
						}),
						Objects: allObjects,
					},
				},
			},
		},
	}
}

// setupEmptyLocalBucket initializes an empty Bucket backed by the local file system.
func setupEmptyLocalBucket(t *testing.T, localBucketDir string, stable bool) (*offloading.Sink, string, offloading.Bucket) {
	ctx := testhelper.Context(t)
	localBucketURL := fmt.Sprintf("file://%s", localBucketDir)
	var bucket offloading.Bucket
	var err error
	if stable {
		bucket, err = blob.OpenBucket(ctx, localBucketURL)
	} else {
		bucket, err = newUnstableBucket(ctx, localBucketURL)
	}
	require.NoError(t, err)
	sink, err := offloading.NewSink(bucket)
	require.NoError(t, err)
	return sink, localBucketURL, bucket
}

func setupOffloadingRepo(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) (
	setup testTransactionSetup, blobs, trees, commits []git.ObjectID, refs map[git.ReferenceName]git.ObjectID,
	alternatesFileContent string,
) {
	t.Helper()

	cfg := testcfg.Build(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           relativePath,
	})

	blobs = make([]git.ObjectID, 4)
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

	trees = []git.ObjectID{subsubTree, subTree, commitTree}
	commits = []git.ObjectID{firstCommit, secondCommit, thirdCommit}

	refs = make(map[git.ReferenceName]git.ObjectID)
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/first", firstCommit)
	refs["refs/heads/first"] = firstCommit
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/second", secondCommit)
	refs["refs/heads/second"] = secondCommit
	gittest.WriteRef(t, cfg, repoPath, "refs/heads/main", thirdCommit)
	refs["refs/heads/main"] = thirdCommit

	fakeAlternateRepoDir := filepath.Join(repoPath, "i_am_fake_alternate")
	alternatesFileEntry, err := filepath.Rel(filepath.Join(repoPath, objectsDir), filepath.Join(fakeAlternateRepoDir, objectsDir))
	require.NoError(t, err)
	alternatesFileContent = fmt.Sprintf("%s\n", alternatesFileEntry)
	err = os.WriteFile(stats.AlternatesFilePath(repoPath), []byte(alternatesFileContent), mode.File)
	require.NoError(t, err)

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	logger := testhelper.NewLogger(t)
	locator := config.NewLocator(cfg)
	localRepo := localrepo.New(
		logger,
		locator,
		cmdFactory,
		catfileCache,
		repo,
	)

	objectHash, err := localRepo.ObjectHash(ctx)
	require.NoError(t, err)

	hasher := objectHash.Hash()
	_, err = hasher.Write([]byte("content does not matter"))
	require.NoError(t, err)
	nonExistentOID, err := objectHash.FromHex(hex.EncodeToString(hasher.Sum(nil)))
	require.NoError(t, err)

	return testTransactionSetup{
		PartitionID:       testPartitionID,
		RelativePath:      relativePath,
		RepositoryPath:    repoPath,
		Repo:              localRepo,
		Config:            cfg,
		ObjectHash:        objectHash,
		CommandFactory:    cmdFactory,
		RepositoryFactory: localrepo.NewFactory(logger, locator, cmdFactory, catfileCache),
		NonExistentOID:    nonExistentOID,
		Commits: testTransactionCommits{
			First: testTransactionCommit{
				OID: firstCommit,
			},
			Second: testTransactionCommit{
				OID: secondCommit,
			},
			Third: testTransactionCommit{
				OID: thirdCommit,
			},
		},
	}, blobs, trees, commits, refs, alternatesFileContent
}

type OffloadingObjectStorageFormat int

const (
	packFile OffloadingObjectStorageFormat = iota
	looseObject
)

// unstableBucket embeds a gocloud.dev/blob.Bucket, and provides unstable behaviours for testing.
type unstableBucket struct {
	uploadActionCount int
	*blob.Bucket
}

func newUnstableBucket(ctx context.Context, localBucketDir string) (*unstableBucket, error) {
	localBucketURI := fmt.Sprintf("file://%s", localBucketDir)
	bucket, err := blob.OpenBucket(ctx, localBucketURI)
	if err != nil {
		return nil, err
	}
	return &unstableBucket{uploadActionCount: 0, Bucket: bucket}, nil
}

func (b *unstableBucket) Upload(ctx context.Context, key string, r io.Reader, opts *blob.WriterOptions) error {
	b.uploadActionCount++
	if b.uploadActionCount > 2 {
		// all attempts after from the 3rd one on will fail
		return fmt.Errorf("unstable bucket uploade error")
	}
	return b.Bucket.Upload(ctx, key, r, opts)
}
