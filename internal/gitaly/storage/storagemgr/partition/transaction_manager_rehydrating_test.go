package partition

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/offloading"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gocloud.dev/blob"
)

func generateRehydratingTests(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) []transactionTestCase {
	// sink and unstableSink point to the same local directory.
	// sink is the regular one used for asserting the state,
	// while unstableSink is used to simulate errors during upload.
	localBucketDir := testhelper.TempDir(t)
	sink, sinkURL, bucket := setupEmptyLocalBucket(t, localBucketDir, true)
	unstableSink, unstableSinkURL, defectedBucket := setupUnstableLocalBucketDownload(t, localBucketDir)

	t.Cleanup(func() {
		_ = bucket.Close()
		_ = defectedBucket.Close()
	})

	cacheRoot := filepath.Join(testhelper.TempDir(t), "offloading_cache")

	// Run setupOffloadingRepo once to gather object information (blobs, trees, etc.) needed for test expectations.
	// This information becomes inaccessible after customSetup() is called within transactionTestCase.
	_, blobs, trees, commits, refs, alternatesFileContent := setupOffloadingRepo(t, ctx, testPartitionID, relativePath)

	noneBlobObjects := append(trees, commits...)
	allObjects := append(blobs, noneBlobObjects...)

	customSetup := func(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) testTransactionSetup {
		// Reuse the existing repo setup instead of creating a new one with setupOffloadingRepo().
		// Creating a new setup would generate different commits due to different timestamps,
		// making it difficult to predict and verify the expected repository state.
		setup, _, _, _, _, _ := setupOffloadingRepo(t, ctx, testPartitionID, relativePath)
		setup.Config.Offloading = config.Offloading{
			Enabled:    true,
			CacheRoot:  cacheRoot,
			GoCloudURL: sinkURL,
		}
		setup.OffloadSink = sink
		return setup
	}

	absCachePath := filepath.Join(cacheRoot, relativePath, objectsDir)
	err := os.MkdirAll(absCachePath, mode.Directory)
	require.NoError(t, err)

	// pathPrefixUUID is a unique path prefix for when uploading
	pathPrefixUUID := uuid.New().String()

	return []transactionTestCase{
		{
			desc:        "rehydrate an offloaded repository",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
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
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{relativePath},
					ExpectedSnapshotLSN: 1,
				},
				RunRehydrating{
					TransactionID: 2,
					Prefix:        filepath.Join(relativePath, pathPrefixUUID),
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     alternatesFileContent,
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
			desc:        "rehydrating when downloading has an error",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
					OverridingSink: unstableSink,
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
						Prefix:      filepath.Join(relativePath, pathPrefixUUID),
					},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{relativePath},
					ExpectedSnapshotLSN: 1,
				},
				RunRehydrating{
					TransactionID: 2,
					Prefix:        filepath.Join(relativePath, pathPrefixUUID),
				},
				Commit{
					TransactionID: 2,
					ExpectedError: errOffloadingObjectDownload,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     alternatesFileContent,
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
			desc:        "conflict with an already committed rehydration",
			customSetup: customSetup,
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)

						// Do a git gc to clean loose objects. git repack with filter may be
						// ineffective when there is loose objects.
						gittest.Exec(tb, cfg, "-C", repoPath, "gc")
					},
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
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{relativePath},
					ExpectedSnapshotLSN: 1,
				},
				RunRehydrating{
					TransactionID: 2,
					Prefix:        filepath.Join(relativePath, pathPrefixUUID),
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{relativePath},
					ExpectedSnapshotLSN: 1,
				},
				RunRehydrating{
					TransactionID: 3,
					Prefix:        filepath.Join(relativePath, pathPrefixUUID),
				},
				Commit{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 3,
					ExpectedError: errHousekeepingConflictConcurrent,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Alternate:     alternatesFileContent,
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

func setupUnstableLocalBucketDownload(t *testing.T, localBucketDir string) (*offloading.Sink, string, offloading.Bucket) {
	ctx := testhelper.Context(t)
	localBucketURL := fmt.Sprintf("file://%s", localBucketDir)
	var bucket offloading.Bucket
	var err error

	bucket, err = newDownloadingUnstableBucket(ctx, localBucketURL)

	require.NoError(t, err)
	sink, err := offloading.NewSink(bucket)
	require.NoError(t, err)
	return sink, localBucketURL, bucket
}

type unstableDownloadingBucket struct {
	downloadActionCount int
	*blob.Bucket
}

func newDownloadingUnstableBucket(ctx context.Context, localBucketDir string) (*unstableDownloadingBucket, error) {
	localBucketURI := fmt.Sprintf("file://%s", localBucketDir)
	bucket, err := blob.OpenBucket(ctx, localBucketURI)
	if err != nil {
		return nil, err
	}
	return &unstableDownloadingBucket{downloadActionCount: 0, Bucket: bucket}, nil
}

func (b *unstableDownloadingBucket) Download(ctx context.Context, key string, w io.Writer, opts *blob.ReaderOptions) error {
	b.downloadActionCount++
	if b.downloadActionCount > 2 {
		// all attempts after from the 3rd one on will fail
		return fmt.Errorf("unstable bucket download error")
	}
	return b.Bucket.Download(ctx, key, w, opts)
}
