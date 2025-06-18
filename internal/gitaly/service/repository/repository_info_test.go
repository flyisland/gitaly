package repository

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRepositoryInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	writeFile := func(t *testing.T, byteCount int, pathComponents ...string) string {
		t.Helper()
		path := filepath.Join(pathComponents...)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), mode.Directory))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0}, byteCount), mode.File))
		return path
	}

	filter := snapshot.NewDefaultFilter(ctx)
	if testhelper.IsWALEnabled() {
		filter = snapshot.NewRegexSnapshotFilter()
	}

	emptyRepoSize := func() uint64 {
		_, repoPath := gittest.CreateRepository(t, ctx, cfg)
		size, err := dirSizeInBytes(repoPath, filter)
		require.NoError(t, err)
		return uint64(size)
	}()

	repoFullRepackTimestamp := func(t *testing.T, repoPath string) *timestamppb.Timestamp {
		timestamp, err := stats.FullRepackTimestamp(repoPath)
		require.NoError(t, err)
		return &timestamppb.Timestamp{Seconds: timestamp.Unix()}
	}

	repoAlternateModifiedTimestamp := func(t *testing.T, repoPath string) *timestamppb.Timestamp {
		alternatesStat, err := os.Stat(filepath.Join(repoPath, "objects", "info", "alternates"))
		require.NoError(t, err)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			require.NoError(t, err)
			return nil
		}

		return &timestamppb.Timestamp{Seconds: alternatesStat.ModTime().Unix()}
	}

	type setupData struct {
		request          *gitalypb.RepositoryInfoRequest
		expectedErr      error
		expectedResponse *gitalypb.RepositoryInfoResponse
	}

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T) setupData
		ignore       bool
		ignoreReason string
	}{
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: nil,
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "invalid repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: &gitalypb.Repository{
							StorageName:  cfg.Storages[0].Name,
							RelativePath: "does/not/exist",
						},
					},
					expectedErr: testhelper.ToInterceptedMetadata(
						structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "does/not/exist")),
					),
				}
			},
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects:        &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with loose reference",
			setup: func(t *testing.T) setupData {
				testhelper.SkipWithReftable(t, "files backend specific test")

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				writeFile(t, 123, repoPath, "refs", "heads", "main")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							LooseCount: 1,
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects:        &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with packed references",
			setup: func(t *testing.T) setupData {
				testhelper.SkipWithReftable(t, "files backend specific test")

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				writeFile(t, 123, repoPath, "packed-refs")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							PackedSize: 123,
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects:        &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with loose blob",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oid := gittest.DefaultObjectHash.ZeroOID.String()
				writeFile(t, 123, repoPath, "objects", oid[0:2], oid[2:])

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:              123,
							RecentSize:        123,
							LooseObjectsCount: 1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with stale loose object",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oid := gittest.DefaultObjectHash.ZeroOID.String()
				objectPath := writeFile(t, 123, repoPath, "objects", oid[0:2], oid[2:])
				stale := time.Now().Add(stats.StaleObjectsGracePeriod)
				require.NoError(t, os.Chtimes(objectPath, stale, stale))

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:                   123,
							StaleSize:              123,
							LooseObjectsCount:      1,
							StaleLooseObjectsCount: 1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with mixed stale and recent loose object",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				stale := time.Now().Add(stats.StaleObjectsGracePeriod)

				recentOID := gittest.DefaultObjectHash.ZeroOID.String()
				writeFile(t, 70, repoPath, "objects", recentOID[0:2], recentOID[2:])

				staleOID := strings.Repeat("1", len(recentOID))
				staleObjectPath := writeFile(t, 700, repoPath, "objects", staleOID[0:2], staleOID[2:])
				require.NoError(t, os.Chtimes(staleObjectPath, stale, stale))

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 70 + 700,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:                   770,
							RecentSize:             70,
							StaleSize:              700,
							LooseObjectsCount:      2,
							StaleLooseObjectsCount: 1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with packfile",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				writeFile(t, 123, repoPath, "objects", "pack", "pack-1234.pack")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:          123,
							RecentSize:    123,
							PackfileCount: 1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with cruft pack",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				writeFile(t, 123, repoPath, "objects", "pack", "pack-1234.pack")
				writeFile(t, 7, repoPath, "objects", "pack", "pack-1234.mtimes")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123 + 7,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:          123,
							StaleSize:     123,
							PackfileCount: 1,
							CruftCount:    1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
			ignore:       testhelper.IsWALEnabled(),
			ignoreReason: "transaction snapshot exclude .mtime file in objects/pack",
		},
		{
			desc: "repository with keep pack",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				writeFile(t, 123, repoPath, "objects", "pack", "pack-1234.pack")
				writeFile(t, 7, repoPath, "objects", "pack", "pack-1234.keep")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 123 + 7,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:          123,
							RecentSize:    123,
							KeepSize:      123,
							PackfileCount: 1,
							KeepCount:     1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
			ignore:       testhelper.IsWALEnabled(),
			ignoreReason: "transaction snapshots exclude .keep file in objects/pack",
		},
		{
			desc: "repository with different pack types",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				writeFile(t, 70, repoPath, "objects", "pack", "pack-1.pack")

				writeFile(t, 700, repoPath, "objects", "pack", "pack-2.pack")
				writeFile(t, 100, repoPath, "objects", "pack", "pack-2.keep")

				writeFile(t, 7000, repoPath, "objects", "pack", "pack-3.pack")
				writeFile(t, 1000, repoPath, "objects", "pack", "pack-3.mtimes")

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: emptyRepoSize + 8000 + 800 + 70,
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:          7770,
							RecentSize:    770,
							KeepSize:      700,
							StaleSize:     7000,
							PackfileCount: 3,
							KeepCount:     1,
							CruftCount:    1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
			ignore:       testhelper.IsWALEnabled(),
			ignoreReason: "transaction snapshot exclude .keep and .mtime file in objects/pack",
		},
		{
			desc: "repository with commit-graph",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c",
					"commitGraph.generationVersion=2",
					"commit-graph",
					"write",
					"--reachable",
					"--changed-paths",
				)

				// This test should only be concerned with the commit-graph info. Other info, especially
				// sizes, are not deterministic.
				repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
				require.NoError(t, err)

				repoSize, err := dirSizeInBytes(repoPath, filter)
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
							LooseCount: uint64(gittest.FilesOrReftables(1, 0)),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							LooseObjectsCount: repoStats.LooseObjects.Count,
							RecentSize:        repoStats.LooseObjects.Size,
							Size:              repoStats.LooseObjects.Size,
						},
						CommitGraph: &gitalypb.RepositoryInfoResponse_CommitGraphInfo{
							HasBloomFilters:   true,
							HasGenerationData: true,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with bitmap info",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Write a commit and create a branch to ensure we have reachable objects
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// Use explicit configuration to ensure bitmap is created with expected properties
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "pack.writeBitmapLookupTable=true",
					"-c", "pack.writeBitmapHashCache=true",
					"repack", "-Adb")

				// Get the actual repository info to use consistent sizes
				repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
				require.NoError(t, err)

				repoSize, err := dirSizeInBytes(repoPath, filter)

				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
							LooseCount: uint64(gittest.FilesOrReftables(1, 0)),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							PackfileCount:     1,
							Size:              repoStats.Packfiles.Size,
							RecentSize:        repoStats.Packfiles.Size,
							ReverseIndexCount: 1,
						},
						Bitmap: &gitalypb.RepositoryInfoResponse_BitmapInfo{
							HasHashCache:   true,
							HasLookupTable: true,
							Version:        1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with multi-pack-index",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create initial commit and packfile
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

				// Add another commit and create a second packfile
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))

				// Create multi-pack-index with explicit configuration
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "pack.writeReverseIndex=true",
					"repack", "-Ad", "--write-midx")

				// Get the actual repository info
				repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
				require.NoError(t, err)

				repoSize, err := dirSizeInBytes(repoPath, filter)

				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
							LooseCount: uint64(gittest.FilesOrReftables(2, 0)),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							PackfileCount:     repoStats.Packfiles.Count,
							Size:              repoStats.Packfiles.Size,
							RecentSize:        repoStats.Packfiles.Size,
							ReverseIndexCount: repoStats.Packfiles.ReverseIndexCount,
						},
						MultiPackIndex: &gitalypb.RepositoryInfoResponse_MultiPackIndexInfo{
							PackfileCount: repoStats.Packfiles.MultiPackIndex.PackfileCount,
							Version:       1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with multi-pack-index and bitmap",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create initial commit and packfile
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("main"))

				// Create multi-pack-index with bitmap using explicit config
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "pack.writeBitmapLookupTable=true",
					"-c", "pack.writeBitmapHashCache=true",
					"-c", "pack.writeReverseIndex=true",
					"repack", "-Adb", "--write-midx")

				// Get actual repository info
				repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
				require.NoError(t, err)

				repoSize, err := dirSizeInBytes(repoPath, filter)

				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
							LooseCount: uint64(gittest.FilesOrReftables(1, 0)),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							PackfileCount:     1,
							Size:              repoStats.Packfiles.Size,
							RecentSize:        repoStats.Packfiles.Size,
							ReverseIndexCount: 1,
						},
						MultiPackIndex: &gitalypb.RepositoryInfoResponse_MultiPackIndexInfo{
							PackfileCount: 1,
							Version:       1,
						},
						MultiPackIndexBitmap: &gitalypb.RepositoryInfoResponse_BitmapInfo{
							HasHashCache:   true,
							HasLookupTable: true,
							Version:        1,
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with garbage",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				writeFile(t, 1000, repoPath, "objects", "17", "garbage")

				// Get actual repository info
				repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
				require.NoError(t, err)

				repoSize, err := dirSizeInBytes(repoPath, filter)

				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
							Size:       repoStats.Packfiles.Size,
							RecentSize: repoStats.Packfiles.Size,

							// When WAL is enabled and snapshot filtering is enabled then the
							// garbage filtered out from snapshot, so it is not counted as
							// a loose object garbage
							LooseObjectsGarbageCount: testhelper.WithOrWithoutWAL(uint64(0), uint64(1)),
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
		{
			desc: "repository with alternates",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				poolRelativePath := gittest.NewObjectPoolName(t)
				gittest.CreateObjectPool(t, ctx, cfg, repo, gittest.CreateObjectPoolConfig{
					RelativePath:               poolRelativePath,
					LinkRepositoryToObjectPool: true,
				})

				var objectsDirectory string
				if testhelper.IsPraefectEnabled() {
					// Praefect has a much more sophiasticated flow for object pool. This RPC should
					// focus on translating repository stats rather than finding the precise
					// alternates path.
					repoStats, err := stats.RepositoryInfoForRepository(ctx, localrepo.NewTestRepo(t, cfg, repo))
					require.NoError(t, err)
					objectsDirectory = repoStats.Alternates.ObjectDirectories[0]
				} else {
					// Without Praefect, it's trivial to imply the alternates path. This assertion
					// uses the precise location.
					var err error
					objectsDirectory, err = filepath.Rel(
						filepath.Join(repoPath, "objects"),
						filepath.Join(cfg.Storages[0].Path, poolRelativePath, "objects"),
					)
					require.NoError(t, err)
				}

				repoSize, err := dirSizeInBytes(repoPath, filter)

				require.NoError(t, err)

				return setupData{
					request: &gitalypb.RepositoryInfoRequest{
						Repository: repo,
					},
					expectedResponse: &gitalypb.RepositoryInfoResponse{
						Size: uint64(repoSize),
						References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
							ReferenceBackend: gittest.FilesOrReftables(
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
								gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
							),
						},
						Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
						Alternates: &gitalypb.RepositoryInfoResponse_AlternatesInfo{
							ObjectDirectories: []string{objectsDirectory},
							LastModified:      repoAlternateModifiedTimestamp(t, repoPath),
						},
						LastFullRepack: repoFullRepackTimestamp(t, repoPath),
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.ignore {
				t.Skip(tc.ignoreReason)
			}

			t.Parallel()

			setup := tc.setup(t)

			response, err := client.RepositoryInfo(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}

func TestConvertRepositoryInfo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		repoSize         uint64
		repoInfo         stats.RepositoryInfo
		expectedResponse *gitalypb.RepositoryInfoResponse
		expectedErr      error
	}{
		{
			desc:        "all-zero",
			expectedErr: fmt.Errorf("invalid reference backend"),
		},
		{
			desc:     "size",
			repoSize: 123,
			repoInfo: stats.RepositoryInfo{
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				Size: 123,
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
			},
		},
		{
			desc: "references",
			repoInfo: stats.RepositoryInfo{
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
					LooseReferencesCount: 123,
					PackedReferencesSize: 456,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					LooseCount: 123,
					PackedSize: 456,
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
			},
		},
		{
			desc: "loose objects",
			repoInfo: stats.RepositoryInfo{
				LooseObjects: stats.LooseObjectsInfo{
					Size:      123,
					StaleSize: 3,
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
					Size:       123,
					RecentSize: 120,
					StaleSize:  3,
				},
			},
		},
		{
			desc: "packfiles",
			repoInfo: stats.RepositoryInfo{
				Packfiles: stats.PackfilesInfo{
					Size:              123,
					CruftSize:         3,
					KeepSize:          7,
					Count:             10,
					ReverseIndexCount: 5,
					CruftCount:        1,
					KeepCount:         3,
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
					Size:              123,
					RecentSize:        120,
					StaleSize:         3,
					KeepSize:          7,
					PackfileCount:     10,
					ReverseIndexCount: 5,
					CruftCount:        1,
					KeepCount:         3,
				},
			},
		},
		{
			desc: "loose objects with counts",
			repoInfo: stats.RepositoryInfo{
				LooseObjects: stats.LooseObjectsInfo{
					Size:       123,
					StaleSize:  3,
					Count:      50,
					StaleCount: 5,
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
					Size:                   123,
					RecentSize:             120,
					StaleSize:              3,
					LooseObjectsCount:      50,
					StaleLooseObjectsCount: 5,
				},
			},
		},
		{
			desc: "loose objects and packfiles",
			repoInfo: stats.RepositoryInfo{
				LooseObjects: stats.LooseObjectsInfo{
					Size:       7,
					StaleSize:  3,
					Count:      10,
					StaleCount: 2,
				},
				Packfiles: stats.PackfilesInfo{
					Size:              700,
					CruftSize:         300,
					KeepSize:          500,
					Count:             15,
					ReverseIndexCount: 10,
					CruftCount:        5,
					KeepCount:         2,
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
					Size:                   707,
					RecentSize:             404,
					StaleSize:              303,
					KeepSize:               500,
					PackfileCount:          15,
					ReverseIndexCount:      10,
					CruftCount:             5,
					KeepCount:              2,
					LooseObjectsCount:      10,
					StaleLooseObjectsCount: 2,
				},
			},
		},
		{
			desc: "commit graph info",
			repoInfo: stats.RepositoryInfo{
				CommitGraph: stats.CommitGraphInfo{
					Exists:                    true,
					CommitGraphChainLength:    3,
					HasBloomFilters:           true,
					HasGenerationData:         true,
					HasGenerationDataOverflow: false,
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				CommitGraph: &gitalypb.RepositoryInfoResponse_CommitGraphInfo{
					CommitGraphChainLength:    3,
					HasBloomFilters:           true,
					HasGenerationData:         true,
					HasGenerationDataOverflow: false,
				},
			},
		},
		{
			desc: "bitmap info",
			repoInfo: stats.RepositoryInfo{
				Packfiles: stats.PackfilesInfo{
					Bitmap: stats.BitmapInfo{
						Exists:         true,
						HasHashCache:   true,
						HasLookupTable: false,
						Version:        2,
					},
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				Bitmap: &gitalypb.RepositoryInfoResponse_BitmapInfo{
					HasHashCache:   true,
					HasLookupTable: false,
					Version:        2,
				},
			},
		},
		{
			desc: "multi-pack-index info",
			repoInfo: stats.RepositoryInfo{
				Packfiles: stats.PackfilesInfo{
					MultiPackIndex: stats.MultiPackIndexInfo{
						Exists:        true,
						PackfileCount: 5,
						Version:       2,
					},
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				MultiPackIndex: &gitalypb.RepositoryInfoResponse_MultiPackIndexInfo{
					PackfileCount: 5,
					Version:       2,
				},
			},
		},
		{
			desc: "multi-pack-index bitmap info",
			repoInfo: stats.RepositoryInfo{
				Packfiles: stats.PackfilesInfo{
					MultiPackIndexBitmap: stats.BitmapInfo{
						Exists:         true,
						HasHashCache:   false,
						HasLookupTable: true,
						Version:        2,
					},
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				MultiPackIndexBitmap: &gitalypb.RepositoryInfoResponse_BitmapInfo{
					HasHashCache:   false,
					HasLookupTable: true,
					Version:        2,
				},
			},
		},
		{
			desc: "alternates info",
			repoInfo: stats.RepositoryInfo{
				Alternates: stats.AlternatesInfo{
					Exists:            true,
					ObjectDirectories: []string{"/path/to/objects", "/another/path"},
					LastModified:      time.Unix(1234567890, 0),
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				Alternates: &gitalypb.RepositoryInfoResponse_AlternatesInfo{
					ObjectDirectories: []string{"/path/to/objects", "/another/path"},
					LastModified:      timestamppb.New(time.Unix(1234567890, 0)),
				},
			},
		},
		{
			desc: "no full repack",
			repoInfo: stats.RepositoryInfo{
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
				// LastFullRepack is not set, so it should default to zero
				Packfiles: stats.PackfilesInfo{
					// Explicitly using a zero time to simulate no repack
					LastFullRepack: time.Time{},
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects:        &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				LastFullRepack: nil, // Should be zero timestamp when no repack has happened
			},
		},
		{
			desc: "is object pool and last full repack",
			repoInfo: stats.RepositoryInfo{
				IsObjectPool: true,
				Packfiles: stats.PackfilesInfo{
					LastFullRepack: time.Unix(1234567890, 0),
				},
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
				},
			},
			expectedResponse: &gitalypb.RepositoryInfoResponse{
				References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
					ReferenceBackend: gittest.FilesOrReftables(
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES,
						gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE,
					),
				},
				Objects:        &gitalypb.RepositoryInfoResponse_ObjectsInfo{},
				IsObjectPool:   true,
				LastFullRepack: timestamppb.New(time.Unix(1234567890, 0)),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := convertRepositoryInfo(tc.repoSize, tc.repoInfo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResponse, response)
		})
	}
}
