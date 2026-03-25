package repository

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) RepositoryInfo(
	ctx context.Context,
	request *gitalypb.RepositoryInfoRequest,
) (*gitalypb.RepositoryInfoResponse, error) {
	if err := s.locator.ValidateRepository(ctx, request.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(request.GetRepository())

	repoPath, err := repo.Path(ctx)
	if err != nil {
		return nil, err
	}

	filter := snapshot.NewDefaultFilter(ctx)
	repoSize, err := dirSizeInBytes(repoPath, filter)
	if err != nil {
		return nil, fmt.Errorf("calculating repository size: %w", err)
	}

	repoInfo, err := stats.RepositoryInfoForRepository(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}

	// If the repository is linked to an object pool, collect pool stats and merge them in so
	// that the response reflects the complete stats.
	if repoInfo.Alternates.Exists && len(repoInfo.Alternates.AbsoluteObjectDirectories()) > 0 {
		poolRepoPath := filepath.Dir(repoInfo.Alternates.AbsoluteObjectDirectories()[0])

		storagePath, err := s.locator.GetStorageByName(ctx, request.GetRepository().GetStorageName())
		if err != nil {
			return nil, fmt.Errorf("getting storage path: %w", err)
		}

		poolRelativePath, err := filepath.Rel(storagePath, poolRepoPath)
		if err != nil {
			return nil, fmt.Errorf("computing pool relative path: %w", err)
		}

		poolRepo := s.localRepoFactory.Build(&gitalypb.Repository{
			StorageName:  request.GetRepository().GetStorageName(),
			RelativePath: poolRelativePath,
		})

		poolLooseObjects, err := stats.LooseObjectsInfoForRepository(ctx, poolRepo, time.Now().Add(stats.StaleObjectsGracePeriod))
		if err != nil {
			return nil, fmt.Errorf("deriving pool loose objects info: %w", err)
		}

		poolPackfiles, err := stats.PackfilesInfoForRepository(ctx, poolRepo)
		if err != nil {
			return nil, fmt.Errorf("deriving pool packfiles info: %w", err)
		}

		poolSize, err := dirSizeInBytes(poolRepoPath, filter)
		if err != nil {
			return nil, fmt.Errorf("calculating pool repository size: %w", err)
		}

		repoSize += poolSize
		repoInfo = mergePoolInfo(repoInfo, stats.RepositoryInfo{
			LooseObjects: poolLooseObjects,
			Packfiles:    poolPackfiles,
		})
	}

	return convertRepositoryInfo(uint64(repoSize), repoInfo)
}

// mergePoolInfo merges pool repository stats into the member repository stats.
func mergePoolInfo(member, pool stats.RepositoryInfo) stats.RepositoryInfo {
	member.LooseObjects.Count += pool.LooseObjects.Count
	member.LooseObjects.Size += pool.LooseObjects.Size
	member.LooseObjects.StaleCount += pool.LooseObjects.StaleCount
	member.LooseObjects.StaleSize += pool.LooseObjects.StaleSize
	member.LooseObjects.GarbageCount += pool.LooseObjects.GarbageCount
	member.LooseObjects.GarbageSize += pool.LooseObjects.GarbageSize

	member.Packfiles.Count += pool.Packfiles.Count
	member.Packfiles.Size += pool.Packfiles.Size
	member.Packfiles.ReverseIndexCount += pool.Packfiles.ReverseIndexCount
	member.Packfiles.CruftCount += pool.Packfiles.CruftCount
	member.Packfiles.CruftSize += pool.Packfiles.CruftSize
	member.Packfiles.KeepCount += pool.Packfiles.KeepCount
	member.Packfiles.KeepSize += pool.Packfiles.KeepSize
	member.Packfiles.GarbageCount += pool.Packfiles.GarbageCount
	member.Packfiles.GarbageSize += pool.Packfiles.GarbageSize

	// The bitmaps and multi-pack-index are only available in the linked pool repository.
	member.Packfiles.Bitmap = pool.Packfiles.Bitmap
	member.Packfiles.MultiPackIndex = pool.Packfiles.MultiPackIndex
	member.Packfiles.MultiPackIndexBitmap = pool.Packfiles.MultiPackIndexBitmap

	return member
}

func convertRepositoryInfo(repoSize uint64, repoInfo stats.RepositoryInfo) (*gitalypb.RepositoryInfoResponse, error) {
	// The loose objects size includes objects which are older than the grace period and thus
	// stale, so we need to subtract the size of stale objects from the overall size.
	recentLooseObjectsSize := repoInfo.LooseObjects.Size - repoInfo.LooseObjects.StaleSize
	// The packfiles size includes the size of cruft packs that contain unreachable objects, so
	// we need to subtract the size of cruft packs from the overall size.
	recentPackfilesSize := repoInfo.Packfiles.Size - repoInfo.Packfiles.CruftSize

	var referenceBackend gitalypb.RepositoryInfoResponse_ReferencesInfo_ReferenceBackend
	switch repoInfo.References.ReferenceBackendName {
	case git.ReferenceBackendReftables.Name:
		referenceBackend = gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE
	case git.ReferenceBackendFiles.Name:
		referenceBackend = gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES
	default:
		return nil, fmt.Errorf("invalid reference backend")
	}

	response := &gitalypb.RepositoryInfoResponse{
		Size: repoSize,
		References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
			LooseCount:       repoInfo.References.LooseReferencesCount,
			PackedSize:       repoInfo.References.PackedReferencesSize,
			ReferenceBackend: referenceBackend,
		},
		Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
			Size:                     repoInfo.LooseObjects.Size + repoInfo.Packfiles.Size,
			RecentSize:               recentLooseObjectsSize + recentPackfilesSize,
			StaleSize:                repoInfo.LooseObjects.StaleSize + repoInfo.Packfiles.CruftSize,
			KeepSize:                 repoInfo.Packfiles.KeepSize,
			PackfileCount:            repoInfo.Packfiles.Count,
			ReverseIndexCount:        repoInfo.Packfiles.ReverseIndexCount,
			CruftCount:               repoInfo.Packfiles.CruftCount,
			KeepCount:                repoInfo.Packfiles.KeepCount,
			LooseObjectsCount:        repoInfo.LooseObjects.Count,
			StaleLooseObjectsCount:   repoInfo.LooseObjects.StaleCount,
			LooseObjectsGarbageCount: repoInfo.LooseObjects.GarbageCount,
		},
	}

	// Only set CommitGraph if it exists
	if repoInfo.CommitGraph.Exists {
		response.CommitGraph = &gitalypb.RepositoryInfoResponse_CommitGraphInfo{
			CommitGraphChainLength:    repoInfo.CommitGraph.CommitGraphChainLength,
			HasBloomFilters:           repoInfo.CommitGraph.HasBloomFilters,
			HasGenerationData:         repoInfo.CommitGraph.HasGenerationData,
			HasGenerationDataOverflow: repoInfo.CommitGraph.HasGenerationDataOverflow,
		}
	}

	// Only set Bitmap if it exists
	if repoInfo.Packfiles.Bitmap.Exists {
		response.Bitmap = &gitalypb.RepositoryInfoResponse_BitmapInfo{
			HasHashCache:   repoInfo.Packfiles.Bitmap.HasHashCache,
			HasLookupTable: repoInfo.Packfiles.Bitmap.HasLookupTable,
			Version:        uint64(repoInfo.Packfiles.Bitmap.Version),
		}
	}

	// Only set MultiPackIndex if it exists
	if repoInfo.Packfiles.MultiPackIndex.Exists {
		response.MultiPackIndex = &gitalypb.RepositoryInfoResponse_MultiPackIndexInfo{
			PackfileCount:     repoInfo.Packfiles.MultiPackIndex.PackfileCount,
			Version:           uint64(repoInfo.Packfiles.MultiPackIndex.Version),
			HasLargeOffsets:   repoInfo.Packfiles.MultiPackIndex.HasLargeOffsets,
			HasBitmappedPacks: repoInfo.Packfiles.MultiPackIndex.HasBitmappedPacks,
			HasReverseIndex:   repoInfo.Packfiles.MultiPackIndex.HasReverseIndex,
		}
	}

	// Only set MultiPackIndexBitmap if it exists
	if repoInfo.Packfiles.MultiPackIndexBitmap.Exists {
		response.MultiPackIndexBitmap = &gitalypb.RepositoryInfoResponse_BitmapInfo{
			HasHashCache:   repoInfo.Packfiles.MultiPackIndexBitmap.HasHashCache,
			HasLookupTable: repoInfo.Packfiles.MultiPackIndexBitmap.HasLookupTable,
			Version:        uint64(repoInfo.Packfiles.MultiPackIndexBitmap.Version),
		}
	}

	// Only set Alternates if the file exists, consistent with other fields
	if repoInfo.Alternates.Exists {
		response.Alternates = &gitalypb.RepositoryInfoResponse_AlternatesInfo{
			ObjectDirectories: repoInfo.Alternates.ObjectDirectories,
			LastModified:      &timestamppb.Timestamp{Seconds: repoInfo.Alternates.LastModified.Unix()},
		}
	}

	response.IsObjectPool = repoInfo.IsObjectPool
	if !repoInfo.Packfiles.LastFullRepack.IsZero() {
		response.LastFullRepack = &timestamppb.Timestamp{Seconds: repoInfo.Packfiles.LastFullRepack.Unix()}
	}

	return response, nil
}
