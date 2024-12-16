package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
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

	repoSize, err := dirSizeInBytes(repoPath)
	if err != nil {
		return nil, fmt.Errorf("calculating repository size: %w", err)
	}

	repoInfo, err := stats.RepositoryInfoForRepository(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}

	return convertRepositoryInfo(uint64(repoSize), repoInfo)
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

	return &gitalypb.RepositoryInfoResponse{
		Size: repoSize,
		References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
			LooseCount:       repoInfo.References.LooseReferencesCount,
			PackedSize:       repoInfo.References.PackedReferencesSize,
			ReferenceBackend: referenceBackend,
		},
		Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
			Size:       repoInfo.LooseObjects.Size + repoInfo.Packfiles.Size,
			RecentSize: recentLooseObjectsSize + recentPackfilesSize,
			StaleSize:  repoInfo.LooseObjects.StaleSize + repoInfo.Packfiles.CruftSize,
			KeepSize:   repoInfo.Packfiles.KeepSize,
		},
	}, nil
}
