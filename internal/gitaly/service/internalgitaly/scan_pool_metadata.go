package internalgitaly

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/walk"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) ScanPoolMetadata(req *gitalypb.ScanPoolMetadataRequest, stream gitalypb.InternalGitaly_ScanPoolMetadataServer) error {
	ctx := stream.Context()
	storageName := req.GetStorageName()

	storagePath, err := s.locator.GetStorageByName(ctx, storageName)
	if err != nil {
		return structerr.NewInvalidArgument("get storage: %w", err)
	}

	processPoolMember := processPoolMemberFunc(ctx, storagePath, stream)

	if err := walk.FindRepositories(ctx, s.locator, storageName, processPoolMember); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func processPoolMemberFunc(_ context.Context, storagePath string, stream gitalypb.InternalGitaly_ScanPoolMetadataServer) func(relPath string, _ fs.FileInfo) error {
	invalidPools := make(map[string]bool)

	return func(relPath string, fi fs.FileInfo) error {
		repoPath := filepath.Join(storagePath, relPath)

		altInfo, err := stats.AlternatesInfoForRepository(repoPath)
		if err != nil {
			return fmt.Errorf("read alternates for %q: %w", relPath, err)
		}

		if !altInfo.Exists || len(altInfo.ObjectDirectories) == 0 {
			return nil
		}

		absPoolPaths := altInfo.AbsoluteObjectDirectories()
		if len(absPoolPaths) == 0 {
			return nil
		}

		poolObjectDir := absPoolPaths[0]
		poolRepoPath := filepath.Dir(poolObjectDir)

		poolDiskPath, err := filepath.Rel(storagePath, poolRepoPath)
		if err != nil {
			return fmt.Errorf("compute relative path for pool %q (repo %q): %w", poolRepoPath, relPath, err)
		}
		poolDiskPath = filepath.ToSlash(poolDiskPath)

		// We could encounter the same invalid pool multiple times.
		if invalidPools[poolDiskPath] {
			return nil
		}

		if err := storage.ValidateGitDirectory(poolRepoPath); err != nil {
			invalidPools[poolDiskPath] = true
			return nil
		}

		return stream.Send(&gitalypb.ScanPoolMetadataResponse{
			RelativePath: relPath,
			PoolDiskPath: poolDiskPath,
		})
	}
}
