package internalgitaly

import (
	"io/fs"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
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

	sendPoolMember := func(relPath string, _ fs.FileInfo) error {
		repoPath := filepath.Join(storagePath, relPath)

		altInfo, err := stats.AlternatesInfoForRepository(repoPath)
		if err != nil {
			return nil
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
			return nil
		}
		poolDiskPath = filepath.ToSlash(poolDiskPath)

		return stream.Send(&gitalypb.ScanPoolMetadataResponse{
			RelativePath: relPath,
			PoolDiskPath: poolDiskPath,
		})
	}

	if err := walk.FindRepositories(ctx, s.locator, storageName, sendPoolMember); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}
