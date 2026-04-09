package internalgitaly

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/walk"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
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

	processPoolMember := processPoolMemberFunc(ctx, storagePath, storageName, s.gitlabClient, stream)

	if err := walk.FindRepositories(ctx, s.locator, storageName, processPoolMember); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func processPoolMemberFunc(ctx context.Context, storagePath, storageName string, gitlabClient gitlab.Client, stream gitalypb.InternalGitaly_ScanPoolMetadataServer) func(relPath string, _ fs.FileInfo) error {
	poolUpstreams := make(map[string]gitlab.ObjectPoolMember)
	invalidPools := make(map[string]bool)
	var poolUpstreamsMu sync.Mutex

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

		poolUpstreamsMu.Lock()
		defer poolUpstreamsMu.Unlock()

		if invalidPools[poolDiskPath] {
			return nil
		}

		if _, ok := poolUpstreams[poolDiskPath]; !ok {
			if err := storage.ValidateGitDirectory(poolRepoPath); err != nil {
				invalidPools[poolDiskPath] = true
				return nil
			}

			members, err := gitlabClient.ObjectPoolMembers(ctx, strings.TrimSuffix(poolDiskPath, ".git"), storageName, true)
			if err != nil {
				return fmt.Errorf("query Rails for pool %q (repo %q): %w", poolDiskPath, relPath, err)
			}

			poolUpstreams[poolDiskPath] = gitlab.ObjectPoolMember{}

			// There should only be one upstream. If there's no upstream, we don't error here
			// in order to reveal the issue back to the user.
			if len(members) == 1 && members[0].Public {
				poolUpstreams[poolDiskPath] = members[0]
			}
		}

		var isUpstream bool
		if member, ok := poolUpstreams[poolDiskPath]; ok {
			if member.RelativePath == relPath {
				isUpstream = true
			}
		}

		if err := stream.Send(&gitalypb.ScanPoolMetadataResponse{
			RelativePath: relPath,
			PoolDiskPath: poolDiskPath,
			IsUpstream:   isUpstream,
		}); err != nil {
			return fmt.Errorf("send response for repo %q: %w", relPath, err)
		}

		return nil
	}
}
