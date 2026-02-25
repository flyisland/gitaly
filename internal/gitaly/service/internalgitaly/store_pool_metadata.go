package internalgitaly

import (
	"errors"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) StorePoolMetadata(stream gitalypb.InternalGitaly_StorePoolMetadataServer) error {
	if s.poolStore == nil {
		return structerr.NewFailedPrecondition("pool metadata store not configured")
	}

	poolsByDiskPath := make(map[string]*relational.PoolMetadata)
	var storageName string

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return structerr.NewInternal("receive: %w", err)
		}

		if storageName == "" {
			storageName = req.GetStorageName()
		}

		relPath := req.GetRelativePath()
		poolDiskPath := req.GetPoolDiskPath()
		isUpstream := req.GetIsUpstream()

		if _, exists := poolsByDiskPath[poolDiskPath]; !exists {
			poolsByDiskPath[poolDiskPath] = &relational.PoolMetadata{
				DiskPath:    poolDiskPath,
				StorageNode: storageName,
				Members:     []string{},
				UpdatedAt:   time.Now(),
			}
		}

		pool := poolsByDiskPath[poolDiskPath]
		pool.Members = append(pool.Members, relPath)
		if isUpstream {
			pool.Upstream = relPath
		}
	}

	if len(poolsByDiskPath) > 0 {
		if err := s.poolStore.StorePoolData(stream.Context(), poolsByDiskPath); err != nil {
			return structerr.NewInternal("store pool data: %w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.StorePoolMetadataResponse{})
}
