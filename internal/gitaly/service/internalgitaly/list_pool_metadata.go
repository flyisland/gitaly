package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) ListPoolMetadata(req *gitalypb.ListPoolMetadataRequest, stream gitalypb.InternalGitaly_ListPoolMetadataServer) error {
	if s.poolStore == nil {
		return structerr.NewFailedPrecondition("pool metadata store not configured")
	}

	if err := s.poolStore.ForEachPoolByStorage(stream.Context(), req.GetStorageName(), func(pool *relational.PoolMetadata) error {
		if err := stream.Send(&gitalypb.ListPoolMetadataResponse{PoolDiskPath: pool.DiskPath}); err != nil {
			return structerr.NewInternal("send: %w", err)
		}
		return nil
	}); err != nil {
		return structerr.NewInternal("list pools: %w", err)
	}

	return nil
}
