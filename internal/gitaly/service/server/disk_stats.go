package server

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) DiskStatistics(ctx context.Context, _ *gitalypb.DiskStatisticsRequest) (*gitalypb.DiskStatisticsResponse, error) {
	var results []*gitalypb.DiskStatisticsResponse_StorageStatus
	for _, shard := range s.storages {
		shardInfo, err := getStorageStatus(shard)
		if err != nil {
			s.logger.WithField("storage", shard).WithError(err).ErrorContext(ctx, "to retrieve shard disk statistics")
			results = append(results, &gitalypb.DiskStatisticsResponse_StorageStatus{StorageName: shard.Name})
			continue
		}

		results = append(results, shardInfo)
	}

	return &gitalypb.DiskStatisticsResponse{
		StorageStatuses: results,
	}, nil
}
