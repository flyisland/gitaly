package objectpool

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) DeleteObjectPool(ctx context.Context, in *gitalypb.DeleteObjectPoolRequest) (*gitalypb.DeleteObjectPoolResponse, error) {
	pool, err := s.poolForRequest(ctx, in)
	if err != nil {
		if errors.Is(err, objectpool.ErrInvalidPoolRepository) {
			// TODO: we really should return an error in case we're trying to delete an
			// object pool that does not exist.
			return &gitalypb.DeleteObjectPoolResponse{}, nil
		}

		return nil, err
	}

	if err := repoutil.Remove(ctx, s.logger, s.locator, nil, s.repositoryCounter, pool); err != nil {
		return nil, fmt.Errorf("remove: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		poolRepo := in.GetObjectPool().GetRepository()
		if err := s.migrationStateManager.RecordKeyDeletion(tx, tx.OriginalRepository(poolRepo).GetRelativePath()); err != nil {
			return nil, structerr.NewInternal("recording migration key: %w", err)
		}
	}

	return &gitalypb.DeleteObjectPoolResponse{}, nil
}
