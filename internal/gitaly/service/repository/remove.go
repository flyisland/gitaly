package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) RemoveRepository(ctx context.Context, in *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Remove(ctx, s.logger, s.locator, s.txManager, s.repositoryCounter, repository); err != nil {
		return nil, err
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := s.migrationStateManager.RecordKeyDeletion(tx, tx.OriginalRepository(repository).GetRelativePath()); err != nil {
			return nil, structerr.NewInternal("recording migration key: %w", err)
		}
	}

	return &gitalypb.RemoveRepositoryResponse{}, nil
}
