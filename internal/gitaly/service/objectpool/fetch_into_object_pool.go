package objectpool

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) FetchIntoObjectPool(ctx context.Context, req *gitalypb.FetchIntoObjectPoolRequest) (*gitalypb.FetchIntoObjectPoolResponse, error) {
	if err := validateFetchIntoObjectPoolRequest(req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	objectPool, err := objectpool.FromProto(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.housekeepingManager, req.GetObjectPool())
	if err != nil {
		return nil, structerr.NewInvalidArgument("object pool invalid: %w", err)
	}

	origin := s.localRepoFactory.Build(req.GetOrigin())

	if err := objectPool.FetchFromOrigin(ctx, origin, func(repo *gitalypb.Repository) *localrepo.Repo {
		return s.localRepoFactory.Build(repo)
	}); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	originalPoolRepo := objectPool.Repo
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		originalPoolRepo = s.localRepoFactory.Build(tx.OriginalRepository(&gitalypb.Repository{
			StorageName:  req.GetObjectPool().GetRepository().GetStorageName(),
			RelativePath: req.GetObjectPool().GetRepository().GetRelativePath(),
		}))
	}

	// When transactions are enabled, housekeeping tasks are scheduled on the transaction (by operations
	// like OptimizeRepository) but are only executed when the transaction is committed.
	// Therefore, we start another transaction here to read the state of the repository after the
	// housekeeping executes as part of the previous transaction.
	//
	// Once housekeeping has been extracted out, we can avoid the transaction here and just read the
	// state before committing the OptimizeRepository operation.
	if err := s.executeMaybeWithTransaction(ctx, originalPoolRepo, func(repo *localrepo.Repo) error {
		stats.LogRepositoryInfo(ctx, s.logger, repo)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("execute maybe with transaction: %w", err)
	}

	return &gitalypb.FetchIntoObjectPoolResponse{}, nil
}

func (s *server) executeMaybeWithTransaction(ctx context.Context, repo *localrepo.Repo, execute func(*localrepo.Repo) error) (returnedErr error) {
	if s.node == nil {
		return execute(repo)
	}

	storageHandle, err := s.node.GetStorage(repo.GetStorageName())
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	transaction, err := storageHandle.Begin(ctx, storage.TransactionOptions{
		ReadOnly:     true,
		RelativePath: repo.GetRelativePath(),
	})
	if err != nil {
		return fmt.Errorf("fail to initiate WAL transaction: %w", err)
	}

	defer func() {
		if returnedErr != nil {
			if err := transaction.Rollback(ctx); err != nil {
				s.logger.WithError(err).Error("failed to rollback WAL transaction")
			}
		}
	}()

	if err := execute(s.localRepoFactory.Build(transaction.RewriteRepository(&gitalypb.Repository{
		StorageName:  repo.GetStorageName(),
		RelativePath: repo.GetRelativePath(),
	}))); err != nil {
		return err
	}

	commitLSN, err := transaction.Commit(ctx)
	if err != nil {
		return fmt.Errorf("fail to commit WAL transaction: %w", err)
	}

	storage.LogTransactionCommit(ctx, s.logger, commitLSN, "FetchIntoObjectPool housekeeping")

	return nil
}

func validateFetchIntoObjectPoolRequest(req *gitalypb.FetchIntoObjectPoolRequest) error {
	if req.GetOrigin() == nil {
		return errors.New("origin is empty")
	}

	if req.GetObjectPool() == nil {
		return errors.New("object pool is empty")
	}

	originRepository, poolRepository := req.GetOrigin(), req.GetObjectPool().GetRepository()

	if originRepository.GetStorageName() != poolRepository.GetStorageName() {
		return errors.New("origin has different storage than object pool")
	}

	return nil
}
