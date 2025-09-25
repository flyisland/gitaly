package repository

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
)

// CreateRepositoryFromBundle creates a Git repository at the specified storage and path, if it does
// not already exist, from the provided Git bundle.
func (s *server) CreateRepositoryFromBundle(stream gitalypb.RepositoryService_CreateRepositoryFromBundleServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("first request failed: %w", err)
	}

	repo := firstRequest.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repo, storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	firstRead := false
	bundleReader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	if err := repoutil.Create(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.repositoryCounter, repo, func(repo *gitalypb.Repository) error {
		if err := s.localRepoFactory.Build(repo).CloneBundle(ctx, bundleReader); err != nil {
			return structerr.NewInternal("cloning bundle: %w", err)
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		return structerr.NewInternal("creating repository: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := s.migrationStateManager.RecordKeyCreation(
			tx,
			tx.OriginalRepository(repo).GetRelativePath(),
		); err != nil {
			return structerr.NewInternal("recording migration key: %w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}
