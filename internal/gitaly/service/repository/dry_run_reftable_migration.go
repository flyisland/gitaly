package repository

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *server) DryRunReftableMigration(
	ctx context.Context,
	in *gitalypb.DryRunReftableMigrationRequest,
) (*gitalypb.DryRunReftableMigrationResponse, error) {
	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	tx := storage.ExtractTransaction(ctx)
	if tx == nil {
		return nil, structerr.NewInternal("transaction not found")
	}

	migrator := migration.NewReftableMigration(1, s.localRepoFactory)

	logger := log.FromContext(ctx)
	t := time.Now()

	if err := migrator.Fn(ctx,
		tx,
		in.GetRepository().GetStorageName(),
		tx.OriginalRepository(in.GetRepository()).GetRelativePath(),
	); err != nil {
		return nil, structerr.NewInternal("migration failed: %w", err)
	}

	duration := time.Since(t)
	resp := &gitalypb.DryRunReftableMigrationResponse{
		Time: durationpb.New(duration),
	}

	logger.WithField("migration_time", duration).Info("migration ran successfully")

	// Return an error on purpose so that the transaction is rolled back.
	return resp, structerr.NewInternal("error to rollback transaction")
}
