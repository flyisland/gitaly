package repository

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *server) MigrateReferenceBackend(
	ctx context.Context,
	in *gitalypb.MigrateReferenceBackendRequest,
) (*gitalypb.MigrateReferenceBackendResponse, error) {
	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	tx := storage.ExtractTransaction(ctx)
	if tx == nil {
		return nil, structerr.NewInternal("transaction not found")
	}

	targetBackend := git.ReferenceBackendFiles
	switch in.GetTargetReferenceBackend() {
	case gitalypb.MigrateReferenceBackendRequest_REFERENCE_BACKEND_UNSPECIFIED:
		return nil, structerr.NewInvalidArgument("target reference backend not set")
	case gitalypb.MigrateReferenceBackendRequest_REFERENCE_BACKEND_REFTABLE:
		targetBackend = git.ReferenceBackendReftables
	}

	migrator := migration.NewReferenceBackendMigration(1, targetBackend, s.localRepoFactory, nil)

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
	resp := &gitalypb.MigrateReferenceBackendResponse{
		Time: durationpb.New(duration),
	}

	logger.WithField("migration_time", duration).Info("migration ran successfully")

	return resp, nil
}
