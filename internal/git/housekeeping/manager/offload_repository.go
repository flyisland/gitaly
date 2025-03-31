package manager

import (
	"context"
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// OffloadRepository tells a transaction that this repository needs to be offloaded with the specified configuration.
func (m *RepositoryManager) OffloadRepository(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadingConfig) error {
	if m.node == nil {
		return fmt.Errorf("unable to retrieve storage node")
	}

	return m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		originalRepo := &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: repo.GetRelativePath(),
		}
		if tx != nil {
			originalRepo = tx.OriginalRepository(originalRepo)
			txID := storage.ExtractTransactionID(ctx)
			// Use original repo's relative path + transaction ID as prefix when in
			// uploading to an offloading storage.
			cfg.Prefix = filepath.Join(originalRepo.GetRelativePath(), fmt.Sprintf("%d", txID))

			if err := validateOffloadingConfig(cfg); err != nil {
				return err
			}

			tx.OffloadRepository(cfg)

			return nil
		}

		return fmt.Errorf("missing transaction")
	})
}

func validateOffloadingConfig(cfg config.OffloadingConfig) error {
	if cfg.Filter == "" {
		return fmt.Errorf("offloading configuration missing filter")
	}
	if cfg.SinkURL == "" {
		return fmt.Errorf("offloading configuration missing sink URL")
	}
	if cfg.Sink == nil {
		return fmt.Errorf("offloading configuration missing sink")
	}
	if cfg.OriginalRepo == "" {
		return fmt.Errorf("offloading configuration missing the absolute original repo path")
	}
	if cfg.CachePath == "" {
		return fmt.Errorf("offloading configuration missing the absolute cache folder path")
	}

	return nil
}
