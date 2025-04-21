package manager

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// OffloadRepository tells a transaction that this repository needs to be offloaded with the specified configuration.
func (m *RepositoryManager) OffloadRepository(ctx context.Context, repo *localrepo.Repo, cfg config.OffloadingConfig) error {
	if m.node == nil {
		return structerr.NewFailedPrecondition("unable to retrieve storage node")
	}

	return m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		if tx != nil {
			tx.OffloadRepository(cfg)

			return nil
		}

		return fmt.Errorf("missing transaction")
	})
}
