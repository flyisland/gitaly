package manager

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// RehydrateRepository restores an offloaded repository by downloading objects from remote
// storage back to local storage.
//
// The `prefix` parameter identifies the path in remote storage where the repository’s objects
// are located. It is derived by removing the GoCloudURL prefix from the configured
// remote.offload.url.
//
// For example:
//   - Git config: remote.offload.url = "gcp://my_bucket/@hash/11/22/112233abc.git/my_uuid"
//   - Gitaly config: GoCloudURL = "gcp://my_bucket"
//     then, Prefix is "@hash/11/22/112233abc.git/my_uuid"
func (m *RepositoryManager) RehydrateRepository(ctx context.Context, repo *localrepo.Repo, prefix string) error {
	if m.node == nil {
		return fmt.Errorf("unable to retrieve storage node")
	}

	return m.runInTransaction(ctx, "housekeeping/rehydrate", false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		tx.SetRehydratingConfig(prefix)
		return nil
	})
}
