package objectpool

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// DisconnectGitAlternates is a slightly dangerous RPC. It optimistically hard-links all alternate
// objects we might need, and then temporarily removes (renames) objects/info/alternates and runs
// a connectivity check. If we are unlucky that leaves the repository in a broken state during the
// connectivity check. If we are very unlucky and Gitaly crashes, the repository stays in a broken
// state until an administrator intervenes and restores the backed-up copy of
// objects/info/alternates.
func (s *server) DisconnectGitAlternates(ctx context.Context, req *gitalypb.DisconnectGitAlternatesRequest) (*gitalypb.DisconnectGitAlternatesResponse, error) {
	repository := req.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)

	storageRoot, err := s.locator.GetStorageByName(ctx, repo.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("storage by name: %w", err)
	}

	f := storage.NewNoopFS(storageRoot)
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		f = tx.FS()
	}

	if err := objectpool.Disconnect(ctx, f, repo, s.logger, s.txManager); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.DisconnectGitAlternatesResponse{}, nil
}
