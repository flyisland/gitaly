package conflicts

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedConflictsServiceServer
	logger           log.Logger
	locator          storage.Locator
	catfileCache     catfile.Cache
	pool             *client.Pool
	hookManager      hook.Manager
	updater          *updateref.UpdaterWithHooks
	localRepoFactory localrepo.Factory
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(deps *service.Dependencies) gitalypb.ConflictsServiceServer {
	return &server{
		logger:           deps.GetLogger(),
		hookManager:      deps.GetHookManager(),
		locator:          deps.GetLocator(),
		catfileCache:     deps.GetCatfileCache(),
		pool:             deps.GetConnsPool(),
		updater:          deps.GetUpdaterWithHooks(),
		localRepoFactory: deps.GetRepositoryFactory(),
	}
}

func (s *server) quarantinedRepo(ctx context.Context, repo *gitalypb.Repository) (*quarantine.Dir, *localrepo.Repo, func(), error) {
	quarantineDir, cleanup, err := quarantine.New(ctx, repo, s.logger, s.locator)
	if err != nil {
		return nil, nil, nil, structerr.NewInternal("creating object quarantine: %w", err)
	}

	quarantineRepo := s.localRepoFactory.Build(quarantineDir.QuarantinedRepo())
	return quarantineDir, quarantineRepo, cleanup, nil
}
