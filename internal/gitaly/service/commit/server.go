package commit

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedCommitServiceServer
	logger           log.Logger
	locator          storage.Locator
	gitCmdFactory    gitcmd.CommandFactory
	catfileCache     catfile.Cache
	cfg              config.Cfg
	localRepoFactory localrepo.Factory
}

// NewServer creates a new instance of a grpc CommitServiceServer
func NewServer(deps *service.Dependencies) gitalypb.CommitServiceServer {
	return &server{
		logger:           deps.GetLogger(),
		locator:          deps.GetLocator(),
		gitCmdFactory:    deps.GetGitCmdFactory(),
		catfileCache:     deps.GetCatfileCache(),
		cfg:              deps.GetCfg(),
		localRepoFactory: deps.GetRepositoryFactory(),
	}
}
