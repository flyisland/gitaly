package remote

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedRemoteServiceServer
	logger           log.Logger
	locator          storage.Locator
	gitCmdFactory    gitcmd.CommandFactory
	catfileCache     catfile.Cache
	txManager        transaction.Manager
	localRepoFactory localrepo.Factory
	conns            *client.Pool
}

// NewServer creates a new instance of a grpc RemoteServiceServer
func NewServer(deps *service.Dependencies) gitalypb.RemoteServiceServer {
	return &server{
		logger:           deps.GetLogger(),
		locator:          deps.GetLocator(),
		gitCmdFactory:    deps.GetGitCmdFactory(),
		catfileCache:     deps.GetCatfileCache(),
		txManager:        deps.GetTxManager(),
		conns:            deps.GetConnsPool(),
		localRepoFactory: deps.GetRepositoryFactory(),
	}
}
