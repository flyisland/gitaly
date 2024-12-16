package ref

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedRefServiceServer
	logger           log.Logger
	txManager        transaction.Manager
	locator          storage.Locator
	catfileCache     catfile.Cache
	localRepoFactory localrepo.Factory
}

// NewServer creates a new instance of a grpc RefServer
func NewServer(deps *service.Dependencies) gitalypb.RefServiceServer {
	return &server{
		logger:           deps.GetLogger(),
		txManager:        deps.GetTxManager(),
		locator:          deps.GetLocator(),
		catfileCache:     deps.GetCatfileCache(),
		localRepoFactory: deps.GetRepositoryFactory(),
	}
}
