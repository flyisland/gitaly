package diff

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const msgSizeThreshold = 5 * 1024

type server struct {
	gitalypb.UnimplementedDiffServiceServer
	MsgSizeThreshold int
	logger           log.Logger
	locator          storage.Locator
	gitCmdFactory    gitcmd.CommandFactory
	catfileCache     catfile.Cache
	localRepoFactory localrepo.Factory
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(deps *service.Dependencies) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		logger:           deps.GetLogger(),
		locator:          deps.GetLocator(),
		gitCmdFactory:    deps.GetGitCmdFactory(),
		catfileCache:     deps.GetCatfileCache(),
		localRepoFactory: deps.GetRepositoryFactory(),
	}
}
