package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/relational"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedInternalGitalyServer
	logger    log.Logger
	storages  []config.Storage
	locator   storage.Locator
	poolStore relational.PoolStore
}

// NewServer return an instance of the Gitaly service.
func NewServer(deps *service.Dependencies) gitalypb.InternalGitalyServer {
	return &server{
		logger:    deps.GetLogger(),
		storages:  deps.GetCfg().Storages,
		locator:   deps.GetLocator(),
		poolStore: deps.GetPoolMetadataStore(),
	}
}
