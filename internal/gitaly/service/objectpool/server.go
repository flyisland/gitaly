package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedObjectPoolServiceServer
	logger                log.Logger
	locator               storage.Locator
	gitCmdFactory         gitcmd.CommandFactory
	catfileCache          catfile.Cache
	txManager             transaction.Manager
	node                  storage.Node
	housekeepingManager   housekeepingmgr.Manager
	repositoryCounter     *counter.RepositoryCounter
	localRepoFactory      localrepo.Factory
	migrationStateManager migration.StateManager
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(deps *service.Dependencies) gitalypb.ObjectPoolServiceServer {
	return &server{
		logger:                deps.GetLogger(),
		locator:               deps.GetLocator(),
		gitCmdFactory:         deps.GetGitCmdFactory(),
		catfileCache:          deps.GetCatfileCache(),
		txManager:             deps.GetTxManager(),
		node:                  deps.GetNode(),
		housekeepingManager:   deps.GetHousekeepingManager(),
		repositoryCounter:     deps.GetRepositoryCounter(),
		localRepoFactory:      deps.GetRepositoryFactory(),
		migrationStateManager: deps.GetMigrationStateManager(),
	}
}
