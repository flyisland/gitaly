package hook

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedHookServiceServer
	logger             log.Logger
	manager            gitalyhook.Manager
	locator            storage.Locator
	gitCmdFactory      gitcmd.CommandFactory
	packObjectsCache   streamcache.Cache
	packObjectsLimiter limiter.Limiter
	txRegistry         *storagemgr.TransactionRegistry
	runPackObjectsFn   func(
		context.Context,
		gitcmd.CommandFactory,
		io.Writer,
		*gitalypb.PackObjectsHookWithSidechannelRequest,
		*packObjectsArgs,
		io.Reader,
		string,
	) error
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(deps *service.Dependencies) gitalypb.HookServiceServer {
	srv := &server{
		logger:             deps.GetLogger(),
		manager:            deps.GetHookManager(),
		locator:            deps.GetLocator(),
		gitCmdFactory:      deps.GetGitCmdFactory(),
		packObjectsCache:   deps.GetPackObjectsCache(),
		packObjectsLimiter: deps.GetPackObjectsLimiter(),
		txRegistry:         deps.GetTransactionRegistry(),
		runPackObjectsFn:   runPackObjects,
	}

	return srv
}
