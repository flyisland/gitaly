package remoterepo_test

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setupGitalyServer(t *testing.T) config.Cfg {
	cfg := testcfg.Build(t)

	// gitaly-hooks is needed for reference updates to be captured with transactions.
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
	})

	return cfg
}
