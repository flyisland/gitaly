package server

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedServerServiceServer
	logger            log.Logger
	gitCmdFactory     gitcmd.CommandFactory
	storages          []config.Storage
	gitSigningKeyPath string
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(deps *service.Dependencies) gitalypb.ServerServiceServer {
	return &server{
		logger:            deps.GetLogger(),
		gitCmdFactory:     deps.GetGitCmdFactory(),
		storages:          deps.GetCfg().Storages,
		gitSigningKeyPath: deps.GetCfg().Git.SigningKey,
	}
}
