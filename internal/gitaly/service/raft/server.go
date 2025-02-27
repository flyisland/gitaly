package raft

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Server is a gRPC server for the Raft service.
type Server struct {
	gitalypb.UnimplementedRaftServiceServer
	logger log.Logger
	node   storage.Node
	cfg    config.Cfg
}

// NewServer creates a new Raft gRPC server.
func NewServer(deps *service.Dependencies) *Server {
	return &Server{
		logger: deps.GetLogger(),
		node:   deps.GetNode(),
		cfg:    deps.GetCfg(),
	}
}
