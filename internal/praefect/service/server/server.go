package server

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// Server is a ServerService server
type Server struct {
	gitalypb.UnimplementedServerServiceServer
	conf   config.Config
	logger log.Logger
	conns  service.Connections
	checks []service.CheckFunc
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(conf config.Config, logger log.Logger, conns service.Connections, checks []service.CheckFunc) gitalypb.ServerServiceServer {
	s := &Server{
		conf:   conf,
		logger: logger,
		conns:  conns,
		checks: checks,
	}

	return s
}
