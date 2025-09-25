package server

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"google.golang.org/grpc"
)

// GitalyServerFactory is a factory of gitaly grpc servers
type GitalyServerFactory struct {
	registry               *backchannel.Registry
	cacheInvalidator       cache.Invalidator
	limitHandlers          []*limithandler.LimiterMiddleware
	cfg                    config.Cfg
	logger                 log.Logger
	externalServers        []*grpc.Server
	internalServers        []*grpc.Server
	txMiddleware           TransactionMiddleware
	housekeepingMiddleware *housekeeping.Middleware
}

// TransactionMiddleware collects transaction middleware into a single struct that can be
// provided to enable transactions and transaction related logic.
type TransactionMiddleware struct {
	// UnaryInterceptors are the unary RPC interceptors that handles transaction logic.
	UnaryInterceptors []grpc.UnaryServerInterceptor
	// StreamInterceptor are the stream RPC interceptors that handles transaction logic.
	StreamInterceptors []grpc.StreamServerInterceptor
}

// NewGitalyServerFactory allows to create and start secure/insecure 'grpc.Server's.
func NewGitalyServerFactory(
	cfg config.Cfg,
	logger log.Logger,
	registry *backchannel.Registry,
	cacheInvalidator cache.Invalidator,
	limitHandlers []*limithandler.LimiterMiddleware,
	housekeepingMiddleware *housekeeping.Middleware,
	txMiddleware TransactionMiddleware,
) *GitalyServerFactory {
	return &GitalyServerFactory{
		cfg:                    cfg,
		logger:                 logger,
		registry:               registry,
		cacheInvalidator:       cacheInvalidator,
		limitHandlers:          limitHandlers,
		txMiddleware:           txMiddleware,
		housekeepingMiddleware: housekeepingMiddleware,
	}
}

// Stop immediately stops all servers created by the GitalyServerFactory.
func (s *GitalyServerFactory) Stop() {
	for _, servers := range [][]*grpc.Server{
		s.externalServers,
		s.internalServers,
	} {
		for _, server := range servers {
			server.Stop()
		}
	}
}

// GracefulStop gracefully stops all servers created by the GitalyServerFactory. ExternalServers
// are stopped before the internal servers to ensure any RPCs accepted by the externals servers
// can still complete their requests to the internal servers. This is important for hooks calling
// back to Gitaly.
func (s *GitalyServerFactory) GracefulStop() {
	for _, servers := range [][]*grpc.Server{
		s.externalServers,
		s.internalServers,
	} {
		var wg sync.WaitGroup

		for _, server := range servers {
			wg.Add(1)
			go func(server *grpc.Server) {
				defer wg.Done()
				server.GracefulStop()
			}(server)
		}

		wg.Wait()
	}
}

// CreateExternal creates a new external gRPC server. The external servers are closed
// before the internal servers when gracefully shutting down.
func (s *GitalyServerFactory) CreateExternal(secure bool, opts ...Option) (*grpc.Server, error) {
	server, err := s.New(true, secure, opts...)
	if err != nil {
		return nil, err
	}

	s.externalServers = append(s.externalServers, server)
	return server, nil
}

// CreateInternal creates a new internal gRPC server. Internal servers are closed
// after the external ones when gracefully shutting down.
func (s *GitalyServerFactory) CreateInternal(opts ...Option) (*grpc.Server, error) {
	server, err := s.New(false, false, opts...)
	if err != nil {
		return nil, err
	}

	s.internalServers = append(s.internalServers, server)
	return server, nil
}
