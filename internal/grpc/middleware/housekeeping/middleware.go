package housekeeping

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/middleware"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// activity tracks housekeeping activity for a specific relative path.
type activity struct {
	writeCount int
	active     bool
}

type repoKey struct {
	storage, relativePath string
}

// Middleware manages scheduling of housekeeping tasks by intercepting gRPC requests.
type Middleware struct {
	interval     int
	repoActivity map[repoKey]*activity

	mu sync.Mutex
	wg sync.WaitGroup

	logger           log.Logger
	registry         *protoregistry.Registry
	manager          manager.Manager
	localRepoFactory localrepo.Factory
}

// NewHousekeepingMiddleware returns a new middleware.
func NewHousekeepingMiddleware(logger log.Logger, registry *protoregistry.Registry, factory localrepo.Factory, manager manager.Manager, interval int) *Middleware {
	return &Middleware{
		interval:         interval,
		logger:           logger,
		registry:         registry,
		localRepoFactory: factory,
		manager:          manager,
		repoActivity:     make(map[repoKey]*activity),
	}
}

// WaitForWorkers waits for any active housekeeping tasks to finish.
func (m *Middleware) WaitForWorkers() {
	m.wg.Wait()
}

// UnaryServerInterceptor returns gRPC unary middleware.
func (m *Middleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if featureflag.HousekeepingMiddleware.IsDisabled(ctx) {
			return handler(ctx, req)
		}

		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "lookup method for housekeeping")
			return handler(ctx, req)
		}

		targetRepo, err := methodInfo.TargetRepo(req.(proto.Message))
		if err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "lookup target repository for housekeeping")
			return handler(ctx, req)
		}

		key := m.getRepoKey(targetRepo)

		if methodInfo.Operation == protoregistry.OpMaintenance {
			m.mu.Lock()

			if m.isActive(key) {
				m.mu.Unlock()
				return nil, structerr.NewAlreadyExists("housekeeping already executing for repository")
			}
			m.markHousekeepingActive(key)

			m.mu.Unlock()

			resp, err := handler(ctx, req)

			m.markHousekeepingInactive(key)

			return resp, err
		}

		if methodInfo.Operation == protoregistry.OpMutator {
			// Execute the handler first so that housekeeping incorporates the latest writes. We also ensure that
			// the scheduling logic doesn't run for invalid requests.
			resp, err := handler(ctx, req)
			if err != nil {
				return resp, err
			}

			m.scheduleHousekeeping(ctx, targetRepo)
			return resp, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns gRPC stream request middleware.
func (m *Middleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if featureflag.HousekeepingMiddleware.IsDisabled(ss.Context()) {
			return handler(srv, ss)
		}

		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			m.logger.WithError(err).ErrorContext(ss.Context(), "lookup method for housekeeping")
			return handler(srv, ss)
		}

		req := methodInfo.NewRequest()
		if err := ss.RecvMsg(req); err != nil {
			m.logger.WithError(err).ErrorContext(ss.Context(), "lookup target repository for housekeeping")
			return handler(srv, middleware.NewPeekedStream(ss.Context(), nil, err, ss))
		}

		targetRepo, err := methodInfo.TargetRepo(req)
		if err != nil {
			return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
		}

		key := m.getRepoKey(targetRepo)

		if methodInfo.Operation == protoregistry.OpMaintenance {
			m.mu.Lock()
			if m.isActive(key) {
				m.mu.Unlock()
				return structerr.NewAlreadyExists("housekeeping already executing for repository")
			}

			m.markHousekeepingActive(key)
			m.mu.Unlock()

			// Ensure that the first message we consumed earlier is relayed to the client.
			err = handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))

			m.markHousekeepingInactive(key)

			return err
		}

		if methodInfo.Operation == protoregistry.OpMutator {
			// Execute the handler first so that housekeeping incorporates the latest writes. We also ensure that
			// the scheduling logic doesn't run for invalid requests.
			if err := handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss)); err != nil {
				return err
			}

			m.scheduleHousekeeping(ss.Context(), targetRepo)
			return nil
		}

		// Ensure that the first message we consumed earlier is relayed to the client.
		return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
	}
}

func (m *Middleware) markHousekeepingActive(key repoKey) {
	a, ok := m.repoActivity[key]
	if !ok {
		a = &activity{}
		m.repoActivity[key] = a
	}

	a.active = true
	// Reset the counter at the start so we can track the number of write RPCs that executed while a housekeeping
	// job is active.
	a.writeCount = 0
}

func (m *Middleware) markHousekeepingInactive(key repoKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.repoActivity[key]
	if !ok {
		a = &activity{}
		m.repoActivity[key] = a
	}

	// Since we reset the counter at the beginning of housekeeping, if the counter remains at 0 after housekeeping
	// it means the repository is low-activity. We can remove the entry in the map if so.
	if a.writeCount == 0 {
		delete(m.repoActivity, key)
		return
	}

	a.active = false
}

func (m *Middleware) isActive(key repoKey) bool {
	a, ok := m.repoActivity[key]
	if !ok {
		return false
	}

	return a.active
}

func (m *Middleware) scheduleHousekeeping(ctx context.Context, repo *gitalypb.Repository) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getRepoKey(repo)

	a, ok := m.repoActivity[key]
	if !ok {
		a = &activity{}
		m.repoActivity[key] = a
	}
	a.writeCount++

	if a.writeCount <= m.interval || a.active {
		return
	}

	m.logger.InfoContext(ctx, "beginning scheduled housekeeping")

	m.markHousekeepingActive(key)

	m.wg.Add(1)
	go func() {
		// We need to call OptimizeRepository with a child context that's disowned from the parent's
		// cancellation signals we're executing it asynchronously. Providing the existing `ctx` would
		// cause it to fail, since `ctx` would be cancelled when this request completes.
		housekeepingCtx, housekeepingCancel := context.WithCancel(context.WithoutCancel(ctx))

		defer func() {
			m.markHousekeepingInactive(key)
			m.logger.InfoContext(housekeepingCtx, "ended scheduled housekeeping")
			housekeepingCancel()
			m.wg.Done()
		}()

		localRepo := m.localRepoFactory.Build(repo)
		if err := m.manager.OptimizeRepository(housekeepingCtx, localRepo, manager.WithOptimizationStrategyConstructor(
			func(info stats.RepositoryInfo) housekeeping.OptimizationStrategy {
				return housekeeping.NewHeuristicalOptimizationStrategy(info)
			},
		)); err != nil {
			m.logger.WithError(err).ErrorContext(housekeepingCtx, "failed scheduled housekeeping")
		}
	}()
}

func (m *Middleware) getRepoKey(repo *gitalypb.Repository) repoKey {
	return repoKey{storage: repo.GetStorageName(), relativePath: repo.GetRelativePath()}
}
