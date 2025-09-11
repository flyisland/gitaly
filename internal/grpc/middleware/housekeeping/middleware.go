package housekeeping

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
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
	statsCache       *sync.Map
	statThreshold    int
}

// forceHousekeepingRPCs are all of the RPCs that we should force housekeeping right after.
var forceHousekeepingRPCs = map[string]struct{}{
	gitalypb.CleanupService_RewriteHistory_FullMethodName: {},
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
		statsCache:       &sync.Map{},
		statThreshold:    1000,
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
			// The only error that occurs here is the failure to find the method in the registry, which
			// happens for health checks and other implicit RPCs not registered with protoregistry.Registry.
			return handler(ctx, req)
		}

		targetRepo, err := methodInfo.TargetRepo(req.(proto.Message))
		if err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "lookup target repository for housekeeping")
			return handler(ctx, req)
		}

		key := m.getRepoKey(ctx, targetRepo)

		switch methodInfo.Operation {
		case protoregistry.OpMaintenance:
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
		case protoregistry.OpMutator:
			// Execute the handler first so that housekeeping incorporates the latest writes. We also ensure that
			// the scheduling logic doesn't run for invalid requests.
			resp, err := handler(ctx, req)
			if err != nil {
				return resp, err
			}

			_, forceHousekeeping := forceHousekeepingRPCs[methodInfo.FullMethodName()]
			m.scheduleHousekeeping(ctx, targetRepo, forceHousekeeping)
			return resp, err
		case protoregistry.OpAccessor:
			m.scheduleHousekeepingIfNeeded(ctx, key, targetRepo)
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
			// The only error that occurs here is the failure to find the method in the registry, which
			// happens for health checks and other implicit RPCs not registered with protoregistry.Registry.
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

		key := m.getRepoKey(ss.Context(), targetRepo)

		switch methodInfo.Operation {
		case protoregistry.OpMaintenance:
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
		case protoregistry.OpMutator:
			// Execute the handler first so that housekeeping incorporates the latest writes. We also ensure that
			// the scheduling logic doesn't run for invalid requests.
			if err := handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss)); err != nil {
				return err
			}

			_, forceHousekeeping := forceHousekeepingRPCs[methodInfo.FullMethodName()]
			m.scheduleHousekeeping(ss.Context(), targetRepo, forceHousekeeping)
			return nil
		case protoregistry.OpAccessor:
			m.scheduleHousekeepingIfNeeded(ss.Context(), key, targetRepo)
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

func (m *Middleware) scheduleHousekeeping(ctx context.Context, repo *gitalypb.Repository, force bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getRepoKey(ctx, repo)

	a, ok := m.repoActivity[key]
	if !ok {
		a = &activity{}
		m.repoActivity[key] = a
	}
	a.writeCount++

	if a.active || (a.writeCount <= m.interval && !force) {
		return
	}

	m.logger.WithField("forced", force).InfoContext(ctx, "beginning scheduled housekeeping")

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

// scheduleHousekeepingIfNeeded walks the repository path to gather file and directory counts,
// and schedules housekeeping if the total count exceeds the configured threshold.
// Uses a sync.Map cache to track repositories where statistics have been calculated, ensuring
// the calculation occurs only once per application restart. This targets accessor RPCs for
// repositories that don't trigger housekeeping through write operations, where a single stats
// check per restart is sufficient for low-activity repos.
func (m *Middleware) scheduleHousekeepingIfNeeded(ctx context.Context, key repoKey, targetRepo *gitalypb.Repository) {
	if _, statsChecked := m.statsCache.Load(key); !statsChecked {
		localRepo := m.localRepoFactory.Build(targetRepo)
		repositoryPath, err := localRepo.Path(ctx)
		if err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "housekeeping: find repo path")
			return
		}

		stats := snapshot.RepositoryStatistics{}
		if err := snapshot.WalkPathForStats(ctx, repositoryPath, &stats); err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "calculate repository statistics")
			return
		}

		m.logger.WithFields(log.Fields{
			"repository_stats": map[string]any{
				"directory_count": stats.DirectoryCount,
				"file_count":      stats.FileCount,
			},
		}).InfoContext(ctx, "collected repository statistics")

		m.statsCache.Store(key, struct{}{})
		if stats.DirectoryCount+stats.FileCount > m.statThreshold {
			m.scheduleHousekeeping(ctx, targetRepo, true)
		}
	}
}

func (m *Middleware) getRepoKey(ctx context.Context, repo *gitalypb.Repository) repoKey {
	relativePath := repo.GetRelativePath()
	if txn := storage.ExtractTransaction(ctx); txn != nil {
		relativePath = txn.OriginalRepository(repo).GetRelativePath()
	}
	return repoKey{storage: repo.GetStorageName(), relativePath: relativePath}
}
