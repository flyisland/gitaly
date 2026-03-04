package housekeeping

import (
	"context"
	"slices"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/middleware"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// OperationThreshold defines when a specific operation should be triggered.
type OperationThreshold struct {
	// RPCInterval is the number of write RPCs after which this operation should potentially run.
	RPCInterval int
	// StatThreshold is the minimum file + directory count to trigger this operation on accessor RPCs.
	StatThreshold int
}

// MiddlewareConfig holds configuration for all housekeeping operations.
type MiddlewareConfig struct {
	// OperationThresholds maps each operation type to its triggering thresholds.
	OperationThresholds map[config.OperationType]OperationThreshold
	// DefaultThresholds provides a default threshold for operations which aren't explicitly configured.
	DefaultThresholds OperationThreshold
}

// DefaultMiddlewareConfig returns the default configuration with pack-refs running more frequently.
func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		DefaultThresholds: OperationThreshold{
			RPCInterval:   20,
			StatThreshold: 1000,
		},
		OperationThresholds: map[config.OperationType]OperationThreshold{
			config.OpRepackRefs: {
				RPCInterval:   10,
				StatThreshold: 500,
			},
		},
	}
}

// activity tracks housekeeping activity for a specific relative path.
type activity struct {
	writeCount int
	active     bool
	// writeCountAtLastRun tracks the write count at which each operation last ran.
	writeCountAtLastRun map[config.OperationType]int
}

func newActivity() *activity {
	return &activity{
		writeCountAtLastRun: make(map[config.OperationType]int),
	}
}

type repoKey struct {
	storage, relativePath string
}

// Middleware manages scheduling of housekeeping tasks by intercepting gRPC requests.
type Middleware struct {
	config       MiddlewareConfig
	repoActivity map[repoKey]*activity

	mu sync.Mutex
	wg sync.WaitGroup

	logger           log.Logger
	registry         *protoregistry.Registry
	manager          manager.Manager
	localRepoFactory localrepo.Factory
	statsCache       *sync.Map
}

// forceHousekeepingRPCs are all the RPCs that we should force housekeeping right after.
var forceHousekeepingRPCs = map[string]struct{}{
	gitalypb.CleanupService_RewriteHistory_FullMethodName: {},
}

// NewHousekeepingMiddleware returns a new middleware.
func NewHousekeepingMiddleware(logger log.Logger, registry *protoregistry.Registry, factory localrepo.Factory, manager manager.Manager, cfg MiddlewareConfig) *Middleware {
	return &Middleware{
		config:           cfg,
		logger:           logger,
		registry:         registry,
		localRepoFactory: factory,
		manager:          manager,
		repoActivity:     make(map[repoKey]*activity),
		statsCache:       &sync.Map{},
	}
}

// getThresholds returns the thresholds for a given operation type.
func (m *Middleware) getThresholds(op config.OperationType) OperationThreshold {
	if thresholds, ok := m.config.OperationThresholds[op]; ok {
		return thresholds
	}
	return m.config.DefaultThresholds
}

// WaitForWorkers waits for any active housekeeping tasks to finish.
func (m *Middleware) WaitForWorkers() {
	m.wg.Wait()
}

// UnaryServerInterceptor returns gRPC unary middleware.
func (m *Middleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
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

		key := m.getRepoKey(targetRepo)

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
			// No point in housekeeping if a mutator request was invalid, rate-limited, or unauthorised.
			// For the rest of the errors, we allow housekeeping execution.
			if err != nil {
				if st, ok := status.FromError(err); ok {
					if slices.Index([]codes.Code{
						codes.FailedPrecondition,
						codes.ResourceExhausted,
						codes.PermissionDenied,
					}, st.Code()) >= 0 {
						return resp, err
					}
				}
			}

			// No point in housekeeping a deleted repo.
			if info.FullMethod == gitalypb.RepositoryService_RemoveRepository_FullMethodName {
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

		key := m.getRepoKey(targetRepo)

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
		a = newActivity()
		m.repoActivity[key] = a
	}

	a.active = true
}

func (m *Middleware) markHousekeepingInactive(key repoKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.repoActivity[key]
	if !ok {
		a = newActivity()
		m.repoActivity[key] = a
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

// allOperations contains all known operation types.
var allOperations = []config.OperationType{
	config.OpRepackRefs, config.OpRepackObjects, config.OpPruneObjects, config.OpWriteCommitGraph,
}

// pendingOperations returns operations that have exceeded their RPC interval thresholds.
func (m *Middleware) pendingOperations(a *activity, force bool) []config.OperationType {
	var pending []config.OperationType

	for _, op := range allOperations {
		thresholds := m.getThresholds(op)
		lastRun := a.writeCountAtLastRun[op]
		writesSinceLastRun := a.writeCount - lastRun

		if force || writesSinceLastRun > thresholds.RPCInterval {
			pending = append(pending, op)
		}
	}

	return pending
}

// operationsExceedingStatThreshold returns operations whose stat threshold is exceeded.
func (m *Middleware) operationsExceedingStatThreshold(statCount int) []config.OperationType {
	var ops []config.OperationType

	for _, op := range allOperations {
		thresholds := m.getThresholds(op)
		if statCount > thresholds.StatThreshold {
			ops = append(ops, op)
		}
	}

	return ops
}

func (m *Middleware) scheduleHousekeeping(ctx context.Context, repo *gitalypb.Repository, force bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getRepoKey(repo)

	a, ok := m.repoActivity[key]
	if !ok {
		a = newActivity()
		m.repoActivity[key] = a
	}
	a.writeCount++

	if a.active {
		return
	}

	pendingOps := m.pendingOperations(a, force)
	if len(pendingOps) == 0 {
		return
	}

	m.logger.WithFields(log.Fields{
		"forced":     force,
		"operations": pendingOps,
	}).InfoContext(ctx, "beginning scheduled housekeeping")

	// Mark that these operations are running at the current write count
	for _, op := range pendingOps {
		a.writeCountAtLastRun[op] = a.writeCount
	}

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
				return housekeeping.NewSelectiveOptimizationStrategy(info, pendingOps)
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

		repoStats := snapshot.RepositoryStatistics{}
		if err := snapshot.WalkPathForStats(ctx, repositoryPath, &repoStats); err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "calculate repository statistics")
			return
		}

		m.logger.WithFields(log.Fields{
			"repository_stats": map[string]any{
				"directory_count": repoStats.DirectoryCount,
				"file_count":      repoStats.FileCount,
			},
		}).InfoContext(ctx, "collected repository statistics")

		m.statsCache.Store(key, struct{}{})
		totalCount := repoStats.DirectoryCount + repoStats.FileCount
		ops := m.operationsExceedingStatThreshold(totalCount)
		if len(ops) > 0 {
			m.scheduleHousekeeping(ctx, targetRepo, true)
		}
	}
}

func (m *Middleware) getRepoKey(repo *gitalypb.Repository) repoKey {
	return repoKey{storage: repo.GetStorageName(), relativePath: repo.GetRelativePath()}
}
