package housekeeping

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type testService struct {
	gitalypb.UnimplementedRepositoryServiceServer
}

// Mutator, Unary
func (ts *testService) WriteRef(context.Context, *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	return &gitalypb.WriteRefResponse{}, nil
}

func (ts *testService) RemoveRepository(context.Context, *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	return &gitalypb.RemoveRepositoryResponse{}, nil
}

// Mutator, Unary, FailedPrecondition error
func (ts *testService) CreateRepository(context.Context, *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "designed to error with FailedPrecondition")
}

// Mutator, Unary, ResourceExhausted error
func (ts *testService) FetchRemote(context.Context, *gitalypb.FetchRemoteRequest) (*gitalypb.FetchRemoteResponse, error) {
	return nil, status.Error(codes.ResourceExhausted, "designed to error with ResourceExhausted")
}

func (ts *testService) HasLocalBranches(context.Context, *gitalypb.HasLocalBranchesRequest) (*gitalypb.HasLocalBranchesResponse, error) {
	return nil, status.Error(codes.PermissionDenied, "designed to error with PermissionDenied")
}

// Mutator, Unary, Erroring
func (ts *testService) CreateRepositoryFromBundle(grpc.ClientStreamingServer[gitalypb.CreateRepositoryFromBundleRequest, gitalypb.CreateRepositoryFromBundleResponse]) error {
	return fmt.Errorf("designed to error")
}

// Accessor, Unary
func (ts *testService) RepositoryExists(context.Context, *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	return &gitalypb.RepositoryExistsResponse{}, nil
}

// Accessor, Stream
func (ts *testService) GetArchive(*gitalypb.GetArchiveRequest, grpc.ServerStreamingServer[gitalypb.GetArchiveResponse]) error {
	return nil
}

// Accessor, Unary, Erroring
func (ts *testService) RepositoryInfo(context.Context, *gitalypb.RepositoryInfoRequest) (*gitalypb.RepositoryInfoResponse, error) {
	return &gitalypb.RepositoryInfoResponse{}, fmt.Errorf("designed to error")
}

// Maintenance, Unary
func (ts *testService) OptimizeRepository(context.Context, *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	return nil, nil
}

// Maintenance, Unary
func (ts *testService) PruneUnreachableObjects(context.Context, *gitalypb.PruneUnreachableObjectsRequest) (*gitalypb.PruneUnreachableObjectsResponse, error) {
	return nil, nil
}

type testCleanupService struct {
	gitalypb.UnimplementedCleanupServiceServer
}

// RewriteHistory is a stream RPC that forces housekeeping
func (ts *testCleanupService) RewriteHistory(stream grpc.ClientStreamingServer[gitalypb.RewriteHistoryRequest, gitalypb.RewriteHistoryResponse]) error {
	// Receive the first request to get the repository
	_, err := stream.Recv()
	if err != nil {
		return err
	}

	// Return success response
	return stream.SendAndClose(&gitalypb.RewriteHistoryResponse{})
}

type healthServer struct {
	healthpb.UnimplementedHealthServer
}

func (*healthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{}, nil
}

type mockHousekeepingManager struct {
	// optimizeRepositoryInvocations allows us to track how many times OptimizeRepository was called for a given repository
	optimizeRepositoryInvocations map[string]int
	// lastEnabledOps allows us to get the last set of operations that were scheduled to run when OptimizeRepository was called
	lastEnabledOps map[string]map[config.OperationType]bool
	mu             sync.Mutex

	useDelayCh bool
	delayCh    chan struct{}
}

func (m *mockHousekeepingManager) getOptimizeRepositoryInvocations(relativePath string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.optimizeRepositoryInvocations[relativePath]
}

func (m *mockHousekeepingManager) getLastEnabledOps(relativePath string) map[config.OperationType]bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.lastEnabledOps[relativePath]
}

func (m *mockHousekeepingManager) withDelay() chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.delayCh = make(chan struct{})
	m.useDelayCh = true

	return m.delayCh
}

func (m *mockHousekeepingManager) withoutDelay() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.useDelayCh = false
}

func (m *mockHousekeepingManager) CleanStaleData(context.Context, *localrepo.Repo, housekeeping.CleanStaleDataConfig) error {
	return nil
}

func (m *mockHousekeepingManager) OptimizeRepository(ctx context.Context, repo *localrepo.Repo, opts ...housekeepingmgr.OptimizeRepositoryOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	relativePath := repo.GetRelativePath()
	if _, ok := m.optimizeRepositoryInvocations[relativePath]; !ok {
		m.optimizeRepositoryInvocations[relativePath] = 0
	}

	m.optimizeRepositoryInvocations[relativePath]++

	// Extract enabled operations from the strategy constructor
	var cfg housekeepingmgr.OptimizeRepositoryConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.StrategyConstructor != nil {
		strategy := cfg.StrategyConstructor(stats.RepositoryInfo{})
		if selective, ok := strategy.(*housekeeping.SelectiveOptimizationStrategy); ok {
			if m.lastEnabledOps == nil {
				m.lastEnabledOps = make(map[string]map[config.OperationType]bool)
			}
			m.lastEnabledOps[relativePath] = selective.EnabledOps()
		}
	}

	if m.useDelayCh {
		<-m.delayCh
	}

	return nil
}

func (m *mockHousekeepingManager) OffloadRepository(context.Context, *localrepo.Repo, config.OffloadingConfig) error {
	return nil
}

func (m *mockHousekeepingManager) RehydrateRepository(ctx context.Context, repo *localrepo.Repo, s string) error {
	return nil
}

// testMiddlewareConfig returns a MiddlewareConfig for testing with all operations
// using the same interval and threshold for simpler test assertions.
func testMiddlewareConfig(interval, statThreshold int) MiddlewareConfig {
	thresholds := OperationThreshold{
		RPCInterval:   interval,
		StatThreshold: statThreshold,
	}
	return MiddlewareConfig{
		DefaultThresholds: thresholds,
		OperationThresholds: map[config.OperationType]OperationThreshold{
			config.OpRepackRefs:       thresholds,
			config.OpRepackObjects:    thresholds,
			config.OpPruneObjects:     thresholds,
			config.OpWriteCommitGraph: thresholds,
		},
	}
}

func TestInterceptors(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, gitalycfg.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), catfileCache)

	housekeepingManager := &mockHousekeepingManager{
		optimizeRepositoryInvocations: make(map[string]int),
		delayCh:                       make(chan struct{}),
	}

	middlewareConfig := testMiddlewareConfig(1, 1000)

	housekeepingMiddleware := NewHousekeepingMiddleware(logger, protoregistry.GitalyProtoPreregistered, localRepoFactory, housekeepingManager, middlewareConfig)
	defer housekeepingMiddleware.WaitForWorkers()

	server := grpc.NewServer(
		grpc.StreamInterceptor(housekeepingMiddleware.StreamServerInterceptor()),
		grpc.UnaryInterceptor(housekeepingMiddleware.UnaryServerInterceptor()),
	)
	t.Cleanup(server.Stop)

	service := &testService{}
	cleanupService := &testCleanupService{}

	gitalypb.RegisterRepositoryServiceServer(server, service)
	gitalypb.RegisterCleanupServiceServer(server, cleanupService)
	healthpb.RegisterHealthServer(server, &healthServer{})

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	go func() {
		testhelper.MustServe(t, server, listener)
	}()

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	t.Run("when the RemoveRepository RPC is invoked", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo1",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		for range 2 {
			sendFn()
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "another invocation after the interval")
	})

	t.Run("when unary mutator RPCs are intercepted", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo1",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations under the interval")

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 1, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "one invocation after the interval")

		for range 2 {
			sendFn()
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 2, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "another invocation after the interval")
	})

	t.Run("when unary accessor RPCs are intercepted", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo2",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations under the interval")

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations after the interval")
	})

	t.Run("when stream accessor RPCs are intercepted", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo3",
		}

		sendFn := func() {
			stream, err := gitalypb.NewRepositoryServiceClient(conn).GetArchive(ctx, &gitalypb.GetArchiveRequest{
				Repository: repo,
			})
			require.NoError(t, err)
			require.NoError(t, stream.CloseSend())
		}

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations under the interval")

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations after the interval")
	})

	t.Run("when an erroring RPC is intercepted", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo4",
		}

		for range 2 {
			_, err = gitalypb.NewRepositoryServiceClient(conn).RepositoryInfo(ctx, &gitalypb.RepositoryInfoRequest{
				Repository: repo,
			})
			require.EqualError(t, err, "rpc error: code = Unknown desc = designed to error", "middleware preserves the original error")
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations after the interval")

		for range 2 {
			stream, err := gitalypb.NewRepositoryServiceClient(conn).CreateRepositoryFromBundle(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
				Repository: repo,
			}))

			_, err = stream.CloseAndRecv()
			require.EqualError(t, err, "rpc error: code = Unknown desc = designed to error", "middleware preserves the original error")
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations after the interval")
	})

	t.Run("when a unary mutator returns a FailedPrecondition error", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo-failedprecondition",
		}

		for range 2 {
			_, err := gitalypb.NewRepositoryServiceClient(conn).CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
				Repository: repo,
			})
			require.EqualError(t, err, "rpc error: code = FailedPrecondition desc = designed to error with FailedPrecondition",
				"middleware preserves the original error")
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"FailedPrecondition errors should skip housekeeping")
	})

	t.Run("when a unary mutator returns a ResourceExhausted error", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrep-ResourceExhausted",
		}

		sendFn := func() {
			_, err := gitalypb.NewRepositoryServiceClient(conn).FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
				Repository: repo,
			})
			require.EqualError(t, err, "rpc error: code = ResourceExhausted desc = designed to error with ResourceExhausted",
				"middleware preserves the original error")
		}

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"no invocations under the interval")

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"ResourceExhausted errors should skip housekeeping")
	})

	t.Run("when a unary mutator returns a PermissionDenied error", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrep-PermissionDenied",
		}

		sendFn := func() {
			_, err := gitalypb.NewRepositoryServiceClient(conn).HasLocalBranches(ctx, &gitalypb.HasLocalBranchesRequest{
				Repository: repo,
			})
			require.EqualError(t, err, "rpc error: code = PermissionDenied desc = designed to error with PermissionDenied",
				"middleware preserves the original error")
		}

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"no invocations under the interval")

		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"PermissionDenied errors should skip housekeeping")
	})

	t.Run("when the OptimizeRepository RPC is invoked", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo5",
		}

		for range 2 {
			_, err = gitalypb.NewRepositoryServiceClient(conn).OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "does not schedule further housekeeping")
	})

	t.Run("when the PruneUnreachableObjects RPC is invoked", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo6",
		}

		for range 2 {
			_, err = gitalypb.NewRepositoryServiceClient(conn).PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "does not schedule further housekeeping")
	})

	t.Run("when a housekeeping task is active when a maintenance RPC is received", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo7",
		}

		ch := housekeepingManager.withDelay()
		defer housekeepingManager.withoutDelay()

		for range 2 {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		_, err = gitalypb.NewRepositoryServiceClient(conn).OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{
			Repository: repo,
		})

		require.EqualError(t, err, "rpc error: code = AlreadyExists desc = housekeeping already executing for repository")

		close(ch)
	})

	t.Run("when a maintenance RPC is active and the write interval is reached", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo8",
		}

		ch := housekeepingManager.withDelay()
		defer housekeepingManager.withoutDelay()

		_, err = gitalypb.NewRepositoryServiceClient(conn).OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{
			Repository: repo,
		})

		for range 2 {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		close(ch)

		require.Equal(t, 1, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations under the interval")
	})

	t.Run("when the write interval is reached again when housekeeping is active", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo9",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		ch := housekeepingManager.withDelay()
		defer housekeepingManager.withoutDelay()

		// The first two requests will trigger housekeeping that runs until ch is closed.
		// The next two requests won't trigger housekeeping as there's already an active job.
		for range 4 {
			sendFn()
		}

		// Release the active housekeeping job.
		close(ch)

		// The next request triggers housekeeping as the counter has already incremented past the interval.
		sendFn()

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 2, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "another invocation after the interval")
	})

	t.Run("when an RPC not registered with protoregistry.GitalyProtoPreregistered is intercepted", func(t *testing.T) {
		hook := testhelper.AddLoggerHook(logger)

		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)

		require.Empty(t, hook.LastEntry(), "it does not log an error")
	})

	t.Run("when housekeeping is forced", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "myrepo-force",
		}

		// Test RewriteHistory RPC which should force housekeeping immediately
		stream, err := gitalypb.NewCleanupServiceClient(conn).RewriteHistory(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.RewriteHistoryRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		require.NoError(t, err)

		// Wait for any async housekeeping to complete
		housekeepingMiddleware.WaitForWorkers()

		// Verify that housekeeping was triggered immediately (forced) even on the first call
		require.Equal(t, 1,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"RewriteHistory should force immediate housekeeping")
	})

	t.Run("when forceHousekeepingRPCs bypass interval compared to regular mutators", func(t *testing.T) {
		forceRepo := &gitalypb.Repository{
			RelativePath: "myrepo-force-bypass",
		}
		regularRepo := &gitalypb.Repository{
			RelativePath: "myrepo-regular-interval",
		}

		// Test that forceHousekeepingRPCs bypass the normal interval constraint
		// First RewriteHistory call should immediately trigger housekeeping (force=true)
		stream, err := gitalypb.NewCleanupServiceClient(conn).RewriteHistory(ctx)
		require.NoError(t, err)

		err = stream.Send(&gitalypb.RewriteHistoryRequest{
			Repository: forceRepo,
			Redactions: [][]byte{[]byte("test-pattern")},
		})
		require.NoError(t, err)

		_, err = stream.CloseAndRecv()
		require.NoError(t, err)

		housekeepingMiddleware.WaitForWorkers()

		// Should trigger housekeeping immediately despite being the first call
		require.Equal(t, 1,
			housekeepingManager.getOptimizeRepositoryInvocations(forceRepo.GetRelativePath()),
			"First RewriteHistory call should force housekeeping immediately")

		// Compare with regular mutator RPCs that respect the interval
		// Single regular mutator call should not trigger housekeeping
		_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: regularRepo,
		})
		require.NoError(t, err)

		housekeepingMiddleware.WaitForWorkers()

		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(regularRepo.GetRelativePath()),
			"Single regular mutator should not trigger housekeeping (respects interval)")

		// The second regular mutator call should trigger housekeeping (interval=1)
		_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: regularRepo,
		})
		require.NoError(t, err)

		housekeepingMiddleware.WaitForWorkers()

		require.Equal(t, 1,
			housekeepingManager.getOptimizeRepositoryInvocations(regularRepo.GetRelativePath()),
			"Second regular mutator should trigger housekeeping after reaching interval")
	})

	t.Run("when snapshot stats indicate higher directory entry count than the threshold", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		// Create a new middleware with low threshold to easily pass it
		lowThresholdConfig := testMiddlewareConfig(1, 1)
		lowThresholdMiddleware := NewHousekeepingMiddleware(logger, protoregistry.GitalyProtoPreregistered, localRepoFactory, housekeepingManager, lowThresholdConfig)
		defer lowThresholdMiddleware.WaitForWorkers()

		lowThresholdServer := grpc.NewServer(
			grpc.StreamInterceptor(lowThresholdMiddleware.StreamServerInterceptor()),
			grpc.UnaryInterceptor(lowThresholdMiddleware.UnaryServerInterceptor()),
		)
		t.Cleanup(lowThresholdServer.Stop)

		gitalypb.RegisterRepositoryServiceServer(lowThresholdServer, service)

		lowThresholdListener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		go func() {
			testhelper.MustServe(t, lowThresholdServer, lowThresholdListener)
		}()

		lowThresholdConn, err := grpc.NewClient(
			lowThresholdListener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer testhelper.MustClose(t, lowThresholdConn)

		initialCount := housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath())

		_, err = gitalypb.NewRepositoryServiceClient(lowThresholdConn).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		lowThresholdMiddleware.WaitForWorkers()

		newCount := housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath())

		// Verify that housekeeping was triggered
		require.Equal(t,
			initialCount+1,
			newCount,
			"snapshot stats should force immediate housekeeping",
		)

		// Next request should not trigger housekeeping as it is added to the stats cache
		_, err = gitalypb.NewRepositoryServiceClient(lowThresholdConn).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		lowThresholdMiddleware.WaitForWorkers()
		require.Equal(t,
			newCount,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"snapshot stats should force immediate housekeeping",
		)
	})

	t.Run("when snapshot stats indicate lower directory entry count than the threshold", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		initialCount := housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath())

		_, err = gitalypb.NewRepositoryServiceClient(conn).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// Wait for any async housekeeping to complete
		housekeepingMiddleware.WaitForWorkers()

		// Verify that housekeeping was not triggered.
		require.Equal(t,
			initialCount,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"snapshot stats should not force immediate housekeeping",
		)
	})
}

func TestIndependentOperationThresholds(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, gitalycfg.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), catfileCache)

	housekeepingManager := &mockHousekeepingManager{
		optimizeRepositoryInvocations: make(map[string]int),
		delayCh:                       make(chan struct{}),
	}

	// Configure different thresholds for different operations
	middlewareConfig := MiddlewareConfig{
		DefaultThresholds: OperationThreshold{
			RPCInterval:   20,
			StatThreshold: 1000,
		},
		OperationThresholds: map[config.OperationType]OperationThreshold{
			config.OpRepackRefs: {
				RPCInterval:   2,
				StatThreshold: 500,
			},
			config.OpRepackObjects: {
				RPCInterval:   5,
				StatThreshold: 1000,
			},
		},
	}

	housekeepingMiddleware := NewHousekeepingMiddleware(logger, protoregistry.GitalyProtoPreregistered, localRepoFactory, housekeepingManager, middlewareConfig)
	defer housekeepingMiddleware.WaitForWorkers()

	server := grpc.NewServer(
		grpc.StreamInterceptor(housekeepingMiddleware.StreamServerInterceptor()),
		grpc.UnaryInterceptor(housekeepingMiddleware.UnaryServerInterceptor()),
	)
	t.Cleanup(server.Stop)

	service := &testService{}
	gitalypb.RegisterRepositoryServiceServer(server, service)

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	go func() {
		testhelper.MustServe(t, server, listener)
	}()

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	t.Run("pack-refs triggers before repack due to lower threshold", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "independent-thresholds-repo",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		sendFn()
		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0, housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"no housekeeping after 1 RPC")

		sendFn()
		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 0,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"no housekeeping AT threshold")

		sendFn()
		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 1,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"housekeeping triggered after pack-refs passing threshold")

		sendFn()
		sendFn()
		sendFn()
		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, 2,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"housekeeping triggered again after pack-refs threshold")
	})

	t.Run("repack-refs and repack-objects run at independent intervals", func(t *testing.T) {
		repo := &gitalypb.Repository{
			RelativePath: "independent-intervals-repo",
		}

		sendFn := func() {
			_, err = gitalypb.NewRepositoryServiceClient(conn).WriteRef(ctx, &gitalypb.WriteRefRequest{
				Repository: repo,
			})
			require.NoError(t, err)
		}

		// With RPCInterval: 2 for pack-refs and RPCInterval: 5 for repack-objects:
		// pack-refs triggers when writeCount > 2 (after 3 RPCs)
		// repack-objects triggers when writeCount > 5 (after 6 RPCs)

		// Send 3 RPCs - pack-refs threshold exceeded (3 > 2), repack-objects not (3 < 5)
		for i := 0; i < 3; i++ {
			sendFn()
		}
		housekeepingMiddleware.WaitForWorkers()

		require.Equal(t,
			1,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"housekeeping should trigger after pack-refs threshold (3 RPCs)")

		// Verify pack-refs was requested but NOT repack-objects
		ops := housekeepingManager.getLastEnabledOps(repo.GetRelativePath())
		require.True(t, ops[config.OpRepackRefs],
			"pack-refs should be enabled after 3 RPCs")
		require.False(t, ops[config.OpRepackObjects],
			"repack-objects should NOT be enabled after only 3 RPCs")

		// Send 3 more RPCs
		// pack-refs: 3 > 2, triggers again
		// repack-objects: 6 > 5, triggers now
		for i := 0; i < 3; i++ {
			sendFn()
		}
		housekeepingMiddleware.WaitForWorkers()

		require.Equal(t,
			2,
			housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()),
			"housekeeping should trigger again after 6 total RPCs")

		ops = housekeepingManager.getLastEnabledOps(repo.GetRelativePath())
		require.True(t, ops[config.OpRepackRefs],
			"pack-refs should be enabled")
		require.True(t, ops[config.OpRepackObjects],
			"repack-objects should now be enabled after 6 RPCs")
	})
}
