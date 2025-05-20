package housekeeping

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testService struct {
	gitalypb.UnimplementedRepositoryServiceServer
}

// Mutator, Unary
func (ts *testService) WriteRef(context.Context, *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	return &gitalypb.WriteRefResponse{}, nil
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

type mockHousekeepingManager struct {
	optimizeRepositoryInvocations map[string]int // RelativePath -> count
	mu                            sync.Mutex

	useDelayCh bool
	delayCh    chan struct{}
}

func (m *mockHousekeepingManager) getOptimizeRepositoryInvocations(relativePath string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.optimizeRepositoryInvocations[relativePath]
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

	if m.useDelayCh {
		<-m.delayCh
	}

	return nil
}

func (m *mockHousekeepingManager) AddPackRefsInhibitor(ctx context.Context, repo storage.Repository) (bool, func(), error) {
	return false, nil, nil
}

func (m *mockHousekeepingManager) OffloadRepository(context.Context, *localrepo.Repo, config.OffloadingConfig) error {
	return nil
}

func TestInterceptors(t *testing.T) {
	testhelper.NewFeatureSets(
		featureflag.HousekeepingMiddleware,
	).Run(t, testInterceptors)
}

func testInterceptors(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	localRepoFactory := localrepo.NewFactory(logger, gitalycfg.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), catfileCache)

	housekeepingManager := &mockHousekeepingManager{
		optimizeRepositoryInvocations: make(map[string]int),
		delayCh:                       make(chan struct{}),
	}

	housekeepingMiddleware := NewHousekeepingMiddleware(logger, protoregistry.GitalyProtoPreregistered, localRepoFactory, housekeepingManager, 1)
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
		require.Equal(t, testhelper.EnabledOrDisabledFlag(ctx, featureflag.HousekeepingMiddleware, 1, 0), housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "one invocation after the interval")

		for range 2 {
			sendFn()
		}

		housekeepingMiddleware.WaitForWorkers()
		require.Equal(t, testhelper.EnabledOrDisabledFlag(ctx, featureflag.HousekeepingMiddleware, 2, 0), housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "another invocation after the interval")
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

		if featureflag.HousekeepingMiddleware.IsEnabled(ctx) {
			require.EqualError(t, err, "rpc error: code = AlreadyExists desc = housekeeping already executing for repository")
		} else {
			require.NoError(t, err)
		}

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

		require.Equal(t, testhelper.EnabledOrDisabledFlag(ctx, featureflag.HousekeepingMiddleware, 1, 0), housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "no invocations under the interval")
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
		require.Equal(t, testhelper.EnabledOrDisabledFlag(ctx, featureflag.HousekeepingMiddleware, 2, 0), housekeepingManager.getOptimizeRepositoryInvocations(repo.GetRelativePath()), "another invocation after the interval")
	})
}
