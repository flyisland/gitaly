package testserver

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v18/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/bundleuri"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/statushandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	praefectconfig "gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RunGitalyServer starts gitaly server based on the provided cfg and returns a connection address.
// It accepts addition Registrar to register all required service instead of
// calling service.RegisterAll explicitly because it creates a circular dependency
// when the function is used in on of internal/gitaly/service/... packages.
func RunGitalyServer(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) string {
	return StartGitalyServer(tb, cfg, registrar, opts...).Address()
}

// StartGitalyServer creates and runs gitaly (and praefect as a proxy) server.
func StartGitalyServer(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) GitalyServer {
	gitalySrv, gitalyAddr, disablePraefect := runGitaly(tb, cfg, registrar, opts...)

	if !testhelper.IsPraefectEnabled() || disablePraefect {
		return GitalyServer{
			Server:   gitalySrv,
			shutdown: gitalySrv.Stop,
			address:  gitalyAddr,
		}
	}

	praefectServer := runPraefectProxy(tb, cfg, gitalyAddr)
	return GitalyServer{
		Server: gitalySrv,
		shutdown: func() {
			praefectServer.Shutdown()
			gitalySrv.Stop()
		},
		address: praefectServer.Address(),
	}
}

func runPraefectProxy(tb testing.TB, gitalyCfg config.Cfg, gitalyAddr string) PraefectServer {
	var virtualStorages []*praefectconfig.VirtualStorage
	for _, storage := range gitalyCfg.Storages {
		virtualStorages = append(virtualStorages, &praefectconfig.VirtualStorage{
			Name: storage.Name,
			Nodes: []*praefectconfig.Node{
				{
					Storage: storage.Name,
					Address: gitalyAddr,
					Token:   gitalyCfg.Auth.Token,
				},
			},
		})
	}

	return StartPraefect(tb, praefectconfig.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(tb),
		Auth: auth.Config{
			Token: gitalyCfg.Auth.Token,
		},
		DB: testdb.GetConfig(tb, testdb.New(tb).Name),
		Failover: praefectconfig.Failover{
			Enabled:          true,
			ElectionStrategy: praefectconfig.ElectionStrategyLocal,
		},
		BackgroundVerification: praefectconfig.BackgroundVerification{
			// Some tests cases purposefully create bad metadata by deleting a repository off
			// the disk. If the background verifier is running, it could find these repositories
			// and remove the invalid metadata related to them. This can cause the test assertions
			// to fail. As the background verifier runs asynchronously and possibly changes state
			// during a test and issues unexpected RPCs, it's disabled generally for all tests.
			VerificationInterval: -1,
		},
		Replication: praefectconfig.DefaultReplicationConfig(),
		Logging: log.Config{
			Format: "json",
			Level:  "info",
		},
		VirtualStorages: virtualStorages,
	})
}

// GitalyServer is a helper that carries additional info and
// functionality about gitaly (+praefect) server.
type GitalyServer struct {
	Server   *grpc.Server
	shutdown func()
	address  string
}

// Shutdown terminates running gitaly (+praefect) server.
func (gs GitalyServer) Shutdown() {
	gs.shutdown()
}

// Address returns address of the running gitaly (or praefect) service.
func (gs GitalyServer) Address() string {
	return gs.address
}

// waitHealthy waits until the server hosted at address becomes healthy.
func waitHealthy(tb testing.TB, ctx context.Context, addr string, authToken string) {
	grpcOpts := []grpc.DialOption{
		client.UnaryInterceptor(),
		client.StreamInterceptor(),
	}
	if authToken != "" {
		grpcOpts = append(grpcOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken)))
	}

	conn, err := client.New(ctx, addr, client.WithGrpcOptions(grpcOpts))
	require.NoError(tb, err)
	defer testhelper.MustClose(tb, conn)

	healthClient := healthpb.NewHealthClient(conn)

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	require.NoError(tb, err)
	require.Equal(tb, healthpb.HealthCheckResponse_SERVING, resp.GetStatus(), "server not yet ready to serve")
}

func runGitaly(tb testing.TB, cfg config.Cfg, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) (*grpc.Server, string, bool) {
	tb.Helper()

	ctx := testhelper.Context(tb)

	var gsd gitalyServerDeps
	for _, opt := range opts {
		gsd = opt(gsd)
	}

	// We set up the structerr interceptors so that any error metadata that gets set via
	// `structerr.WithMetadata()` is not only logged, but also present in the error details.
	serverOpts := []server.Option{
		server.WithUnaryInterceptor(StructErrUnaryInterceptor),
		server.WithUnaryInterceptor(statushandler.AbortedErrorUnaryInterceptor),
		server.WithStreamInterceptor(StructErrStreamInterceptor),
		server.WithStreamInterceptor(statushandler.AbortedErrorStreamInterceptor),
	}

	deps := gsd.createDependencies(tb, ctx, cfg)
	tb.Cleanup(func() { testhelper.MustClose(tb, gsd.conns) })

	var txMiddleware server.TransactionMiddleware
	if deps.GetNode() != nil {
		unaryInterceptors := []grpc.UnaryServerInterceptor{
			storagemgr.NewUnaryInterceptor(
				deps.Logger, protoregistry.GitalyProtoPreregistered, deps.GetTransactionRegistry(), deps.GetNode(), deps.GetLocator(),
			),
		}
		streamInterceptors := []grpc.StreamServerInterceptor{
			storagemgr.NewStreamInterceptor(
				deps.Logger, protoregistry.GitalyProtoPreregistered, deps.GetTransactionRegistry(), deps.GetNode(), deps.GetLocator(),
			),
		}

		if gsd.transactionInterceptorsFn != nil {
			unary, stream := gsd.transactionInterceptorsFn(deps.GetLogger(), deps.GetNode(), deps.GetRepositoryFactory())
			unaryInterceptors = append(unaryInterceptors, unary...)
			streamInterceptors = append(streamInterceptors, stream...)
		}

		txMiddleware = server.TransactionMiddleware{
			UnaryInterceptors:  unaryInterceptors,
			StreamInterceptors: streamInterceptors,
		}
	}

	serverFactory := server.NewGitalyServerFactory(
		cfg,
		gsd.logger.WithField("test", tb.Name()),
		deps.GetBackchannelRegistry(),
		deps.GetDiskCache(),
		[]*limithandler.LimiterMiddleware{deps.GetLimitHandler()},
		nil,
		txMiddleware,
	)

	if cfg.RuntimeDir != "" {
		internalServer, err := serverFactory.CreateInternal(serverOpts...)
		require.NoError(tb, err)

		registrar(internalServer, deps)
		registerHealthServerIfNotRegistered(internalServer)

		require.NoError(tb, os.MkdirAll(cfg.InternalSocketDir(), mode.Directory))
		tb.Cleanup(func() { require.NoError(tb, os.RemoveAll(cfg.InternalSocketDir())) })

		internalListener, err := net.Listen("unix", cfg.InternalSocketPath())
		require.NoError(tb, err)
		go func() {
			assert.NoError(tb, internalServer.Serve(internalListener), "failure to serve internal gRPC")
		}()

		waitHealthy(tb, ctx, "unix://"+internalListener.Addr().String(), cfg.Auth.Token)
	}

	secure := cfg.TLS.CertPath != "" && cfg.TLS.KeyPath != ""

	externalServer, err := serverFactory.CreateExternal(secure, serverOpts...)
	require.NoError(tb, err)

	tb.Cleanup(serverFactory.GracefulStop)

	registrar(externalServer, deps)
	registerHealthServerIfNotRegistered(externalServer)

	var listener net.Listener
	var addr string
	switch {
	case cfg.TLSListenAddr != "":
		listener, err = net.Listen("tcp", cfg.TLSListenAddr)
		require.NoError(tb, err)
		_, port, err := net.SplitHostPort(listener.Addr().String())
		require.NoError(tb, err)
		addr = "tls://localhost:" + port
	case cfg.ListenAddr != "":
		listener, err = net.Listen("tcp", cfg.ListenAddr)
		require.NoError(tb, err)
		addr = "tcp://" + listener.Addr().String()
	default:
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(tb)
		listener, err = net.Listen("unix", serverSocketPath)
		require.NoError(tb, err)
		addr = "unix://" + serverSocketPath
	}

	go func() {
		assert.NoError(tb, externalServer.Serve(listener), "failure to serve external gRPC")
	}()

	waitHealthy(tb, ctx, addr, cfg.Auth.Token)

	return externalServer, addr, gsd.disablePraefect
}

func registerHealthServerIfNotRegistered(srv *grpc.Server) {
	if _, found := srv.GetServiceInfo()["grpc.health.v1.Health"]; !found {
		// we should register health service as it is used for the health checks
		// praefect service executes periodically (and on the bootstrap step)
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}
}

type gitalyServerDeps struct {
	disablePraefect           bool
	logger                    log.Logger
	conns                     *client.Pool
	locator                   storage.Locator
	txMgr                     transaction.Manager
	hookMgr                   hook.Manager
	gitlabClient              gitlab.Client
	gitCmdFactory             gitcmd.CommandFactory
	backchannelReg            *backchannel.Registry
	catfileCache              catfile.Cache
	diskCache                 cache.Cache
	packObjectsCache          streamcache.Cache
	packObjectsLimiter        limiter.Limiter
	limitHandler              *limithandler.LimiterMiddleware
	repositoryCounter         *counter.RepositoryCounter
	updaterWithHooks          *updateref.UpdaterWithHooks
	housekeepingManager       housekeepingmgr.Manager
	backupSink                *backup.Sink
	backupLocator             backup.Locator
	signingKey                string
	transactionRegistry       *storagemgr.TransactionRegistry
	procReceiveRegistry       *hook.ProcReceiveRegistry
	bundleURIManager          *bundleuri.GenerationManager
	bundleURISink             *bundleuri.Sink
	bundleURIStrategy         bundleuri.GenerationStrategy
	localRepoFactory          localrepo.Factory
	migrations                *[]migration.Migration
	archiveCache              streamcache.Cache
	MigrationStateManager     migration.StateManager
	transactionInterceptorsFn func(log.Logger, storage.Node, localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor)
}

func (gsd *gitalyServerDeps) createDependencies(tb testing.TB, ctx context.Context, cfg config.Cfg) *service.Dependencies {
	if gsd.logger == nil {
		gsd.logger = testhelper.NewLogger(tb, testhelper.WithLoggerName("gitaly"))
	}

	if gsd.conns == nil {
		gsd.conns = client.NewPool(client.WithDialOptions(client.UnaryInterceptor(), client.StreamInterceptor()))
	}

	if gsd.locator == nil {
		gsd.locator = config.NewLocator(cfg)
	}

	if gsd.gitlabClient == nil {
		gsd.gitlabClient = gitlab.NewMockClient(
			tb, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
		)
	}

	if gsd.backchannelReg == nil {
		gsd.backchannelReg = backchannel.NewRegistry()
	}

	if gsd.txMgr == nil {
		gsd.txMgr = transaction.NewManager(cfg, gsd.logger, gsd.backchannelReg)
	}

	if gsd.gitCmdFactory == nil {
		gsd.gitCmdFactory = gittest.NewCommandFactory(tb, cfg)
	}

	if gsd.transactionRegistry == nil {
		gsd.transactionRegistry = storagemgr.NewTransactionRegistry()
	}

	if gsd.procReceiveRegistry == nil {
		gsd.procReceiveRegistry = hook.NewProcReceiveRegistry()
	}

	if gsd.MigrationStateManager == nil {
		gsd.MigrationStateManager = migration.NewStateManager(gsd.migrations)
	}

	var node storage.Node
	if testhelper.IsWALEnabled() {
		dbMgr, err := databasemgr.NewDBManager(
			ctx,
			cfg.Storages,
			keyvalue.NewBadgerStore,
			helper.NewNullTickerFactory(),
			gsd.logger,
		)
		require.NoError(tb, err)
		tb.Cleanup(dbMgr.Close)

		var raftFactory raftmgr.RaftReplicaFactory
		var raftNode *raftmgr.Node
		if testhelper.IsRaftEnabled() && !testhelper.IsPraefectEnabled() {
			cfg.Raft = config.DefaultRaftConfig(uuid.New().String())
			// Speed up initial election overhead in the test setup
			cfg.Raft.ElectionTicks = 5
			cfg.Raft.RTTMilliseconds = 100
			cfg.Raft.SnapshotDir = testhelper.TempDir(tb)
			raftNode, err = raftmgr.NewNode(cfg, gsd.logger, dbMgr, gsd.conns)
			require.NoError(tb, err)
			raftFactory = raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)
		}

		partitionFactoryOptions := []partition.FactoryOption{
			partition.WithCmdFactory(gsd.gitCmdFactory),
			partition.WithRepoFactory(localrepo.NewFactory(gsd.logger, gsd.locator, gsd.gitCmdFactory, gsd.catfileCache)),
			partition.WithMetrics(partition.NewMetrics(housekeeping.NewMetrics(cfg.Prometheus))),
			partition.WithRaftConfig(cfg.Raft),
			partition.WithRaftFactory(raftFactory),
		}

		nodeMgr, err := nodeimpl.NewManager(
			cfg.Storages,
			storagemgr.NewFactory(
				gsd.logger,
				dbMgr,
				migration.NewFactory(
					partition.NewFactory(partitionFactoryOptions...),
					migration.NewMetrics(),
					gsd.migrations,
				),
				config.DefaultMaxInactivePartitions,
				storagemgr.NewMetrics(cfg.Prometheus),
			),
		)
		require.NoError(tb, err)
		tb.Cleanup(nodeMgr.Close)

		if testhelper.IsRaftEnabled() && !testhelper.IsPraefectEnabled() {
			for _, storageCfg := range cfg.Storages {
				baseStorage, err := nodeMgr.GetStorage(storageCfg.Name)
				require.NoError(tb, err)
				require.NoError(tb, raftNode.SetBaseStorage(storageCfg.Name, baseStorage))
			}
			node = raftNode
		} else {
			node = nodeMgr
		}
	}

	// This is to allow building a bundle generation from a Sink
	// without having to create one beforehand.
	// If bundleURIManager is defined though, we use that one.
	if gsd.bundleURIManager == nil && gsd.bundleURISink != nil {
		var strategy bundleuri.GenerationStrategy
		if gsd.bundleURIStrategy != nil {
			strategy = gsd.bundleURIStrategy
		} else {
			strategy = bundleuri.NewSimpleStrategy(true)
		}
		manager, err := bundleuri.NewGenerationManager(ctx, gsd.bundleURISink, gsd.logger, node, strategy)
		require.NoError(tb, err)

		gsd.bundleURIManager = manager
	}

	if gsd.hookMgr == nil {
		gsd.hookMgr = hook.NewManager(
			cfg, gsd.locator,
			gsd.logger,
			gsd.gitCmdFactory,
			gsd.txMgr,
			gsd.gitlabClient,
			hook.NewTransactionRegistry(gsd.transactionRegistry),
			gsd.procReceiveRegistry,
			node,
		)
	}

	if gsd.catfileCache == nil {
		cache := catfile.NewCache(cfg)
		gsd.catfileCache = cache
		tb.Cleanup(cache.Stop)
	}

	if gsd.diskCache == nil {
		gsd.diskCache = cache.New(cfg, gsd.locator, gsd.logger)
	}

	if gsd.packObjectsCache == nil {
		gsd.packObjectsCache = streamcache.New(cfg.PackObjectsCache, gsd.logger)
		tb.Cleanup(gsd.packObjectsCache.Stop)
	}

	if gsd.packObjectsLimiter == nil {
		gsd.packObjectsLimiter = limiter.NewConcurrencyLimiter(
			limiter.NewAdaptiveLimit("staticLimit", limiter.AdaptiveSetting{Initial: 0}),
			0,
			0,
			limiter.NewNoopConcurrencyMonitor(),
		)
	}

	if gsd.archiveCache == nil {
		gsd.archiveCache = streamcache.New(cfg.ArchiveCache, gsd.logger)
		tb.Cleanup(gsd.archiveCache.Stop)
	}

	if gsd.limitHandler == nil {
		_, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
		gsd.limitHandler = limithandler.New(cfg, limithandler.LimitConcurrencyByRepo, setupPerRPCConcurrencyLimiters)
	}

	if gsd.repositoryCounter == nil {
		gsd.repositoryCounter = counter.NewRepositoryCounter(cfg.Storages)
	}

	if gsd.updaterWithHooks == nil {
		gsd.updaterWithHooks = updateref.NewUpdaterWithHooks(cfg, gsd.logger, gsd.locator, gsd.hookMgr, gsd.gitCmdFactory, gsd.catfileCache)
	}

	if gsd.housekeepingManager == nil {
		gsd.housekeepingManager = housekeepingmgr.New(cfg.Prometheus, gsd.logger, gsd.txMgr, node)
	}

	if gsd.signingKey != "" {
		cfg.Git.SigningKey = gsd.signingKey
	}

	gsd.localRepoFactory = localrepo.NewFactory(gsd.logger, gsd.locator, gsd.gitCmdFactory, gsd.catfileCache)

	return &service.Dependencies{
		Logger:                 gsd.logger,
		Cfg:                    cfg,
		ClientPool:             gsd.conns,
		StorageLocator:         gsd.locator,
		TransactionManager:     gsd.txMgr,
		GitalyHookManager:      gsd.hookMgr,
		GitCmdFactory:          gsd.gitCmdFactory,
		BackchannelRegistry:    gsd.backchannelReg,
		GitlabClient:           gsd.gitlabClient,
		CatfileCache:           gsd.catfileCache,
		DiskCache:              gsd.diskCache,
		PackObjectsCache:       gsd.packObjectsCache,
		PackObjectsLimiter:     gsd.packObjectsLimiter,
		LimitHandler:           gsd.limitHandler,
		RepositoryCounter:      gsd.repositoryCounter,
		UpdaterWithHooks:       gsd.updaterWithHooks,
		HousekeepingManager:    gsd.housekeepingManager,
		TransactionRegistry:    gsd.transactionRegistry,
		Node:                   node,
		BackupSink:             gsd.backupSink,
		BackupLocator:          gsd.backupLocator,
		ProcReceiveRegistry:    gsd.procReceiveRegistry,
		BundleURIManager:       gsd.bundleURIManager,
		LocalRepositoryFactory: gsd.localRepoFactory,
		MigrationStateManager:  gsd.MigrationStateManager,
		ArchiveCache:           gsd.archiveCache,
	}
}

// GitalyServerOpt is a helper type to shorten declarations.
type GitalyServerOpt func(gitalyServerDeps) gitalyServerDeps

// WithLogger sets a log.Logger instance that will be used for gitaly services initialisation.
func WithLogger(logger log.Logger) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.logger = logger
		return deps
	}
}

// WithLocator sets a storage.Locator instance that will be used for gitaly services initialisation.
func WithLocator(locator storage.Locator) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.locator = locator
		return deps
	}
}

// WithGitCommandFactory sets a gitcmd.CommandFactory instance that will be used for gitaly services
// initialisation.
func WithGitCommandFactory(gitCmdFactory gitcmd.CommandFactory) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitCmdFactory = gitCmdFactory
		return deps
	}
}

// WithGitLabClient sets gitlab.Client instance that will be used for gitaly services initialisation.
func WithGitLabClient(gitlabClient gitlab.Client) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitlabClient = gitlabClient
		return deps
	}
}

// WithHookManager sets hook.Manager instance that will be used for gitaly services initialisation.
func WithHookManager(hookMgr hook.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.hookMgr = hookMgr
		return deps
	}
}

// WithTransactionManager sets transaction.Manager instance that will be used for gitaly services initialisation.
func WithTransactionManager(txMgr transaction.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.txMgr = txMgr
		return deps
	}
}

// WithDisablePraefect disables setup and usage of the praefect as a proxy before the gitaly service.
func WithDisablePraefect() GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.disablePraefect = true
		return deps
	}
}

// WithBackchannelRegistry sets backchannel.Registry instance that will be used for gitaly services initialisation.
func WithBackchannelRegistry(backchannelReg *backchannel.Registry) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backchannelReg = backchannelReg
		return deps
	}
}

// WithDiskCache sets the cache.Cache instance that will be used for gitaly services initialisation.
func WithDiskCache(diskCache cache.Cache) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.diskCache = diskCache
		return deps
	}
}

// WithRepositoryCounter sets the counter.RepositoryCounter instance that will be used for gitaly services initialisation.
func WithRepositoryCounter(repositoryCounter *counter.RepositoryCounter) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.repositoryCounter = repositoryCounter
		return deps
	}
}

// WithPackObjectsLimiter sets the PackObjectsLimiter that will be
// used for gitaly services initialization.
func WithPackObjectsLimiter(limiter *limiter.ConcurrencyLimiter) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.packObjectsLimiter = limiter
		return deps
	}
}

// WithPackObjectsCache sets the PackObjectsCache that will be
// used for gitaly services initialization.
func WithPackObjectsCache(cache streamcache.Cache) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.packObjectsCache = cache
		return deps
	}
}

// WithHousekeepingManager sets the housekeeping.Manager that will be used for Gitaly services
// initialization.
func WithHousekeepingManager(manager housekeepingmgr.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.housekeepingManager = manager
		return deps
	}
}

// WithBackupSink sets the backup.Sink that will be used for Gitaly services
func WithBackupSink(backupSink *backup.Sink) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backupSink = backupSink
		return deps
	}
}

// WithBackupLocator sets the backup.Locator that will be used for Gitaly services
func WithBackupLocator(backupLocator backup.Locator) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backupLocator = backupLocator
		return deps
	}
}

// WithBundleURIStrategy sets the bundleuri.GenerationStrategy that will be used by the bundleuri.GenerationManager
// The default value is bundleuri.NewSimpleStrategy(true)
func WithBundleURIStrategy(strategy bundleuri.GenerationStrategy) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.bundleURIStrategy = strategy
		return deps
	}
}

// WithBundleURISink sets the *bundleuri.Sink that will be used by the bundleuri.GenerationManager
func WithBundleURISink(sink *bundleuri.Sink) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.bundleURISink = sink
		return deps
	}
}

// WithBundleURIManager sets the *bundleuri.GenerationManager that will be used by the bundleuri.GenerationManager
func WithBundleURIManager(manager *bundleuri.GenerationManager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.bundleURIManager = manager
		return deps
	}
}

// WithSigningKey sets the signing key path that will be used for Gitaly
// services.
func WithSigningKey(signingKey string) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.signingKey = signingKey
		return deps
	}
}

// WithTransactionRegistry sets the transaction registry that will be used for Gitaly services.
func WithTransactionRegistry(registry *storagemgr.TransactionRegistry) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.transactionRegistry = registry
		return deps
	}
}

// WithProcReceiveRegistry sets the proc receive registry that will be used for Gitaly services.
func WithProcReceiveRegistry(registry *hook.ProcReceiveRegistry) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.procReceiveRegistry = registry
		return deps
	}
}

// WithRepositoryFactory sets the localrepo.Factory that will be used for gitaly services initialisation.
func WithRepositoryFactory(repoFactory localrepo.Factory) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.localRepoFactory = repoFactory
		return deps
	}
}

// WithTransactionInterceptors allows for setting additional transaction middlewares to the server via
// a callback function.
func WithTransactionInterceptors(
	fn func(log.Logger, storage.Node, localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor),
) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.transactionInterceptorsFn = fn
		return deps
	}
}

// WithMigrations registers migration tasks to be executed before any transaction begins.
func WithMigrations(migrations *[]migration.Migration) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.migrations = migrations
		return deps
	}
}
