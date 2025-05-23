package gitaly

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/bundleuri"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingmgr "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/manager"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/maintenance"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mdfile"
	nodeimpl "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/node"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration/reftable"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	housekeepingmw "gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter/watchers"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/offloading"
	"gitlab.com/gitlab-org/gitaly/v16/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
	"gitlab.com/gitlab-org/labkit/fips"
	"gitlab.com/gitlab-org/labkit/monitoring"
	labkittracing "gitlab.com/gitlab-org/labkit/tracing"
	"go.uber.org/automaxprocs/maxprocs"
	"gocloud.dev/blob"
	"google.golang.org/grpc"

	// Import to register the proxy codec with gRPC.
	_ "gitlab.com/gitlab-org/gitaly/v16/internal/grpc/proxy"
)

func newServeCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "launch the server daemon",
		UsageText: `gitaly serve <gitaly_config_file>

Example: gitaly serve gitaly.config.toml`,
		Description: "Launch the Gitaly server daemon.",
		Action:      serveAction,
	}
}

func loadConfig(configPath string) (config.Cfg, error) {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return config.Cfg{}, err
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return config.Cfg{}, err
	}

	if err := cfg.Validate(); err != nil {
		return config.Cfg{}, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func serveAction(ctx context.Context, cmd *cli.Command) error {
	if cmd.NArg() != 1 || cmd.Args().First() == "" {
		cli.ShowSubcommandHelpAndExit(cmd, 2)
	}

	cfg, logger, err := configure(cmd.Args().First())
	if err != nil {
		return cli.Exit(err, 1)
	}

	if cfg.Auth.Transitioning && len(cfg.Auth.Token) > 0 {
		logger.Warn("Authentication is enabled but not enforced because transitioning=true. Gitaly will accept unauthenticated requests.")
	}

	logger.WithField("version", version.GetVersion()).Info("Starting Gitaly")
	fips.Check()

	if err := run(cmd, cfg, logger); err != nil {
		return cli.Exit(fmt.Errorf("unclean Gitaly shutdown: %w", err), 1)
	}

	logger.Info("Gitaly shutdown")

	return nil
}

func configure(configPath string) (config.Cfg, log.Logger, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return config.Cfg{}, nil, fmt.Errorf("load config: config_path %q: %w", configPath, err)
	}

	urlSanitizer := log.NewURLSanitizerHook()
	urlSanitizer.AddPossibleGrpcMethod(
		"CreateRepositoryFromURL",
		"FetchRemote",
		"UpdateRemoteMirror",
	)

	logger, err := log.Configure(log.NewSyncWriter(os.Stdout), cfg.Logging.Format, cfg.Logging.Level, urlSanitizer)
	if err != nil {
		return config.Cfg{}, nil, fmt.Errorf("configuring logger failed: %w", err)
	}

	if err := cfg.ValidateV2(); err != nil {
		logger.Warn(
			fmt.Sprintf(
				"The current configurations will cause Gitaly to fail to start up in future versions. Please run 'gitaly configuration validate < %s' and fix the errors that are printed.",
				configPath,
			),
		)
	}

	if undo, err := maxprocs.Set(maxprocs.Logger(func(s string, i ...interface{}) {
		logger.Info(fmt.Sprintf(s, i...))
	})); err != nil {
		logger.WithError(err).Error("failed to set GOMAXPROCS")
		undo()
	}

	sentry.ConfigureSentry(logger, version.GetVersion(), sentry.Config(cfg.Logging.Sentry))
	cfg.Prometheus.Configure(logger)
	labkittracing.Initialize(labkittracing.WithServiceName("gitaly"))
	preloadLicenseDatabase(logger)

	return cfg, logger, nil
}

func preloadLicenseDatabase(logger log.Logger) {
	go func() {
		// the first call to `licensedb.Detect` could be too long
		// https://github.com/go-enry/go-license-detector/issues/13
		// this is why we're calling it here to preload license database
		// on server startup to avoid long initialization on gRPC
		// method handling.
		began := time.Now()
		licensedb.Preload()
		logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("License database preloaded")
	}()
}

func run(appCtx *cli.Command, cfg config.Cfg, logger log.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	beganRun := time.Now()

	bootstrapSpan, ctx := tracing.StartSpan(ctx, "gitaly-bootstrap", nil)
	defer bootstrapSpan.Finish()

	if cfg.RuntimeDir != "" {
		if err := config.PruneOldGitalyProcessDirectories(logger, cfg.RuntimeDir); err != nil {
			return fmt.Errorf("prune runtime directories: %w", err)
		}
	}

	var err error
	cfg, err = config.SetupRuntimeDirectory(cfg, os.Getpid())
	if err != nil {
		return fmt.Errorf("setup runtime directory: %w", err)
	}

	cgroupMgr := cgroups.NewManager(cfg.Cgroups, logger, os.Getpid())

	began := time.Now()
	if err := cgroupMgr.Setup(); err != nil {
		return fmt.Errorf("failed setting up cgroups: %w", err)
	}
	logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("finished initializing cgroups")

	defer func() {
		if err := os.RemoveAll(cfg.RuntimeDir); err != nil {
			logger.Warn("could not clean up runtime dir")
		}
	}()

	began = time.Now()
	if err := gitaly.UnpackAuxiliaryBinaries(cfg.RuntimeDir, func(string) bool {
		return true
	}); err != nil {
		return fmt.Errorf("unpack auxiliary binaries: %w", err)
	}
	logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("finished unpacking auxiliary binaries")

	began = time.Now()
	b, err := bootstrap.New(logger, promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_connections_total",
			Help: "Total number of connections to Gitaly",
		},
		[]string{"type"},
	))
	if err != nil {
		return fmt.Errorf("init bootstrap: %w", err)
	}
	logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("finished initializing bootstrap")

	began = time.Now()
	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger, gitcmd.WithCgroupsManager(cgroupMgr))
	if err != nil {
		return fmt.Errorf("creating Git command factory: %w", err)
	}
	defer cleanup()
	logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("finished initializing command factory")

	logger.WithField("binary_path", gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath).Info("using Git binary")

	began = time.Now()
	gitVersion, err := gitCmdFactory.GitVersion(ctx)
	if err != nil {
		return fmt.Errorf("git version detection: %w", err)
	}
	logger.WithField("duration_ms", time.Since(began).Milliseconds()).Info("finished detecting git version")

	if !gitVersion.IsSupported() {
		return fmt.Errorf("unsupported Git version: %q", gitVersion)
	}

	logger.WithField("version", gitVersion.String()).Info("using Git version")

	registry := backchannel.NewRegistry()
	transactionManager := transaction.NewManager(cfg, logger, registry)
	prometheus.MustRegister(transactionManager)

	locator := config.NewLocator(cfg)

	repoCounter := counter.NewRepositoryCounter(cfg.Storages)
	prometheus.MustRegister(repoCounter)

	prometheus.MustRegister(gitCmdFactory)

	txRegistry := storagemgr.NewTransactionRegistry()

	conns := client.NewPool(
		client.WithDialer(client.HealthCheckDialer(
			func(ctx context.Context, address string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
				return client.New(ctx, address, client.WithGrpcOptions(opts))
			},
		)),
		client.WithDialOptions(
			client.UnaryInterceptor(),
			client.StreamInterceptor(),
		),
	)
	defer func() {
		_ = conns.Close()
	}()

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()
	prometheus.MustRegister(catfileCache)

	localrepoFactory := localrepo.NewFactory(logger, locator, gitCmdFactory, catfileCache)

	diskCache := cache.New(cfg, locator, logger)
	prometheus.MustRegister(diskCache)
	if err := diskCache.StartWalkers(); err != nil {
		return fmt.Errorf("disk cache walkers: %w", err)
	}

	// List of tracking adaptive limits. They will be calibrated by the adaptive calculator
	adaptiveLimits := []limiter.AdaptiveLimiter{}

	perRPCLimits, setupPerRPCConcurrencyLimiters := limithandler.WithConcurrencyLimiters(cfg)
	for _, concurrency := range cfg.Concurrency {
		// Connect adaptive limits to the adaptive calculator
		if concurrency.Adaptive {
			adaptiveLimits = append(adaptiveLimits, perRPCLimits[concurrency.RPC])
		}
	}
	perRPCLimitHandler := limithandler.New(
		cfg,
		limithandler.LimitConcurrencyByRepo,
		setupPerRPCConcurrencyLimiters,
	)
	prometheus.MustRegister(perRPCLimitHandler)

	var packObjectLimit *limiter.AdaptiveLimit
	if cfg.PackObjectsLimiting.Adaptive {
		packObjectLimit = limiter.NewAdaptiveLimit("packObjects", limiter.AdaptiveSetting{
			Initial:       cfg.PackObjectsLimiting.InitialLimit,
			Max:           cfg.PackObjectsLimiting.MaxLimit,
			Min:           cfg.PackObjectsLimiting.MinLimit,
			BackoffFactor: limiter.DefaultBackoffFactor,
		})
		adaptiveLimits = append(adaptiveLimits, packObjectLimit)
	} else {
		packObjectLimit = limiter.NewAdaptiveLimit("packObjects", limiter.AdaptiveSetting{
			Initial: cfg.PackObjectsLimiting.MaxConcurrency,
		})
	}

	packObjectsMonitor := limiter.NewPackObjectsConcurrencyMonitor(
		cfg.Prometheus.GRPCLatencyBuckets,
	)
	packObjectsLimiter := limiter.NewConcurrencyLimiter(
		packObjectLimit,
		cfg.PackObjectsLimiting.MaxQueueLength,
		cfg.PackObjectsLimiting.MaxQueueWait.Duration(),
		packObjectsMonitor,
	)
	prometheus.MustRegister(packObjectsMonitor)

	// Enable the adaptive calculator only if there is any limit needed to be adaptive.
	if len(adaptiveLimits) > 0 {
		adaptiveCalculator := limiter.NewAdaptiveCalculator(
			limiter.DefaultCalibrateFrequency,
			logger,
			adaptiveLimits,
			[]limiter.ResourceWatcher{
				watchers.NewCgroupCPUWatcher(cgroupMgr, cfg.AdaptiveLimiting.CPUThrottledThreshold),
				watchers.NewCgroupMemoryWatcher(cgroupMgr, cfg.AdaptiveLimiting.MemoryThreshold),
			},
		)
		prometheus.MustRegister(adaptiveCalculator)

		stop, err := adaptiveCalculator.Start(ctx)
		if err != nil {
			logger.WithError(err).Warn("error starting adaptive limiter calculator")
		}
		defer stop()
	}

	storageMetrics := storagemgr.NewMetrics(cfg.Prometheus)
	housekeepingMetrics := housekeeping.NewMetrics(cfg.Prometheus)
	raftMetrics := raftmgr.NewMetrics()
	partitionMetrics := partition.NewMetrics(housekeepingMetrics)
	migrationMetrics := migration.NewMetrics()
	reftableMigratorMetrics := reftable.NewMetrics()
	prometheus.MustRegister(housekeepingMetrics, storageMetrics, partitionMetrics, migrationMetrics, raftMetrics, reftableMigratorMetrics)

	migrations := []migration.Migration{}

	var txMiddleware server.TransactionMiddleware
	var node storage.Node
	if cfg.Transactions.Enabled {
		logger.WarnContext(ctx, "Transactions enabled. Transactions are an experimental feature. The feature is not production ready yet and might lead to various issues including data loss.")

		dbMgr, err := databasemgr.NewDBManager(
			ctx,
			cfg.Storages,
			keyvalue.NewBadgerStore,
			helper.NewTimerTickerFactory(time.Minute),
			logger,
		)
		if err != nil {
			return fmt.Errorf("new db manager: %w", err)
		}
		defer dbMgr.Close()

		var logConsumer storage.LogConsumer
		if cfg.Backup.WALGoCloudURL != "" {
			walSink, err := backup.ResolveSink(ctx, cfg.Backup.WALGoCloudURL)
			if err != nil {
				return fmt.Errorf("resolving write-ahead log backup sink: %w", err)
			}

			walArchiver := backup.NewLogEntryArchiver(logger, walSink, cfg.Backup.WALWorkerCount, &node)
			prometheus.MustRegister(walArchiver)
			walArchiver.Run()
			defer walArchiver.Close()

			logConsumer = walArchiver
		}

		var raftFactory raftmgr.RaftReplicaFactory
		var raftNode *raftmgr.Node

		if cfg.Raft.Enabled {
			raftNode, err = raftmgr.NewNode(cfg, logger, dbMgr, conns)
			if err != nil {
				return fmt.Errorf("new raft node: %w", err)
			}

			raftFactory = raftmgr.DefaultFactoryWithNode(cfg.Raft, raftNode)
		}

		var offloadingSink *offloading.Sink
		if cfg.Offloading.Enabled {
			if cfg.Offloading.GoCloudURL == "" {
				return fmt.Errorf("empty offloading storage URL")
			}
			var bucket *blob.Bucket
			var err error
			if bucket, err = blob.OpenBucket(ctx, cfg.Offloading.GoCloudURL); err != nil {
				return fmt.Errorf("create offloading bucket: %w", err)
			}
			defer func() { _ = bucket.Close() }()

			if offloadingSink, err = offloading.NewSink(bucket); err != nil {
				return fmt.Errorf("create offloading sink: %w", err)
			}
		}

		partitionFactoryOptions := []partition.FactoryOption{
			partition.WithCmdFactory(gitCmdFactory),
			partition.WithRepoFactory(localrepoFactory),
			partition.WithMetrics(partitionMetrics),
			partition.WithLogConsumer(logConsumer),
			partition.WithRaftConfig(cfg.Raft),
			partition.WithRaftFactory(raftFactory),
			partition.WithOffloadingSink(offloadingSink),
		}

		nodeMgr, err := nodeimpl.NewManager(
			cfg.Storages,
			storagemgr.NewFactory(
				logger,
				dbMgr,
				migration.NewFactory(
					partition.NewFactory(partitionFactoryOptions...),
					migrationMetrics,
					migrations,
				),
				cfg.Transactions.MaxInactivePartitions,
				storageMetrics,
			),
		)
		if err != nil {
			return fmt.Errorf("new node manager: %w", err)
		}
		defer nodeMgr.Close()

		if cfg.Raft.Enabled {
			for _, storageCfg := range cfg.Storages {
				baseStorage, err := nodeMgr.GetStorage(storageCfg.Name)
				if err != nil {
					return fmt.Errorf("get base storage %q from node manager: %w", storageCfg.Name, err)
				}
				if err := raftNode.SetBaseStorage(storageCfg.Name, baseStorage); err != nil {
					return fmt.Errorf("set base storage for raft node %q: %w", storageCfg.Name, err)
				}
			}
			node = raftNode

			// Start partition worker synchronously as it is a pre-requisite for Raft.
			if err := storagemgr.AssignmentWorker(ctx, cfg, node, dbMgr, locator); err != nil {
				return fmt.Errorf("partition assignment worker: %w", err)
			}
		} else {
			node = nodeMgr
		}

		reftableMigrator := reftable.NewMigrator(logger, reftableMigratorMetrics, node, localrepoFactory)
		reftableMigrator.Run()
		defer reftableMigrator.Close()

		txMiddleware = server.TransactionMiddleware{
			UnaryInterceptors: []grpc.UnaryServerInterceptor{
				storagemgr.NewUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, txRegistry, node, locator),
				reftable.NewUnaryInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator),
			},
			StreamInterceptors: []grpc.StreamServerInterceptor{
				storagemgr.NewStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, txRegistry, node, locator),
				reftable.NewStreamInterceptor(logger, protoregistry.GitalyProtoPreregistered, reftableMigrator),
			},
		}
	} else {
		storagePaths := make([]string, len(cfg.Storages))
		for i := range cfg.Storages {
			storagePaths[i] = cfg.Storages[i].Path
		}

		if mayHaveWAL, err := storagemgr.MayHavePendingWAL(storagePaths); err != nil {
			return fmt.Errorf("may have pending WAL: %w", err)
		} else if mayHaveWAL {
			dbMgr, err := databasemgr.NewDBManager(
				ctx,
				cfg.Storages,
				keyvalue.NewBadgerStore,
				helper.NewTimerTickerFactory(time.Minute),
				logger,
			)
			if err != nil {
				return fmt.Errorf("new db manager: %w", err)
			}
			defer dbMgr.Close()

			partitionFactoryOptions := []partition.FactoryOption{
				partition.WithCmdFactory(gitCmdFactory),
				partition.WithRepoFactory(localrepoFactory),
				partition.WithMetrics(partitionMetrics),
				partition.WithRaftConfig(cfg.Raft),
			}

			nodeMgr, err := nodeimpl.NewManager(
				cfg.Storages,
				storagemgr.NewFactory(
					logger,
					dbMgr,
					partition.NewFactory(partitionFactoryOptions...),
					// In recovery mode we don't want to keep inactive partitions active. The cache
					// however can't be disabled so simply set it to one.
					1,
					storageMetrics,
				),
			)
			if err != nil {
				return fmt.Errorf("new node: %w", err)
			}
			defer nodeMgr.Close()

			recoveryMiddleware := storagemgr.NewTransactionRecoveryMiddleware(protoregistry.GitalyProtoPreregistered, nodeMgr)
			txMiddleware = server.TransactionMiddleware{
				UnaryInterceptors: []grpc.UnaryServerInterceptor{
					recoveryMiddleware.UnaryServerInterceptor(),
				},
				StreamInterceptors: []grpc.StreamServerInterceptor{
					recoveryMiddleware.StreamServerInterceptor(),
				},
			}
		}
	}

	housekeepingManager := housekeepingmgr.New(cfg.Prometheus, logger, transactionManager, node)
	prometheus.MustRegister(housekeepingManager)

	housekeepingMiddleware := housekeepingmw.NewHousekeepingMiddleware(logger, protoregistry.GitalyProtoPreregistered, localrepoFactory, housekeepingManager, 20)
	defer housekeepingMiddleware.WaitForWorkers()

	gitalyServerFactory := server.NewGitalyServerFactory(
		cfg,
		logger,
		registry,
		diskCache,
		[]*limithandler.LimiterMiddleware{perRPCLimitHandler},
		housekeepingMiddleware,
		txMiddleware,
	)
	defer gitalyServerFactory.Stop()

	gitlabClient := gitlab.NewStubClient()
	if skipHooks, _ := env.GetBool("GITALY_TESTING_NO_GIT_HOOKS", false); skipHooks {
		logger.Warn("skipping GitLab API client creation since hooks are bypassed via GITALY_TESTING_NO_GIT_HOOKS")
	} else {
		httpClient, err := gitlab.NewHTTPClient(logger, cfg.Gitlab, cfg.TLS, cfg.Prometheus)
		if err != nil {
			return fmt.Errorf("could not create GitLab API client: %w", err)
		}
		prometheus.MustRegister(httpClient)
		gitlabClient = httpClient
	}

	hookManager := hook.NewManager(
		cfg,
		locator,
		logger,
		gitCmdFactory,
		transactionManager,
		gitlabClient,
		hook.NewTransactionRegistry(txRegistry),
		hook.NewProcReceiveRegistry(),
		node,
	)

	updaterWithHooks := updateref.NewUpdaterWithHooks(cfg, logger, locator, hookManager, gitCmdFactory, catfileCache)

	streamCache := streamcache.New(cfg.PackObjectsCache, logger)

	var backupSink *backup.Sink
	var backupLocator backup.Locator
	if cfg.Backup.GoCloudURL != "" {
		var err error
		backupSink, err = backup.ResolveSink(ctx, cfg.Backup.GoCloudURL, backup.WithBufferSize(cfg.Backup.BufferSize))
		if err != nil {
			return fmt.Errorf("resolve backup sink: %w", err)
		}
		backupLocator, err = backup.ResolveLocator(cfg.Backup.Layout, backupSink)
		if err != nil {
			return fmt.Errorf("resolve backup locator: %w", err)
		}
	}

	var bundleURIManager *bundleuri.GenerationManager
	if cfg.BundleURI.GoCloudURL != "" {
		bundleURISink, err := bundleuri.NewSink(ctx, cfg.BundleURI.GoCloudURL)
		if err != nil {
			return fmt.Errorf("create bundle-URI sink: %w", err)
		}

		// The manager created here merely to have a non-nil manager in order to use
		// the SignedURL() method on it. It is not used, yet, to generate bundles
		// based on this configuration.
		// Further tests and analysis would be required to come up with the
		// appropriate configuration. This will be done once we are ready to use this manager
		// to generate bundles.
		maxBundleAge := time.Hour * 24
		interval := time.Minute
		maxConcurrent := 5
		threshold := 5
		bundleGenerationStrategy, err := bundleuri.NewOccurrenceStrategy(logger, threshold, interval, maxConcurrent, maxBundleAge)
		if err != nil {
			return fmt.Errorf("error creating bundle generation strategy: %w", err)
		}

		prometheus.MustRegister(bundleGenerationStrategy)
		stop := bundleGenerationStrategy.Start(ctx)
		defer stop()

		bundleURIManager, err = bundleuri.NewGenerationManager(ctx, bundleURISink, logger, node, bundleGenerationStrategy)
		if err != nil {
			return fmt.Errorf("error creating bundle manager: %w", err)
		}
		logger.Info(fmt.Sprintf("bundle-uri bucket configured: %s", cfg.BundleURI.GoCloudURL))
		prometheus.MustRegister(bundleURIManager)
	}

	for _, c := range []starter.Config{
		{Name: starter.Unix, Addr: cfg.SocketPath, HandoverOnUpgrade: true},
		{Name: starter.Unix, Addr: cfg.InternalSocketPath(), HandoverOnUpgrade: false},
		{Name: starter.TCP, Addr: cfg.ListenAddr, HandoverOnUpgrade: true},
		{Name: starter.TLS, Addr: cfg.TLSListenAddr, HandoverOnUpgrade: true},
	} {
		if c.Addr == "" {
			continue
		}

		var srv *grpc.Server
		if c.HandoverOnUpgrade {
			srv, err = gitalyServerFactory.CreateExternal(c.IsSecure())
			if err != nil {
				return fmt.Errorf("create external gRPC server: %w", err)
			}
		} else {
			srv, err = gitalyServerFactory.CreateInternal()
			if err != nil {
				return fmt.Errorf("create internal gRPC server: %w", err)
			}
		}

		setup.RegisterAll(srv, &service.Dependencies{
			Logger:                 logger,
			Cfg:                    cfg,
			GitalyHookManager:      hookManager,
			TransactionManager:     transactionManager,
			StorageLocator:         locator,
			ClientPool:             conns,
			GitCmdFactory:          gitCmdFactory,
			CatfileCache:           catfileCache,
			DiskCache:              diskCache,
			PackObjectsCache:       streamCache,
			PackObjectsLimiter:     packObjectsLimiter,
			RepositoryCounter:      repoCounter,
			UpdaterWithHooks:       updaterWithHooks,
			Node:                   node,
			TransactionRegistry:    txRegistry,
			HousekeepingManager:    housekeepingManager,
			BackupSink:             backupSink,
			BackupLocator:          backupLocator,
			LocalRepositoryFactory: localrepoFactory,
			BundleURIManager:       bundleURIManager,
			MigrationStateManager:  migration.NewStateManager(migrations),
		})
		b.RegisterStarter(starter.New(c, srv, logger))
	}

	if addr := cfg.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error, _ *prometheus.CounterVec) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			logger.WithField("address", addr).Info("starting prometheus listener")

			go func() {
				opts := []monitoring.Option{
					monitoring.WithListener(l),
					monitoring.WithBuildExtraLabels(
						map[string]string{"git_version": gitVersion.String()},
					),
				}

				if buildInfo, ok := debug.ReadBuildInfo(); ok {
					opts = append(opts, monitoring.WithGoBuildInformation(buildInfo))
				}

				if err := monitoring.Start(opts...); err != nil {
					logger.WithError(err).Error("Unable to serve prometheus")
				}
			}()

			return nil
		})
	}

	for _, shard := range cfg.Storages {
		if err := mdfile.WriteMetadataFile(ctx, shard.Path); err != nil {
			// TODO should this be a return? https://gitlab.com/gitlab-org/gitaly/issues/1893
			logger.WithError(err).Error("Unable to write gitaly metadata file")
		}
	}

	// When cgroups are configured, we create a directory structure each
	// time a gitaly process is spawned. Look through the hierarchy root
	// to find any cgroup directories that belong to old gitaly processes
	// and remove them.
	cgroups.StartPruningOldCgroups(cfg.Cgroups, logger)
	repoCounter.StartCountingRepositories(ctx, locator, logger)
	tempdir.StartCleaning(logger, locator, cfg.Storages, time.Hour)

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %w", err)
	}
	bootstrapSpan.Finish()
	// There are a few goroutines running async tasks that may still be in progress (i.e. preloading the license
	// database), but this is a close enough indication of startup latency.
	logger.WithField("duration_ms", time.Since(beganRun).Milliseconds()).Info("Started Gitaly")

	if !cfg.DailyMaintenance.IsDisabled() {
		shutdownWorkers, err := maintenance.StartWorkers(
			ctx,
			logger,
			maintenance.DailyOptimizationWorker(cfg, maintenance.OptimizerFunc(func(ctx context.Context, logger log.Logger, repo storage.Repository) error {
				return housekeepingManager.OptimizeRepository(ctx, localrepo.New(logger, locator, gitCmdFactory, catfileCache, repo))
			})),
		)
		if err != nil {
			return fmt.Errorf("initialize auxiliary workers: %w", err)
		}
		defer shutdownWorkers()
	}

	gracefulStopTicker := helper.NewTimerTicker(cfg.GracefulRestartTimeout.Duration())
	defer gracefulStopTicker.Stop()

	return b.Wait(gracefulStopTicker, gitalyServerFactory.GracefulStop)
}
