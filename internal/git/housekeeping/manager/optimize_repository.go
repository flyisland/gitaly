package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

// OptimizeRepositoryConfig is the configuration used by OptimizeRepository that is computed by
// applying all the OptimizeRepositoryOption modifiers.
type OptimizeRepositoryConfig struct {
	StrategyConstructor OptimizationStrategyConstructor
}

// OptimizeRepositoryOption is an option that can be passed to OptimizeRepository.
type OptimizeRepositoryOption func(cfg *OptimizeRepositoryConfig)

// OptimizationStrategyConstructor is a constructor for an OptimizationStrategy that is being
// informed by the passed-in RepositoryInfo.
type OptimizationStrategyConstructor func(stats.RepositoryInfo) housekeeping.OptimizationStrategy

// WithOptimizationStrategyConstructor changes the constructor for the optimization strategy.that is
// used to determine which parts of the repository will be optimized. By default the
// HeuristicalOptimizationStrategy is used.
func WithOptimizationStrategyConstructor(strategyConstructor OptimizationStrategyConstructor) OptimizeRepositoryOption {
	return func(cfg *OptimizeRepositoryConfig) {
		cfg.StrategyConstructor = strategyConstructor
	}
}

// OptimizeRepository performs optimizations on the repository. Whether optimizations are performed
// or not depends on a set of heuristics.
func (m *RepositoryManager) OptimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	opts ...OptimizeRepositoryOption,
) error {
	var cfg OptimizeRepositoryConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "housekeeping.OptimizeRepository", nil)
	defer span.Finish()

	if err := m.maybeStartTransaction(ctx, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
		originalRepo := &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: repo.GetRelativePath(),
		}
		if tx != nil {
			originalRepo = tx.OriginalRepository(originalRepo)
		}

		// tryRunningHousekeeping acquires a lock on the repository to prevent other concurrent housekeeping calls on the repository.
		// As we may be in a transaction, the repository's relative path may have been rewritten. We use the original unrewritten relative
		// path here to ensure we hit the same key regardless if we run in different transactions where the snapshot prefixes in the
		// relative paths may differ.
		ok, cleanup := m.repositoryStates.tryRunningHousekeeping(originalRepo)
		// If we didn't succeed to set the state to "running" because of a concurrent housekeeping run
		// we exit early.
		if !ok {
			return nil
		}
		defer cleanup()

		if m.optimizeFunc != nil {
			strategy, err := m.validate(ctx, repo, cfg)
			if err != nil {
				return err
			}
			return m.optimizeFunc(ctx, repo, strategy)
		}

		if tx != nil {
			return m.optimizeRepositoryWithTransaction(ctx, repo, cfg)
		}

		return m.optimizeRepository(ctx, repo, cfg)
	}); err != nil {
		return err
	}

	return nil
}

func (m *RepositoryManager) maybeStartTransaction(ctx context.Context, repo *localrepo.Repo, run func(context.Context, storage.Transaction, *localrepo.Repo) error) error {
	if m.node == nil {
		return run(ctx, nil, repo)
	}

	return m.runInTransaction(ctx, true, repo, run)
}

func (m *RepositoryManager) runInTransaction(ctx context.Context, readOnly bool, repo *localrepo.Repo, run func(context.Context, storage.Transaction, *localrepo.Repo) error) (returnedErr error) {
	originalRepo := &gitalypb.Repository{
		StorageName:  repo.GetStorageName(),
		RelativePath: repo.GetRelativePath(),
	}
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		originalRepo = tx.OriginalRepository(originalRepo)
	}

	storageHandle, err := m.node.GetStorage(repo.GetStorageName())
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	tx, err := storageHandle.Begin(ctx, storage.TransactionOptions{
		ReadOnly:     readOnly,
		RelativePath: originalRepo.GetRelativePath(),
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			if err := tx.Rollback(ctx); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("rollback: %w", err))
			}
		}
	}()

	if err := run(
		storage.ContextWithTransaction(ctx, tx),
		tx,
		localrepo.NewFrom(repo, tx.RewriteRepository(originalRepo)),
	); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (m *RepositoryManager) validate(
	ctx context.Context,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
) (housekeeping.OptimizationStrategy, error) {
	repositoryInfo, err := stats.RepositoryInfoForRepository(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}

	repositoryInfo.Log(ctx, m.logger)
	m.metrics.ReportRepositoryInfo(repositoryInfo)

	var strategy housekeeping.OptimizationStrategy
	if cfg.StrategyConstructor == nil {
		strategy = housekeeping.NewHeuristicalOptimizationStrategy(repositoryInfo)
	} else {
		strategy = cfg.StrategyConstructor(repositoryInfo)
	}

	return strategy, nil
}

func (m *RepositoryManager) optimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
) error {
	strategy, err := m.validate(ctx, repo, cfg)
	if err != nil {
		return err
	}

	finishTotalTimer := m.metrics.ReportTaskLatency("total", "apply")
	totalStatus := "failure"

	optimizations := make(map[string]string)
	defer func() {
		finishTotalTimer()
		m.logger.WithField("optimizations", optimizations).Info("optimized repository")

		for task, status := range optimizations {
			m.metrics.TasksTotal.WithLabelValues(task, status).Inc()
		}

		m.metrics.TasksTotal.WithLabelValues("total", totalStatus).Add(1)
	}()

	finishTimer := m.metrics.ReportTaskLatency("clean-stale-data", "apply")
	if err := m.CleanStaleData(ctx, repo, housekeeping.DefaultStaleDataCleanup()); err != nil {
		return fmt.Errorf("could not execute housekeeping: %w", err)
	}
	finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("clean-worktrees", "apply")
	if err := housekeeping.CleanupWorktrees(ctx, repo); err != nil {
		return fmt.Errorf("could not clean up worktrees: %w", err)
	}
	finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("repack", "apply")
	didRepack, repackCfg, err := repackIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_objects_"+string(repackCfg.Strategy)] = "failure"
		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = "failure"
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = "failure"
		}

		return fmt.Errorf("could not repack: %w", err)
	}
	if didRepack {
		optimizations["packed_objects_"+string(repackCfg.Strategy)] = "success"

		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = "success"
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = "success"
		}
	}
	finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("prune", "apply")
	didPrune, err := pruneIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["pruned_objects"] = "failure"
		return fmt.Errorf("could not prune: %w", err)
	} else if didPrune {
		optimizations["pruned_objects"] = "success"
	}
	finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("pack-refs", "apply")
	didPackRefs, err := m.packRefsIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_refs"] = "failure"
		return fmt.Errorf("could not pack refs: %w", err)
	} else if didPackRefs {
		optimizations["packed_refs"] = "success"
	}
	finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("commit-graph", "apply")
	if didWriteCommitGraph, writeCommitGraphCfg, err := writeCommitGraphIfNeeded(ctx, repo, strategy); err != nil {
		optimizations["written_commit_graph_full"] = "failure"
		optimizations["written_commit_graph_incremental"] = "failure"
		return fmt.Errorf("could not write commit-graph: %w", err)
	} else if didWriteCommitGraph {
		if writeCommitGraphCfg.ReplaceChain {
			optimizations["written_commit_graph_full"] = "success"
		} else {
			optimizations["written_commit_graph_incremental"] = "success"
		}
	}
	finishTimer()

	totalStatus = "success"

	return nil
}

// optimizeRepositoryWithTransaction performs optimizations in the context of WAL transaction.
//
// Reference repacking and object repacking are run in two different transactions. This decreases the chance of conflicts as it
// allow reference repacking to commit faster. Reference repacking conflicts with reference deletions but runs relatively fast.
// Object repacking is slower but is conflict-free if no pruning is done.
//
// Note that the strategy is selected in a parent transaction. The repository's state may change in the meanwhile but this shouldn't
// really change things too much. RepositoryManager itself prevents concurrent housekeeping. Even if there was a housekeeping operation
// committed in between, we'd just do redundant repacks.
func (m *RepositoryManager) optimizeRepositoryWithTransaction(
	ctx context.Context,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
) error {
	strategy, err := m.validate(ctx, repo, cfg)
	if err != nil {
		return err
	}

	repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
	packRefsNeeded := strategy.ShouldRepackReferences(ctx)

	writeCommitGraphNeeded, writeCommitGraphCfg, err := strategy.ShouldWriteCommitGraph(ctx)
	if err != nil {
		return fmt.Errorf("checking commit graph writing eligibility: %w", err)
	}

	var errPackReferences error
	if packRefsNeeded {
		if err := m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
			tx.PackRefs()
			return nil
		}); err != nil {
			errPackReferences = fmt.Errorf("run reference packing: %w", err)
		}
	}

	var errRepackObjects error
	if repackNeeded || writeCommitGraphNeeded {
		if err := m.runInTransaction(ctx, false, repo, func(ctx context.Context, tx storage.Transaction, repo *localrepo.Repo) error {
			if repackNeeded {
				tx.Repack(repackCfg)
			}

			if writeCommitGraphNeeded {
				tx.WriteCommitGraphs(writeCommitGraphCfg)
			}
			return nil
		}); err != nil {
			errRepackObjects = fmt.Errorf("run object repacking: %w", err)
		}
	}

	getStatus := func(err error) string {
		if err != nil {
			return "failure"
		}

		return "success"
	}

	repackObjectsStatus := getStatus(errRepackObjects)

	optimizations := make(map[string]string)
	if repackNeeded {
		optimizations["packed_objects_"+string(repackCfg.Strategy)] = repackObjectsStatus
		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = repackObjectsStatus
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = repackObjectsStatus
		}
	}

	if packRefsNeeded {
		optimizations["packed_refs"] = getStatus(errPackReferences)
	}

	if writeCommitGraphNeeded {
		if writeCommitGraphCfg.ReplaceChain {
			optimizations["written_commit_graph_full"] = repackObjectsStatus
		} else {
			optimizations["written_commit_graph_incremental"] = repackObjectsStatus
		}
	}

	m.logger.WithField("optimizations", optimizations).Info("optimized repository with WAL")
	for task, status := range optimizations {
		m.metrics.TasksTotal.WithLabelValues(task, status).Inc()
	}

	errCombined := errors.Join(errPackReferences, errRepackObjects)
	m.metrics.TasksTotal.WithLabelValues("total", getStatus(errCombined)).Add(1)

	return errCombined
}

// repackIfNeeded repacks the repository according to the strategy.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, config.RepackObjectsConfig, error) {
	repackNeeded, cfg := strategy.ShouldRepackObjects(ctx)
	if !repackNeeded {
		return false, config.RepackObjectsConfig{}, nil
	}

	if err := housekeeping.RepackObjects(ctx, repo, cfg); err != nil {
		return false, cfg, err
	}

	return true, cfg, nil
}

// writeCommitGraphIfNeeded writes the commit-graph if required.
func writeCommitGraphIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, config.WriteCommitGraphConfig, error) {
	needed, cfg, err := strategy.ShouldWriteCommitGraph(ctx)
	if !needed || err != nil {
		return false, config.WriteCommitGraphConfig{}, err
	}

	if err := housekeeping.WriteCommitGraph(ctx, repo, cfg); err != nil {
		return true, cfg, fmt.Errorf("writing commit-graph: %w", err)
	}

	return true, cfg, nil
}

// pruneIfNeeded removes objects from the repository which are either unreachable or which are
// already part of a packfile. We use a grace period of two weeks.
func pruneIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, error) {
	needed, cfg := strategy.ShouldPruneObjects(ctx)
	if !needed {
		return false, nil
	}

	if err := housekeeping.PruneObjects(ctx, repo, cfg); err != nil {
		return true, fmt.Errorf("pruning objects: %w", err)
	}

	return true, nil
}

func (m *RepositoryManager) packRefsIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, error) {
	if !strategy.ShouldRepackReferences(ctx) {
		return false, nil
	}

	// If there are any inhibitors, we don't run git-pack-refs(1).
	ok, cleanup := m.repositoryStates.tryRunningPackRefs(repo)
	if !ok {
		return false, nil
	}
	defer cleanup()

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, gitcmd.Command{
		Name: "pack-refs",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--auto"},
		},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return false, fmt.Errorf("packing refs: %w, stderr: %q", err, stderr.String())
	}

	return true, nil
}

// CleanStaleData removes any stale data in the repository as per the provided configuration.
func (m *RepositoryManager) CleanStaleData(ctx context.Context, repo *localrepo.Repo, cfg housekeeping.CleanStaleDataConfig) error {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "housekeeping.CleanStaleData", nil)
	defer span.Finish()

	repoPath, err := repo.Path(ctx)
	if err != nil {
		m.logger.WithError(err).WarnContext(ctx, "housekeeping failed to get repo path")
		if structerr.GRPCCode(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("housekeeping failed to get repo path: %w", err)
	}

	staleDataByType := map[string]int{}
	defer func() {
		if len(staleDataByType) == 0 {
			return
		}

		logEntry := m.logger
		for staleDataType, count := range staleDataByType {
			logEntry = logEntry.WithField(fmt.Sprintf("stale_data.%s", staleDataType), count)
			m.metrics.PrunedFilesTotal.WithLabelValues(staleDataType).Add(float64(count))
		}
		logEntry.InfoContext(ctx, "removed files")
	}()

	var filesToPrune []string
	for staleFileType, staleFileFinder := range cfg.StaleFileFinders {
		staleFiles, err := staleFileFinder(ctx, repoPath)
		if err != nil {
			return fmt.Errorf("housekeeping failed to find %s: %w", staleFileType, err)
		}

		filesToPrune = append(filesToPrune, staleFiles...)
		staleDataByType[staleFileType] = len(staleFiles)
	}

	for _, path := range filesToPrune {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			staleDataByType["failures"]++
			m.logger.WithError(err).WithField("path", path).WarnContext(ctx, "unable to remove stale file")
		}
	}

	for repoCleanupName, repoCleanupFn := range cfg.RepoCleanups {
		cleanupCount, err := repoCleanupFn(ctx, repo)
		staleDataByType[repoCleanupName] = cleanupCount
		if err != nil {
			return fmt.Errorf("housekeeping could not perform cleanup %s: %w", repoCleanupName, err)
		}
	}

	for repoCleanupName, repoCleanupFn := range cfg.RepoCleanupWithTxManagers {
		cleanupCount, err := repoCleanupFn(ctx, repo, m.txManager)
		staleDataByType[repoCleanupName] = cleanupCount
		if err != nil {
			return fmt.Errorf("housekeeping could not perform cleanup (with TxManager) %s: %w", repoCleanupName, err)
		}
	}

	return nil
}
