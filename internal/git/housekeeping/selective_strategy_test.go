package housekeeping

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestSelectiveOptimizationStrategy_ShouldRepackObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Repository info that would trigger a repack in HeuristicalOptimizationStrategy
	infoNeedingRepack := stats.RepositoryInfo{
		LooseObjects: stats.LooseObjectsInfo{
			Count: 2000,
		},
		Packfiles: stats.PackfilesInfo{
			Bitmap: stats.BitmapInfo{
				Exists: true,
			},
			MultiPackIndex: stats.MultiPackIndexInfo{
				Exists: true,
			},
		},
	}

	for _, tc := range []struct {
		desc           string
		enabledOps     []config.OperationType
		info           stats.RepositoryInfo
		expectedNeeded bool
		expectedConfig config.RepackObjectsConfig
	}{
		{
			desc:           "repack disabled",
			enabledOps:     []config.OperationType{config.OpRepackRefs, config.OpPruneObjects, config.OpWriteCommitGraph},
			info:           infoNeedingRepack,
			expectedNeeded: false,
			expectedConfig: config.RepackObjectsConfig{},
		},
		{
			desc:           "repack enabled delegates to inner strategy",
			enabledOps:     []config.OperationType{config.OpRepackObjects},
			info:           infoNeedingRepack,
			expectedNeeded: true,
			expectedConfig: config.RepackObjectsConfig{
				Strategy: config.RepackObjectsStrategyIncrementalWithUnreachable,
			},
		},
		{
			desc:           "repack enabled but not needed by inner strategy",
			enabledOps:     []config.OperationType{config.OpRepackObjects},
			info:           stats.RepositoryInfo{},
			expectedNeeded: false,
			expectedConfig: config.RepackObjectsConfig{},
		},
		{
			desc:           "all operations enabled",
			enabledOps:     []config.OperationType{config.OpRepackRefs, config.OpRepackObjects, config.OpPruneObjects, config.OpWriteCommitGraph},
			info:           infoNeedingRepack,
			expectedNeeded: true,
			expectedConfig: config.RepackObjectsConfig{
				Strategy: config.RepackObjectsStrategyIncrementalWithUnreachable,
			},
		},
		{
			desc:           "no operations enabled",
			enabledOps:     []config.OperationType{},
			info:           infoNeedingRepack,
			expectedNeeded: false,
			expectedConfig: config.RepackObjectsConfig{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			strategy := NewSelectiveOptimizationStrategy(tc.info, tc.enabledOps)

			repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
			require.Equal(t, tc.expectedNeeded, repackNeeded)
			require.Equal(t, tc.expectedConfig, repackCfg)
		})
	}
}

func TestSelectiveOptimizationStrategy_ShouldPruneObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Repository info that would trigger a prune in HeuristicalOptimizationStrategy
	infoNeedingPrune := stats.RepositoryInfo{
		LooseObjects: stats.LooseObjectsInfo{
			StaleCount: 2000,
		},
	}

	for _, tc := range []struct {
		desc           string
		enabledOps     []config.OperationType
		info           stats.RepositoryInfo
		expectedNeeded bool
	}{
		{
			desc:           "prune disabled",
			enabledOps:     []config.OperationType{config.OpRepackRefs, config.OpRepackObjects, config.OpWriteCommitGraph},
			info:           infoNeedingPrune,
			expectedNeeded: false,
		},
		{
			desc:           "prune enabled delegates to inner strategy",
			enabledOps:     []config.OperationType{config.OpPruneObjects},
			info:           infoNeedingPrune,
			expectedNeeded: true,
		},
		{
			desc:           "prune enabled but not needed by inner strategy",
			enabledOps:     []config.OperationType{config.OpPruneObjects},
			info:           stats.RepositoryInfo{},
			expectedNeeded: false,
		},
		{
			desc:       "prune disabled for object pools even when enabled",
			enabledOps: []config.OperationType{config.OpPruneObjects},
			info: stats.RepositoryInfo{
				IsObjectPool: true,
				LooseObjects: stats.LooseObjectsInfo{
					StaleCount: 2000,
				},
			},
			expectedNeeded: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			strategy := NewSelectiveOptimizationStrategy(tc.info, tc.enabledOps)

			pruneNeeded, pruneCfg := strategy.ShouldPruneObjects(ctx)
			require.Equal(t, tc.expectedNeeded, pruneNeeded)
			if tc.expectedNeeded {
				require.NotZero(t, pruneCfg.ExpireBefore)
			} else {
				require.Equal(t, PruneObjectsConfig{}, pruneCfg)
			}
		})
	}
}

func TestSelectiveOptimizationStrategy_ShouldRepackReferences(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Repository info that would trigger a pack-refs in HeuristicalOptimizationStrategy
	infoNeedingPackRefs := stats.RepositoryInfo{
		References: stats.ReferencesInfo{
			ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
			LooseReferencesCount: 100,
			PackedReferencesSize: 1024,
		},
	}

	for _, tc := range []struct {
		desc           string
		enabledOps     []config.OperationType
		info           stats.RepositoryInfo
		expectedNeeded bool
	}{
		{
			desc:           "pack-refs disabled",
			enabledOps:     []config.OperationType{config.OpRepackObjects, config.OpPruneObjects, config.OpWriteCommitGraph},
			info:           infoNeedingPackRefs,
			expectedNeeded: false,
		},
		{
			desc:           "pack-refs enabled delegates to inner strategy",
			enabledOps:     []config.OperationType{config.OpRepackRefs},
			info:           infoNeedingPackRefs,
			expectedNeeded: true,
		},
		{
			desc:           "pack-refs enabled but not needed by inner strategy",
			enabledOps:     []config.OperationType{config.OpRepackRefs},
			info:           stats.RepositoryInfo{},
			expectedNeeded: false,
		},
		{
			desc:       "pack-refs enabled with no loose refs",
			enabledOps: []config.OperationType{config.OpRepackRefs},
			info: stats.RepositoryInfo{
				References: stats.ReferencesInfo{
					LooseReferencesCount: 0,
				},
			},
			expectedNeeded: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			strategy := NewSelectiveOptimizationStrategy(tc.info, tc.enabledOps)

			packRefsNeeded := strategy.ShouldRepackReferences(ctx)
			require.Equal(t, tc.expectedNeeded, packRefsNeeded)
		})
	}
}

func TestSelectiveOptimizationStrategy_ShouldWriteCommitGraph(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Repository info that would trigger a commit-graph write in HeuristicalOptimizationStrategy
	infoNeedingCommitGraph := stats.RepositoryInfo{
		References: stats.ReferencesInfo{
			ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
			LooseReferencesCount: 1,
		},
		CommitGraph: stats.CommitGraphInfo{
			HasBloomFilters: false,
		},
	}

	for _, tc := range []struct {
		desc           string
		enabledOps     []config.OperationType
		info           stats.RepositoryInfo
		expectedNeeded bool
		expectedConfig config.WriteCommitGraphConfig
	}{
		{
			desc:           "commit-graph disabled",
			enabledOps:     []config.OperationType{config.OpRepackRefs, config.OpRepackObjects, config.OpPruneObjects},
			info:           infoNeedingCommitGraph,
			expectedNeeded: false,
			expectedConfig: config.WriteCommitGraphConfig{},
		},
		{
			desc:           "commit-graph enabled delegates to inner strategy",
			enabledOps:     []config.OperationType{config.OpWriteCommitGraph},
			info:           infoNeedingCommitGraph,
			expectedNeeded: true,
			expectedConfig: config.WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc:       "commit-graph enabled but not needed by inner strategy",
			enabledOps: []config.OperationType{config.OpWriteCommitGraph},
			info: stats.RepositoryInfo{
				References: stats.ReferencesInfo{
					ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
					LooseReferencesCount: 1,
				},
				CommitGraph: stats.CommitGraphInfo{
					CommitGraphChainLength: 1,
					HasBloomFilters:        true,
					HasGenerationData:      true,
				},
			},
			expectedNeeded: false,
			expectedConfig: config.WriteCommitGraphConfig{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			strategy := NewSelectiveOptimizationStrategy(tc.info, tc.enabledOps)

			commitGraphNeeded, commitGraphCfg, err := strategy.ShouldWriteCommitGraph(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedNeeded, commitGraphNeeded)
			require.Equal(t, tc.expectedConfig, commitGraphCfg)
		})
	}
}

func TestSelectiveOptimizationStrategy_IndependentOperations(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Repository info where all operations would be triggered by HeuristicalOptimizationStrategy
	info := stats.RepositoryInfo{
		LooseObjects: stats.LooseObjectsInfo{
			Count:      2000,
			StaleCount: 2000,
		},
		Packfiles: stats.PackfilesInfo{
			Bitmap: stats.BitmapInfo{
				Exists: true,
			},
			MultiPackIndex: stats.MultiPackIndexInfo{
				Exists: true,
			},
		},
		References: stats.ReferencesInfo{
			ReferenceBackendName: gittest.DefaultReferenceBackend.Name,
			LooseReferencesCount: 100,
			PackedReferencesSize: 1024,
		},
		CommitGraph: stats.CommitGraphInfo{
			HasBloomFilters: false,
		},
	}

	t.Run("only pack-refs enabled", func(t *testing.T) {
		strategy := NewSelectiveOptimizationStrategy(info, []config.OperationType{config.OpRepackRefs})

		repackNeeded, _ := strategy.ShouldRepackObjects(ctx)
		require.False(t, repackNeeded, "repack should be disabled")

		pruneNeeded, _ := strategy.ShouldPruneObjects(ctx)
		require.False(t, pruneNeeded, "prune should be disabled")

		packRefsNeeded := strategy.ShouldRepackReferences(ctx)
		require.True(t, packRefsNeeded, "pack-refs should be enabled and needed")

		commitGraphNeeded, _, err := strategy.ShouldWriteCommitGraph(ctx)
		require.NoError(t, err)
		require.False(t, commitGraphNeeded, "commit-graph should be disabled")
	})

	t.Run("only repack and commit-graph enabled", func(t *testing.T) {
		strategy := NewSelectiveOptimizationStrategy(info, []config.OperationType{config.OpRepackObjects, config.OpWriteCommitGraph})

		repackNeeded, _ := strategy.ShouldRepackObjects(ctx)
		require.True(t, repackNeeded, "repack should be enabled and needed")

		pruneNeeded, _ := strategy.ShouldPruneObjects(ctx)
		require.False(t, pruneNeeded, "prune should be disabled")

		packRefsNeeded := strategy.ShouldRepackReferences(ctx)
		require.False(t, packRefsNeeded, "pack-refs should be disabled")

		commitGraphNeeded, _, err := strategy.ShouldWriteCommitGraph(ctx)
		require.NoError(t, err)
		require.True(t, commitGraphNeeded, "commit-graph should be enabled and needed")
	})

	t.Run("all operations enabled", func(t *testing.T) {
		allOps := []config.OperationType{config.OpRepackRefs, config.OpRepackObjects, config.OpPruneObjects, config.OpWriteCommitGraph}
		selectiveStrategy := NewSelectiveOptimizationStrategy(info, allOps)
		heuristicStrategy := NewHeuristicalOptimizationStrategy(info)

		selectiveRepack, selectiveRepackCfg := selectiveStrategy.ShouldRepackObjects(ctx)
		heuristicRepack, heuristicRepackCfg := heuristicStrategy.ShouldRepackObjects(ctx)
		require.Equal(t, heuristicRepack, selectiveRepack)
		require.Equal(t, heuristicRepackCfg, selectiveRepackCfg)

		selectivePrune, _ := selectiveStrategy.ShouldPruneObjects(ctx)
		heuristicPrune, _ := heuristicStrategy.ShouldPruneObjects(ctx)
		require.Equal(t, heuristicPrune, selectivePrune)

		selectivePackRefs := selectiveStrategy.ShouldRepackReferences(ctx)
		heuristicPackRefs := heuristicStrategy.ShouldRepackReferences(ctx)
		require.Equal(t, heuristicPackRefs, selectivePackRefs)

		selectiveCommitGraph, selectiveCommitGraphCfg, err := selectiveStrategy.ShouldWriteCommitGraph(ctx)
		require.NoError(t, err)
		heuristicCommitGraph, heuristicCommitGraphCfg, err := heuristicStrategy.ShouldWriteCommitGraph(ctx)
		require.NoError(t, err)
		require.Equal(t, heuristicCommitGraph, selectiveCommitGraph)
		require.Equal(t, heuristicCommitGraphCfg, selectiveCommitGraphCfg)
	})
}
