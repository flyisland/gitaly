package housekeeping

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
)

// SelectiveOptimizationStrategy wraps HeuristicalOptimizationStrategy but only
// enables operations that have been explicitly scheduled based on their individual thresholds.
type SelectiveOptimizationStrategy struct {
	inner      *HeuristicalOptimizationStrategy
	enabledOps map[config.OperationType]bool
}

// NewSelectiveOptimizationStrategy creates a strategy that only runs the specified operations.
func NewSelectiveOptimizationStrategy(info stats.RepositoryInfo, ops []config.OperationType) *SelectiveOptimizationStrategy {
	enabledOps := make(map[config.OperationType]bool)
	for _, op := range ops {
		enabledOps[op] = true
	}

	heuristicalStrategy := NewHeuristicalOptimizationStrategy(info)
	return &SelectiveOptimizationStrategy{
		inner:      &heuristicalStrategy,
		enabledOps: enabledOps,
	}
}

// EnabledOps returns the map of enabled operations for testing purposes.
func (s *SelectiveOptimizationStrategy) EnabledOps() map[config.OperationType]bool {
	return s.enabledOps
}

// ShouldRepackObjects delegates to the inner strategy only if repack is enabled.
func (s *SelectiveOptimizationStrategy) ShouldRepackObjects(ctx context.Context) (bool, config.RepackObjectsConfig) {
	if !s.enabledOps[config.OpRepackObjects] {
		return false, config.RepackObjectsConfig{}
	}
	return s.inner.ShouldRepackObjects(ctx)
}

// ShouldPruneObjects delegates to the inner strategy only if prune is enabled.
func (s *SelectiveOptimizationStrategy) ShouldPruneObjects(ctx context.Context) (bool, PruneObjectsConfig) {
	if !s.enabledOps[config.OpPruneObjects] {
		return false, PruneObjectsConfig{}
	}
	return s.inner.ShouldPruneObjects(ctx)
}

// ShouldRepackReferences delegates to the inner strategy only if pack-refs is enabled.
func (s *SelectiveOptimizationStrategy) ShouldRepackReferences(ctx context.Context) bool {
	if !s.enabledOps[config.OpRepackRefs] {
		return false
	}
	return s.inner.ShouldRepackReferences(ctx)
}

// ShouldWriteCommitGraph delegates to the inner strategy only if commit-graph is enabled.
func (s *SelectiveOptimizationStrategy) ShouldWriteCommitGraph(ctx context.Context) (bool, config.WriteCommitGraphConfig, error) {
	if !s.enabledOps[config.OpWriteCommitGraph] {
		return false, config.WriteCommitGraphConfig{}, nil
	}
	return s.inner.ShouldWriteCommitGraph(ctx)
}
