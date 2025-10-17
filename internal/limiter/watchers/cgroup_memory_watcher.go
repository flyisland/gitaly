package watchers

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
)

const (
	cgroupMemoryWatcherName = "CgroupMemory"
	defaultMemoryThreshold  = 0.9
)

// CgroupMemoryWatcher implements ResourceWatcher interface. This watcher polls
// the statistics from the cgroup manager. It returns a backoff event in two
// conditions:
// * The current memory usage exceeds a soft threshold (90%).
// * The cgroup is under OOM.
type CgroupMemoryWatcher struct {
	manager         cgroups.Manager
	memoryThreshold float64
}

// NewCgroupMemoryWatcher is the initializer of CgroupMemoryWatcher
func NewCgroupMemoryWatcher(manager cgroups.Manager, memoryThreshold float64) *CgroupMemoryWatcher {
	if memoryThreshold == 0 {
		memoryThreshold = defaultMemoryThreshold
	}
	return &CgroupMemoryWatcher{
		manager:         manager,
		memoryThreshold: memoryThreshold,
	}
}

// Name returns the name of CgroupMemoryWatcher
func (c *CgroupMemoryWatcher) Name() string {
	return cgroupMemoryWatcherName
}

// Poll asserts the cgroup statistics and returns a backoff event accordingly
// when it is triggered. These stats are fetched from cgroup manager.
func (c *CgroupMemoryWatcher) Poll(context.Context) (*limiter.BackoffEvent, error) {
	if !c.manager.Ready() {
		return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
	}

	stats, err := c.manager.Stats()
	if err != nil {
		return nil, fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", err)
	}
	parentStats := stats.ParentStats

	// Whether the parent cgroup isthe memory cgroup is under OOM, tasks may be stopped. This stat is available in
	// Cgroup V1 only.
	if parentStats.UnderOOM {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        "cgroup is under OOM",
		}, nil
	}

	// MemoryUsage reports the total memory usage of the parent cgroup and its descendants. However, it aggregates
	// different types of memory. Each of them affect cgroup reclaim and eviction policy. The more accurate
	// breakdown can be found in `memory.stat` file of the parent cgroup. The stat consists of:
	// - Anonymous memory (`rss` in V1/`anon` in V2).
	// - Page caches (cache in V1/File in V2)
	// - Swap and some Kernel memory.
	// When the cgroup faces a memory pressure, the cgroup attempts to evict a small amount of memory enough for new
	// allocations. If it cannot make enough space, OOM-Killer kicks in. Anonymous memory cannot be evicted, except
	// for some special insignificant cases (LazyFree for example). A portion of the Page Caches, noted by `inactive_file`,
	// is the target for the eviction first. So, it makes sense to exclude the easy evictable memory from the threshold.
	if parentStats.MemoryLimit > 0 && parentStats.MemoryUsage > 0 &&
		float64(parentStats.MemoryUsage-parentStats.TotalInactiveFile)/float64(parentStats.MemoryLimit) >= c.memoryThreshold {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        "cgroup memory exceeds threshold",
			Stats: map[string]any{
				"memory_usage":     parentStats.MemoryUsage,
				"inactive_file":    parentStats.TotalInactiveFile,
				"memory_limit":     parentStats.MemoryLimit,
				"memory_threshold": c.memoryThreshold,
			},
		}, nil
	}

	return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
}
