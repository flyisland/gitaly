package watchers

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	cgroupMemoryWatcherName = "CgroupMemory"
	defaultMemoryThreshold  = 0.9
	anonMemoryThreshold     = 0.6
)

// CgroupMemoryWatcher implements ResourceWatcher interface. This watcher polls
// the statistics from the cgroup manager. It returns a backoff event in two
// conditions:
// * The current memory usage exceeds a soft threshold (90%).
// * The cgroup is under OOM.
type CgroupMemoryWatcher struct {
	manager         cgroups.Manager
	memoryThreshold float64
	logger          log.Logger
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

// WithLogger sets the logger for dry-run anonymous memory logging
// It is added as a separate enricher to reduce code changes and make
// the cleanup later easier.
func (c *CgroupMemoryWatcher) WithLogger(logger log.Logger) *CgroupMemoryWatcher {
	c.logger = logger
	return c
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

	// Log anonymous memory pressure independently (dry-run, no backoff)
	if c.logger != nil && exceedsAnonMemoryThreshold(parentStats) {
		c.logger.WithFields(buildMemoryBackoffStats(parentStats, float64(anonMemoryThreshold))).Warn("Anonymous memory pressure detected")
	}

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
			Stats:         buildMemoryBackoffStats(parentStats, c.memoryThreshold),
		}, nil
	}

	return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
}

// PSI metrics are only available on cgroups v2 (will be 0 on v1).
func buildBackoffStats(stats cgroups.CgroupStats) map[string]any {
	anonRatio := 0.0
	if stats.MemoryLimit > 0 {
		anonRatio = float64(stats.TotalAnon) / float64(stats.MemoryLimit)
	}

	return map[string]any{
		"memory_usage":               stats.MemoryUsage,
		"memory_limit":               stats.MemoryLimit,
		"inactive_file":              stats.TotalInactiveFile,
		"anon":                       stats.TotalAnon,
		"anon_ratio":                 anonRatio,
		"memory_high_events":         stats.MemoryHighEvents,
		"memory_max_events":          stats.MemoryMaxEvents,
		"oom_kills":                  stats.OOMKills,
		"memory_pressure_some_avg10": stats.MemoryPSI.Some.Avg10,
		"memory_pressure_some_avg60": stats.MemoryPSI.Some.Avg60,
		"memory_pressure_full_avg10": stats.MemoryPSI.Full.Avg10,
		"memory_pressure_full_avg60": stats.MemoryPSI.Full.Avg60,
		"io_pressure_some_avg10":     stats.IOPSI.Some.Avg10,
		"io_pressure_some_avg60":     stats.IOPSI.Some.Avg60,
		"io_pressure_full_avg10":     stats.IOPSI.Full.Avg10,
		"io_pressure_full_avg60":     stats.IOPSI.Full.Avg60,
		"pgmajfault":                 stats.PgMajFault,
	}
}

func buildMemoryBackoffStats(stats cgroups.CgroupStats, memoryThreshold float64) map[string]any {
	m := buildBackoffStats(stats)
	m["memory_threshold"] = memoryThreshold
	return m
}

// exceedsAnonMemoryThreshold reports whether the anonymous memory usage of the
// cgroup exceeds the configured threshold relative to the memory limit.
func exceedsAnonMemoryThreshold(stats cgroups.CgroupStats) bool {
	if stats.MemoryLimit == 0 {
		return false
	}

	anonRatio := float64(stats.TotalAnon) / float64(stats.MemoryLimit)
	return anonRatio >= anonMemoryThreshold
}
