package watchers

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
)

const (
	cgroupMemoryWatcherName = "CgroupMemory"
	defaultMemoryThreshold  = 0.6
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

	if c.exceedsAnonMemoryThreshold(parentStats) {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        "cgroup memory exceeds threshold",
			Stats:         buildMemoryBackoffStats(parentStats, c.memoryThreshold),
		}, nil
	}

	return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
}

// exceedsAnonMemoryThreshold reports whether the anonymous memory usage of the
// cgroup exceeds the configured threshold relative to the memory limit.
func (c *CgroupMemoryWatcher) exceedsAnonMemoryThreshold(stats cgroups.CgroupStats) bool {
	if stats.MemoryLimit == 0 {
		return false
	}

	anonRatio := float64(stats.TotalAnon) / float64(stats.MemoryLimit)
	return anonRatio >= c.memoryThreshold
}

// PSI metrics are only available on cgroups v2 (will be 0 on v1).
func buildBackoffStats(stats cgroups.CgroupStats) map[string]any {
	anonRatio := 0.0
	if stats.MemoryLimit > 0 {
		anonRatio = float64(stats.TotalAnon) / float64(stats.MemoryLimit)
	}

	return map[string]any{
		"memory_usage":                stats.MemoryUsage,
		"memory_limit":                stats.MemoryLimit,
		"inactive_file":               stats.TotalInactiveFile,
		"anon":                        stats.TotalAnon,
		"anon_ratio":                  anonRatio,
		"memory_high_events":          stats.MemoryHighEvents,
		"memory_max_events":           stats.MemoryMaxEvents,
		"oom_kills":                   stats.OOMKills,
		"memory_pressure_some_avg10":  stats.MemoryPSI.Some.Avg10,
		"memory_pressure_some_avg60":  stats.MemoryPSI.Some.Avg60,
		"memory_pressure_some_avg300": stats.MemoryPSI.Some.Avg300,
		"memory_pressure_full_avg10":  stats.MemoryPSI.Full.Avg10,
		"memory_pressure_full_avg60":  stats.MemoryPSI.Full.Avg60,
		"memory_pressure_full_avg300": stats.MemoryPSI.Full.Avg300,
		"io_pressure_some_avg10":      stats.IOPSI.Some.Avg10,
		"io_pressure_some_avg60":      stats.IOPSI.Some.Avg60,
		"io_pressure_some_avg300":     stats.IOPSI.Some.Avg300,
		"io_pressure_full_avg10":      stats.IOPSI.Full.Avg10,
		"io_pressure_full_avg60":      stats.IOPSI.Full.Avg60,
		"io_pressure_full_avg300":     stats.IOPSI.Full.Avg300,
		"cpu_pressure_some_avg10":     stats.CPUPSI.Some.Avg10,
		"cpu_pressure_some_avg60":     stats.CPUPSI.Some.Avg60,
		"cpu_pressure_some_avg300":    stats.CPUPSI.Some.Avg300,
		"pgmajfault":                  stats.PgMajFault,
	}
}

func buildMemoryBackoffStats(stats cgroups.CgroupStats, memoryThreshold float64) map[string]any {
	m := buildBackoffStats(stats)
	m["memory_threshold"] = memoryThreshold
	return m
}
