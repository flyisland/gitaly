package cgroups

import (
	"io"
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

type addCommandCfg struct {
	cgroupKey string
}

// PSIData holds the pressure stall information averages and total.
// These metrics indicate the percentage of wall-clock time tasks were stalled.
type PSIData struct {
	Avg10  float64 // 10-second average percentage
	Avg60  float64 // 60-second average percentage
	Avg300 float64 // 300-second (5 minute) average percentage
	Total  uint64  // Cumulative total stall time in microseconds
}

// PSIMetrics holds both "some" and "full" pressure metrics.
// "Some" means at least one task was stalled; "Full" means all tasks were stalled.
type PSIMetrics struct {
	Some PSIData
	Full PSIData
}

// CgroupStats stores the current usage statistics of the resources managed by
// cgroup manager. They are fetched from the cgroupfs statistic files.
type CgroupStats struct {
	// CPUThrottledCount is much the CPU was throttled, this field is fetched from the `nr_throttled` field of
	// `cpu.stat` file.
	CPUThrottledCount uint64
	// CPUThrottledCount is the accumulated duration of CPU throttled time in seconds. It is from the
	// `throttled_time` (V1) or the `throttled_usec` (v2) field of the `cpu.stat` file.
	CPUThrottledDuration float64
	// MemoryUsage is the current memory usage in bytes of a cgroup. It's fetched from the `memory.usage_in_bytes`
	// (V1) or the `memory.current` (V2) files.
	MemoryUsage uint64
	// MemoryLimit is the current memory limit in bytes of a cgroup. It's from the `memory.limit_in_bytes` (V1) or
	// the `memory.max` (V2) files.
	MemoryLimit uint64
	// OOMKills is the accumulated count when the cgroup is being OOM-ed. It's read from the `memory.oom_control`
	// (V1) and the `memory.events` (V2) files.
	OOMKills uint64
	// UnderOOM is the current OOM status of a cgroup. This information is available for Cgroup V1 only. It's read
	// from the `memory.oom_control` file.
	UnderOOM bool
	// TotalAnon reflects the `rss` (V1) or `anon` (V2) of `memory.stat` file. That's amount of memory used in anonymous mappings
	TotalAnon uint64
	// ActiveAnon reflects the `active_file` of `memory.stat` file, anonymous and swap cache memory on active LRU list.
	TotalActiveAnon uint64
	// InactiveAnon reflects the `inactive_anon` of `memory.stat` file, anonymous and swap cache memory on inactive LRU list.
	TotalInactiveAnon uint64
	// TotalFile reflects the `cache` (V1) or `file` (V2) of `memory.stat` file, bytes of page cache memory.
	TotalFile uint64
	// TotalActiveFile reflects the `active_file` of `memory.stat` file, bytes of file-backed memory that cannot be reclaimed.
	TotalActiveFile uint64
	// TotalInactiveFile reflects the `inactive_file` of `memory.stat` file, bytes of file-backed memory on inactive LRU list.
	TotalInactiveFile uint64

	// PSI Memory Pressure from `memory.pressure` file (cgroups v2 only).
	MemoryPSI PSIMetrics
	// PSI IO Pressure from `io.pressure` file (cgroups v2 only).
	IOPSI PSIMetrics
	// PSI CPU Pressure from `cpu.pressure` (cgroups v2 only, "Some" only).
	CPUPSI PSIMetrics

	// Memory Events from `memory.events` file (cgroups v2 only).
	// These are counters that increment each time certain memory events occur.

	// MemoryHighEvents is the number of times the cgroup's memory usage exceeded memory.high.
	// A rising count indicates the cgroup is being throttled.
	MemoryHighEvents uint64
	// MemoryMaxEvents is the number of times the cgroup's memory usage was about to exceed memory.max.
	// This indicates how close we are to triggering OOM.
	MemoryMaxEvents uint64
	// MemoryOOMEvents is the number of times the OOM killer was invoked in this cgroup.
	// Note: This may differ from OOMKills which counts actual kills.
	MemoryOOMEvents uint64

	// PgFault is the total number of page faults incurred.
	PgFault uint64
	// PgMajFault is the number of major page faults incurred (required disk IO).
	// High values indicate cache misses causing disk reads - a key indicator of cache pressure.
	PgMajFault uint64
}

// Stats stores statistics of all cgroups managed by a manager
type Stats struct {
	// ParentStats stores the statistics of the parent cgroup. There should be
	// an array of per-repository cgroups, but we haven't used that information
	// yet.
	ParentStats CgroupStats
}

// AddCommandOption is an option that can be passed to AddCommand.
type AddCommandOption func(*addCommandCfg)

// WithCgroupKey overrides the key used to derive the Cgroup bucket. If not passed, then the command
// arguments will be used as the cgroup key.
func WithCgroupKey(cgroupKey string) AddCommandOption {
	return func(cfg *addCommandCfg) {
		cfg.cgroupKey = cgroupKey
	}
}

// Manager supplies an interface for interacting with cgroups
type Manager interface {
	// Setup creates cgroups and assigns configured limitations.
	// It is expected to be called once at Gitaly startup from any
	// instance of the Manager.
	Setup() error
	// Ready returns true if this cgroup manager is configured and
	// ready to use.
	Ready() bool
	// AddCommand adds a Cmd to a cgroup.
	AddCommand(*exec.Cmd, ...AddCommandOption) (string, error)
	// SupportsCloneIntoCgroup returns whether this Manager supports the CloneIntoCgroup method.
	// CloneIntoCgroup requires CLONE_INTO_CGROUP which is only supported with cgroups version 2
	// with Linux 5.7 or newer.
	SupportsCloneIntoCgroup() bool
	// CloneIntoCgroup configures the cgroup parameters UseCgroupFD and CgroupFD in SysProcAttr
	// to start the command directly in the correct cgroup. On success, the function returns an io.Closer
	// that must be closed after the command has been started to close the cgroup's file descriptor.
	CloneIntoCgroup(*exec.Cmd, ...AddCommandOption) (string, io.Closer, error)
	// Stats returns cgroup accounting statistics collected by reading
	// cgroupfs files. Those statistics are generic for both Cgroup V1
	// and Cgroup V2.
	Stats() (Stats, error)
	Describe(ch chan<- *prometheus.Desc)
	Collect(ch chan<- prometheus.Metric)
}

// NewManager returns the appropriate Cgroups manager
func NewManager(cfg cgroups.Config, logger log.Logger, pid int) Manager {
	if cfg.Repositories.Count < 1 {
		if cfg.CPUShares > 0 || cfg.MemoryBytes > 0 {
			logger.Warn("cgroups repositories.count value was not configured or set to 0. Cgroup support will be disabled. Please set a positive value for repositories.count")
		}
		return &NoopManager{}
	}

	return newCgroupManager(cfg, logger, pid)
}

// StartPruningOldCgroups prunes old cgroups for both the memory and cpu subsystems
// in a goroutine.
func StartPruningOldCgroups(cfg cgroups.Config, logger log.Logger) {
	dontpanic.Go(logger, func() {
		pruneOldCgroups(cfg, logger)
	})
}
