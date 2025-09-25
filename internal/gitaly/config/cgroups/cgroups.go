package cgroups

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
)

// Config is a struct for cgroups config
type Config struct {
	// Mountpoint is where the cgroup filesystem is mounted, usually under /sys/fs/cgroup/
	Mountpoint string `toml:"mountpoint"`
	// HierarchyRoot is the parent cgroup under which Gitaly creates <Count> of cgroups.
	// A system administrator is expected to create such cgroup/directory under <Mountpoint>/memory
	// and/or <Mountpoint>/cpu depending on which resource is enabled. HierarchyRoot is expected to
	// be owned by the user and group Gitaly runs as.
	HierarchyRoot string       `toml:"hierarchy_root"`
	Repositories  Repositories `toml:"repositories"`
	// MemoryBytes is the memory limit for the parent cgroup. 0 implies no memory limit.
	MemoryBytes int64 `toml:"memory_bytes"`
	// CPUShares are the shares of CPU the parent cgroup is allowed to utilize. A value of 1024
	// is full utilization of the CPU. 0 implies no CPU limit.
	CPUShares uint64 `toml:"cpu_shares"`
	// CPUQuotaUs sets cpu_quota_us for the parent cgroup
	// https://docs.kernel.org/scheduler/sched-bwc.html?highlight=cfs_period_us#management
	// below 0 implies no quota
	//
	// The cfs_period_us is hardcoded to 100ms
	CPUQuotaUs     int64 `toml:"cpu_quota_us"`
	MetricsEnabled bool  `toml:"metrics_enabled"`

	// Deprecated: No longer supported after 15.0
	Count  uint   `toml:"count"`
	CPU    CPU    `toml:"cpu"`
	Memory Memory `toml:"memory"`
}

// FallbackToOldVersion translates the old format of cgroups into the new
// format.
func (c *Config) FallbackToOldVersion() {
	if c.Repositories.Count == 0 {
		c.Repositories.Count = c.Count

		if c.Repositories.MemoryBytes == 0 && c.Memory.Enabled {
			c.Repositories.MemoryBytes = c.Memory.Limit
		}

		if c.Repositories.CPUShares == 0 && c.CPU.Enabled {
			c.Repositories.CPUShares = c.CPU.Shares
		}
	}
}

// Validate runs validation on all fields and compose all found errors.
func (c *Config) Validate() error {
	return cfgerror.New().
		Append(c.Repositories.Validate(c.MemoryBytes, c.CPUShares, c.CPUQuotaUs), "repositories").
		AsError()
}

// Repositories configures cgroups to be created that are isolated by repository.
type Repositories struct {
	// Count is the number of cgroups that will be created for repository-level isolation
	// of git commands.
	Count uint `toml:"count"`
	// MaxCgroupsPerRepo specifies the maximum number of cgroups to which a single repository can allocate its
	// processes.
	// By default, a repository can spawn processes in at most one cgroups. If the number of repositories
	// is more than the number of cgroups (likely), multiple repositories share the same one. This model works very
	// well if the repositories under the management of Cgroup are equivalent in size or traffic. If a node has some
	// enormous repositories (mono-repo, for example), the scoping cgroups become excessively large comparing to the
	// rest. This imbalance situation might force the operators to lift the repository-level cgroups. As a result,
	// the isolation effect is not effective.
	// This config is designed to balance resource usage between cgroups, mitigate competition for resources
	// within a single cgroup, and enhance memory usage efficiency and isolation. The value can be adjusted based on
	// the specific workload and number of repository cgroups on the node.
	// A Git process uses its target repository's relative path as the hash key to find the corresponding cgroup. It
	// is allocated randomly to any of the consequent MaxCgroupsPerRepo cgroups. It wraps around if needed.
	//                repo-X
	//               ┌───────┐
	// □ □ □ □ □ □ □ ■ ■ ■ ■ ■ ■ ■ ■ □ □ □ □
	//                     └────────┘
	//                      repo-Y
	// The default value is "1".
	MaxCgroupsPerRepo uint `toml:"max_cgroups_per_repo"`
	// MemoryBytes is the memory limit for each cgroup. 0 implies no memory limit.
	MemoryBytes int64 `toml:"memory_bytes"`
	// CPUShares are the shares of CPU that each cgroup is allowed to utilize. A value of 1024
	// is full utilization of the CPU. 0 implies no CPU limit.
	CPUShares uint64 `toml:"cpu_shares"`
	// CPUQuotaUs sets the value of `cfs_quota_us` for the repository cgroup.
	// https://docs.kernel.org/scheduler/sched-bwc.html?highlight=cfs_period_us
	// Below 0 implies no quota
	//
	// The cfs_period_us is hardcoded to 100ms
	CPUQuotaUs int64 `toml:"cpu_quota_us"`
}

// Validate runs validation on all fields and compose all found errors.
func (r *Repositories) Validate(memBytes int64, cpuShares uint64, cpuQuotaUs int64) error {
	errs := cfgerror.New().
		Append(cfgerror.InRange(0, r.Count, r.MaxCgroupsPerRepo, cfgerror.InRangeOptIncludeMin, cfgerror.InRangeOptIncludeMax), "max_cgroups_per_repo").
		Append(cfgerror.IsNaturalNumber(memBytes), "memory_bytes").
		Append(cfgerror.IsNaturalNumber(cpuQuotaUs), "cpu_quota_us")

	// 0 implies no limit, so we only validate the range if the user explicitly set the value.
	if memBytes > 0 {
		errs = errs.Append(cfgerror.InRange(0, memBytes, r.MemoryBytes, cfgerror.InRangeOptIncludeMin, cfgerror.InRangeOptIncludeMax), "memory_bytes")
	}
	if cpuShares > 0 {
		errs = errs.Append(cfgerror.InRange(0, cpuShares, r.CPUShares, cfgerror.InRangeOptIncludeMin, cfgerror.InRangeOptIncludeMax), "cpu_shares")
	}
	if cpuQuotaUs > 0 {
		errs = errs.Append(cfgerror.InRange(0, cpuQuotaUs, r.CPUQuotaUs, cfgerror.InRangeOptIncludeMin, cfgerror.InRangeOptIncludeMax), "cpu_quota_us")
	}

	return errs.AsError()
}

// Memory is a struct storing cgroups memory config
// Deprecated: Not in use after 15.0.
type Memory struct {
	Enabled bool `toml:"enabled"`
	// Limit is the memory limit in bytes. Could be -1 to indicate unlimited memory.
	Limit int64 `toml:"limit"`
}

// CPU is a struct storing cgroups CPU config
// Deprecated: Not in use after 15.0.
type CPU struct {
	Enabled bool `toml:"enabled"`
	// Shares is the number of CPU shares (relative weight (ratio) vs. other cgroups with CPU shares).
	Shares uint64 `toml:"shares"`
}
