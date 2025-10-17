//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/cgroups/v3/cgroup1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

type cgroupV1Handler struct {
	cfg       cgroupscfg.Config
	logger    log.Logger
	hierarchy func() ([]cgroup1.Subsystem, error)

	*cgroupsMetrics
	pid int
}

func newV1Handler(cfg cgroupscfg.Config, logger log.Logger, pid int) *cgroupV1Handler {
	return &cgroupV1Handler{
		cfg:    cfg,
		logger: logger,
		pid:    pid,
		hierarchy: func() ([]cgroup1.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
		cgroupsMetrics: newV1CgroupsMetrics(),
	}
}

func (cvh *cgroupV1Handler) setupParent(parentResources *specs.LinuxResources) error {
	if _, err := cgroup1.New(
		cgroup1.StaticPath(cvh.currentProcessCgroup()),
		parentResources,
		cgroup1.WithHierarchy(cvh.hierarchy),
	); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}
	return nil
}

func (cvh *cgroupV1Handler) createCgroup(reposResources *specs.LinuxResources, cgroupPath string) error {
	_, err := cgroup1.New(
		cgroup1.StaticPath(cgroupPath),
		reposResources,
		cgroup1.WithHierarchy(cvh.hierarchy),
	)

	return err
}

func (cvh *cgroupV1Handler) addToCgroup(pid int, cgroupPath string) error {
	control, err := cvh.loadCgroup(cgroupPath)
	if err != nil {
		return err
	}

	if err := control.Add(cgroup1.Process{Pid: pid}); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV1Handler) loadCgroup(cgroupPath string) (cgroup1.Cgroup, error) {
	control, err := cgroup1.Load(
		cgroup1.StaticPath(cgroupPath),
		cgroup1.WithHierarchy(cvh.hierarchy),
	)
	if err != nil {
		return nil, fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}
	return control, nil
}

func (cvh *cgroupV1Handler) collect(repoPath string, ch chan<- prometheus.Metric) {
	logger := cvh.logger.WithField("cgroup_path", repoPath)
	control, err := cvh.loadCgroup(repoPath)
	if err != nil {
		logger.WithError(err).Warn("unable to load cgroup controller")
		return
	}

	if metrics, err := control.Stat(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup stats")
	} else {
		memoryMetric := cvh.memoryReclaimAttemptsTotal.WithLabelValues(repoPath)
		memoryMetric.Set(float64(metrics.GetMemory().GetUsage().GetFailcnt()))
		ch <- memoryMetric

		cpuUserMetric := cvh.cpuUsage.WithLabelValues(repoPath, "user")
		cpuUserMetric.Set(float64(metrics.GetCPU().GetUsage().GetUser()))
		ch <- cpuUserMetric

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSPeriods,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetThrottling().GetPeriods()),
			repoPath,
		)

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSThrottledPeriods,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetThrottling().GetThrottledPeriods()),
			repoPath,
		)

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSThrottledTime,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetThrottling().GetThrottledTime())/float64(time.Second),
			repoPath,
		)

		cpuKernelMetric := cvh.cpuUsage.WithLabelValues(repoPath, "kernel")
		cpuKernelMetric.Set(float64(metrics.GetCPU().GetUsage().GetKernel()))
		ch <- cpuKernelMetric
	}

	if subsystems, err := cvh.hierarchy(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup hierarchy")
	} else {
		for _, subsystem := range subsystems {
			processes, err := control.Processes(subsystem.Name(), true)
			if err != nil {
				logger.WithField("subsystem", subsystem.Name()).
					WithError(err).
					Warn("unable to get process list")
				continue
			}

			procsMetric := cvh.procs.WithLabelValues(repoPath, string(subsystem.Name()))
			procsMetric.Set(float64(len(processes)))
			ch <- procsMetric
		}
	}
}

func (cvh *cgroupV1Handler) repoPath(groupID int) string {
	return filepath.Join(cvh.currentProcessCgroup(), fmt.Sprintf("repos-%d", groupID))
}

func (cvh *cgroupV1Handler) currentProcessCgroup() string {
	return config.GetGitalyProcessTempDir(cvh.cfg.HierarchyRoot, cvh.pid)
}

func (cvh *cgroupV1Handler) stats() (Stats, error) {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cvh.loadCgroup(processCgroupPath)
	if err != nil {
		return Stats{}, err
	}

	metrics, err := control.Stat()
	if err != nil {
		return Stats{}, fmt.Errorf("failed to fetch metrics %s: %w", processCgroupPath, err)
	}

	return Stats{
		ParentStats: CgroupStats{
			CPUThrottledCount:    metrics.GetCPU().GetThrottling().GetThrottledPeriods(),
			CPUThrottledDuration: float64(metrics.GetCPU().GetThrottling().GetThrottledTime()) / float64(time.Second),
			MemoryUsage:          metrics.GetMemory().GetUsage().GetUsage(),
			MemoryLimit:          metrics.GetMemory().GetUsage().GetLimit(),
			OOMKills:             metrics.GetMemoryOomControl().GetOomKill(),
			UnderOOM:             metrics.GetMemoryOomControl().GetUnderOom() != 0,
			TotalAnon:            metrics.GetMemory().GetTotalRSS(),
			TotalActiveAnon:      metrics.GetMemory().GetTotalActiveAnon(),
			TotalInactiveAnon:    metrics.GetMemory().GetTotalInactiveAnon(),
			TotalFile:            metrics.GetMemory().GetTotalCache(),
			TotalActiveFile:      metrics.GetMemory().GetTotalActiveFile(),
			TotalInactiveFile:    metrics.GetMemory().GetTotalInactiveFile(),
		},
	}, nil
}

func (cvh *cgroupV1Handler) supportsCloneIntoCgroup() bool {
	return false
}

func defaultSubsystems(root string) ([]cgroup1.Subsystem, error) {
	subsystems := []cgroup1.Subsystem{
		cgroup1.NewMemory(root, cgroup1.OptionalSwap()),
		cgroup1.NewCpu(root),
	}

	return subsystems, nil
}

func pruneOldCgroupsV1(cfg cgroupscfg.Config, logger log.Logger) {
	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, "memory",
			cfg.HierarchyRoot),
	); err != nil {
		logger.WithError(err).Error("failed to clean up memory cgroups")
	}

	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, "cpu",
			cfg.HierarchyRoot),
	); err != nil {
		logger.WithError(err).Error("failed to clean up cpu cgroups")
	}
}
