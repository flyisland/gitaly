//go:build linux

package cgroups

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/cgroups/v3/cgroup2"
	stats2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/kernel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// supportsCloneIntoCgroup performs two checks to determine whether the running system allows
// a process to be forked directly into an arbitrary cgroup. Firstly, the kernel must be new
// enough to support this. Secondly, Gitaly must have permission to execute the clone3 syscall.
//
// The second factor has historically been problematic in containerised environments, where a
// restrictive seccomp profile blocks the clone3(2) syscall. We need to detect these cases so we
// can gracefully fall back to using clone(2) and moving the process into the cgroup _after_ it's
// been started.
func supportsCloneIntoCgroup(cfg cgroupscfg.Config) (bool, error) {
	kernelSupported, err := kernel.IsAtLeast(kernel.Version{Major: 5, Minor: 7})
	if err != nil {
		return false, fmt.Errorf("detect kernel version: %w", err)
	}

	if !kernelSupported {
		return false, nil
	}

	// Use the root of the Gitaly hierarchy, since we don't want to worry about allocating
	// a repository cgroup.
	cgroupDirPath := filepath.Join(cfg.Mountpoint, cfg.HierarchyRoot)
	dir, err := os.Open(cgroupDirPath)
	if err != nil {
		return false, fmt.Errorf("open cgroup directory: %w", err)
	}

	defer func() {
		_ = dir.Close()
	}()

	// The simplest command imaginable. The command setup is similar to what we do in
	// CGroupManager.CloneIntoCgroup().
	cmd := exec.CommandContext(context.Background(), "/bin/true")
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(dir.Fd())

	if err := cmd.Start(); err != nil {
		// The "function not implemented" error is the signal that something is blocking
		// us from invoking clone3(2).
		if errors.Is(err, syscall.ENOSYS) {
			return false, nil
		}

		return false, fmt.Errorf("exec command in cgroup: %w", err)
	}

	return true, cmd.Wait()
}

type cgroupV2Handler struct {
	cfg    cgroupscfg.Config
	logger log.Logger

	*cgroupsMetrics
	pid             int
	cloneIntoCgroup bool
}

func newV2Handler(cfg cgroupscfg.Config, logger log.Logger, pid int) *cgroupV2Handler {
	cloneIntoCgroup, err := supportsCloneIntoCgroup(cfg)
	if err != nil {
		logger.WithError(err).Error("failed to check cloneIntoCgroup support. Cloning directly into a cgroup will be disabled.")
	}

	return &cgroupV2Handler{
		cfg:             cfg,
		logger:          logger,
		pid:             pid,
		cgroupsMetrics:  newV2CgroupsMetrics(),
		cloneIntoCgroup: cloneIntoCgroup,
	}
}

func (cvh *cgroupV2Handler) setupParent(parentResources *specs.LinuxResources) error {
	if _, err := cgroup2.NewManager(cvh.cfg.Mountpoint, "/"+cvh.currentProcessCgroup(), cgroup2.ToResources(parentResources)); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV2Handler) createCgroup(reposResources *specs.LinuxResources, cgroupPath string) error {
	_, err := cgroup2.NewManager(
		cvh.cfg.Mountpoint,
		"/"+cgroupPath,
		cgroup2.ToResources(reposResources),
	)

	return err
}

func (cvh *cgroupV2Handler) addToCgroup(pid int, cgroupPath string) error {
	control, err := cvh.loadCgroup(cgroupPath)
	if err != nil {
		return err
	}

	if err := control.AddProc(uint64(pid)); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV2Handler) loadCgroup(cgroupPath string) (*cgroup2.Manager, error) {
	control, err := cgroup2.Load("/"+cgroupPath, cgroup2.WithMountpoint(cvh.cfg.Mountpoint))
	if err != nil {
		return nil, fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}
	return control, nil
}

func (cvh *cgroupV2Handler) collect(repoPath string, ch chan<- prometheus.Metric) {
	logger := cvh.logger.WithField("cgroup_path", repoPath)
	control, err := cvh.loadCgroup(repoPath)
	if err != nil {
		logger.WithError(err).Warn("unable to load cgroup controller")
		return
	}

	if metrics, err := control.Stat(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup stats")
	} else {
		cpuUserMetric := cvh.cpuUsage.WithLabelValues(repoPath, "user")
		cpuUserMetric.Set(float64(metrics.GetCPU().GetUserUsec()))
		ch <- cpuUserMetric

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSPeriods,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetNrPeriods()),
			repoPath,
		)

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSThrottledPeriods,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetNrThrottled()),
			repoPath,
		)

		ch <- prometheus.MustNewConstMetric(
			cvh.cpuCFSThrottledTime,
			prometheus.CounterValue,
			float64(metrics.GetCPU().GetThrottledUsec())/float64(time.Second),
			repoPath,
		)

		cpuKernelMetric := cvh.cpuUsage.WithLabelValues(repoPath, "kernel")
		cpuKernelMetric.Set(float64(metrics.GetCPU().GetSystemUsec()))
		ch <- cpuKernelMetric

		// PSI Memory Pressure metrics
		if memPressure := metrics.GetMemory().GetPSI(); memPressure != nil {
			if some := memPressure.GetSome(); some != nil {
				memPressureSomeAvg10 := cvh.memoryPressure.WithLabelValues(repoPath, "some", "avg10")
				memPressureSomeAvg10.Set(some.GetAvg10())
				ch <- memPressureSomeAvg10

				memPressureSomeAvg60 := cvh.memoryPressure.WithLabelValues(repoPath, "some", "avg60")
				memPressureSomeAvg60.Set(some.GetAvg60())
				ch <- memPressureSomeAvg60

				memPressureSomeAvg300 := cvh.memoryPressure.WithLabelValues(repoPath, "some", "avg300")
				memPressureSomeAvg300.Set(some.GetAvg300())
				ch <- memPressureSomeAvg300
			}
			if full := memPressure.GetFull(); full != nil {
				memPressureFullAvg10 := cvh.memoryPressure.WithLabelValues(repoPath, "full", "avg10")
				memPressureFullAvg10.Set(full.GetAvg10())
				ch <- memPressureFullAvg10

				memPressureFullAvg60 := cvh.memoryPressure.WithLabelValues(repoPath, "full", "avg60")
				memPressureFullAvg60.Set(full.GetAvg60())
				ch <- memPressureFullAvg60

				memPressureFullAvg300 := cvh.memoryPressure.WithLabelValues(repoPath, "full", "avg300")
				memPressureFullAvg300.Set(full.GetAvg300())
				ch <- memPressureFullAvg300
			}
		}

		// PSI IO Pressure metrics
		if ioPressure := metrics.GetIo().GetPSI(); ioPressure != nil {
			if some := ioPressure.GetSome(); some != nil {
				ioPressureSomeAvg10 := cvh.ioPressure.WithLabelValues(repoPath, "some", "avg10")
				ioPressureSomeAvg10.Set(some.GetAvg10())
				ch <- ioPressureSomeAvg10

				ioPressureSomeAvg60 := cvh.ioPressure.WithLabelValues(repoPath, "some", "avg60")
				ioPressureSomeAvg60.Set(some.GetAvg60())
				ch <- ioPressureSomeAvg60

				ioPressureSomeAvg300 := cvh.ioPressure.WithLabelValues(repoPath, "some", "avg300")
				ioPressureSomeAvg300.Set(some.GetAvg300())
				ch <- ioPressureSomeAvg300
			}
			if full := ioPressure.GetFull(); full != nil {
				ioPressureFullAvg10 := cvh.ioPressure.WithLabelValues(repoPath, "full", "avg10")
				ioPressureFullAvg10.Set(full.GetAvg10())
				ch <- ioPressureFullAvg10

				ioPressureFullAvg60 := cvh.ioPressure.WithLabelValues(repoPath, "full", "avg60")
				ioPressureFullAvg60.Set(full.GetAvg60())
				ch <- ioPressureFullAvg60

				ioPressureFullAvg300 := cvh.ioPressure.WithLabelValues(repoPath, "full", "avg300")
				ioPressureFullAvg300.Set(full.GetAvg300())
				ch <- ioPressureFullAvg300
			}
		}

		if cpuPressure := metrics.GetCPU().GetPSI(); cpuPressure != nil {
			if some := cpuPressure.GetSome(); some != nil {
				cpuPressureSomeAvg10 := cvh.cpuPressure.WithLabelValues(repoPath, "some", "avg10")
				cpuPressureSomeAvg10.Set(some.GetAvg10())
				ch <- cpuPressureSomeAvg10

				cpuPressureSomeAvg60 := cvh.cpuPressure.WithLabelValues(repoPath, "some", "avg60")
				cpuPressureSomeAvg60.Set(some.GetAvg60())
				ch <- cpuPressureSomeAvg60

				cpuPressureSomeAvg300 := cvh.cpuPressure.WithLabelValues(repoPath, "some", "avg300")
				cpuPressureSomeAvg300.Set(some.GetAvg300())
				ch <- cpuPressureSomeAvg300
			}
		}

		// Memory events metrics
		if memEvents := metrics.GetMemoryEvents(); memEvents != nil {
			ch <- prometheus.MustNewConstMetric(
				cvh.memoryEventsHigh,
				prometheus.CounterValue,
				float64(memEvents.GetHigh()),
				repoPath,
			)
			ch <- prometheus.MustNewConstMetric(
				cvh.memoryEventsMax,
				prometheus.CounterValue,
				float64(memEvents.GetMax()),
				repoPath,
			)
			ch <- prometheus.MustNewConstMetric(
				cvh.memoryEventsOOM,
				prometheus.CounterValue,
				float64(memEvents.GetOom()),
				repoPath,
			)
		}

		// Page fault metrics
		ch <- prometheus.MustNewConstMetric(
			cvh.pgFault,
			prometheus.CounterValue,
			float64(metrics.GetMemory().GetPgfault()),
			repoPath,
		)
		ch <- prometheus.MustNewConstMetric(
			cvh.pgMajFault,
			prometheus.CounterValue,
			float64(metrics.GetMemory().GetPgmajfault()),
			repoPath,
		)
	}

	if subsystems, err := control.Controllers(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup hierarchy")
	} else {
		processes, err := control.Procs(true)
		if err != nil {
			logger.WithError(err).
				Warn("unable to get process list")
			return
		}

		for _, subsystem := range subsystems {
			procsMetric := cvh.procs.WithLabelValues(repoPath, subsystem)
			procsMetric.Set(float64(len(processes)))
			ch <- procsMetric
		}
	}
}

func (cvh *cgroupV2Handler) repoPath(groupID int) string {
	return filepath.Join(cvh.currentProcessCgroup(), fmt.Sprintf("repos-%d", groupID))
}

func (cvh *cgroupV2Handler) currentProcessCgroup() string {
	return config.GetGitalyProcessTempDir(cvh.cfg.HierarchyRoot, cvh.pid)
}

func (cvh *cgroupV2Handler) stats(path string) (CgroupStats, error) {
	control, err := cvh.loadCgroup(path)
	if err != nil {
		return CgroupStats{}, err
	}

	metrics, err := control.Stat()
	if err != nil {
		return CgroupStats{}, fmt.Errorf("failed to fetch metrics %s: %w", path, err)
	}

	stats := CgroupStats{
		Path:                 path,
		CPUThrottledCount:    metrics.GetCPU().GetNrThrottled(),
		CPUThrottledDuration: float64(metrics.GetCPU().GetThrottledUsec()) / float64(time.Second),
		MemoryUsage:          metrics.GetMemory().GetUsage(),
		MemoryLimit:          metrics.GetMemory().GetUsageLimit(),
		// memory.stat breaks down the cgroup's memory footprint into different types of memory. In
		// Cgroup V2, this file includes the consumption of the cgroup's entire subtree. Total_* stats
		// were removed.
		TotalAnon:         metrics.GetMemory().GetAnon(),
		TotalActiveAnon:   metrics.GetMemory().GetActiveAnon(),
		TotalInactiveAnon: metrics.GetMemory().GetInactiveAnon(),
		TotalFile:         metrics.GetMemory().GetFile(),
		TotalActiveFile:   metrics.GetMemory().GetActiveFile(),
		TotalInactiveFile: metrics.GetMemory().GetInactiveFile(),
		PgFault:           metrics.GetMemory().GetPgfault(),
		PgMajFault:        metrics.GetMemory().GetPgmajfault(),
	}

	if memEvents := metrics.GetMemoryEvents(); memEvents != nil {
		stats.OOMKills = memEvents.GetOomKill()
		stats.MemoryHighEvents = memEvents.GetHigh()
		stats.MemoryMaxEvents = memEvents.GetMax()
		stats.MemoryOOMEvents = memEvents.GetOom()
	}

	setPSI := func(psim *PSIMetrics, psiStats *stats2.PSIStats) {
		if psiStats != nil {
			if some := psiStats.GetSome(); some != nil {
				psim.Some.Avg10 = some.GetAvg10()
				psim.Some.Avg60 = some.GetAvg60()
				psim.Some.Avg300 = some.GetAvg300()
				psim.Some.Total = some.GetTotal()
			}
			if full := psiStats.GetFull(); full != nil {
				psim.Full.Avg10 = full.GetAvg10()
				psim.Full.Avg60 = full.GetAvg60()
				psim.Full.Avg300 = full.GetAvg300()
				psim.Full.Total = full.GetTotal()
			}
		}
	}

	setPSI(&stats.MemoryPSI, metrics.GetMemory().GetPSI())
	setPSI(&stats.IOPSI, metrics.GetIo().GetPSI())
	setPSI(&stats.CPUPSI, metrics.GetCPU().GetPSI())
	return stats, nil
}

func (cvh *cgroupV2Handler) supportsCloneIntoCgroup() bool {
	return cvh.cloneIntoCgroup
}

func pruneOldCgroupsV2(cfg cgroupscfg.Config, logger log.Logger) {
	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, cfg.HierarchyRoot),
	); err != nil {
		var pathError *fs.PathError
		if !errors.As(err, &pathError) {
			logger.WithError(err).Error("failed to clean up cpu cgroups")
		}
	}
}
