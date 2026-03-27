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
