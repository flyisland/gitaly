package watchers

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const cgroupPressureWatcherName = "CgroupPressure"

// PressureResource identifies the cgroup resource type to monitor for PSI pressure.
type PressureResource string

// Supported PSI pressure resource types.
const (
	PressureResourceMemory PressureResource = "memory"
	PressureResourceIO     PressureResource = "io"
	PressureResourceCPU    PressureResource = "cpu"
)

// PSI severity levels.
const (
	PSISeverityCritical = "critical"
	PSISeverityBackoff  = "backoff"
	PSISeverityWarning  = "warning"
	PSIHealthy          = "healthy"
)

// CgroupPressureWatcher monitors cgroup-level PSI using a 3-condition check:
// sustained pressure + current at backoff threshold + not recovering. Currently log-only,
// it does not trigger backoff events. Logs at warning, backoff, and critical severity.
type CgroupPressureWatcher struct {
	manager  cgroups.Manager
	logger   log.Logger
	resource PressureResource

	warningThreshold  float64
	backoffThreshold  float64
	criticalThreshold float64
	sustainDuration   time.Duration
	fastFallRatio     float64

	hitThresholdAt time.Time
	lastPressure   cgroups.PSIData

	timeFunc func() time.Time
}

// NewCgroupPressureWatcher creates a new PSI pressure watcher for the given resource.
func NewCgroupPressureWatcher(
	manager cgroups.Manager,
	logger log.Logger,
	resource PressureResource,
	cfg config.PSIResourceConfig,
) *CgroupPressureWatcher {
	return &CgroupPressureWatcher{
		manager:           manager,
		logger:            logger,
		resource:          resource,
		warningThreshold:  cfg.WarningThreshold,
		backoffThreshold:  cfg.BackoffThreshold,
		criticalThreshold: cfg.CriticalThreshold,
		sustainDuration:   time.Duration(cfg.SustainDurationSeconds) * time.Second,
		fastFallRatio:     cfg.FastFallRatio,
	}
}

// Name returns the name of the watcher including the resource type.
func (w *CgroupPressureWatcher) Name() string {
	return fmt.Sprintf("%s/%s", cgroupPressureWatcherName, w.resource)
}

func (w *CgroupPressureWatcher) now() time.Time {
	if w.timeFunc != nil {
		return w.timeFunc()
	}
	return time.Now()
}

// Poll checks PSI pressure and logs at the appropriate severity.
func (w *CgroupPressureWatcher) Poll(_ context.Context) (*limiter.BackoffEvent, error) {
	noBackoff := &limiter.BackoffEvent{WatcherName: w.Name(), ShouldBackoff: false}

	if !w.manager.Ready() {
		return noBackoff, nil
	}

	stats, err := w.manager.Stats()
	if err != nil {
		return nil, fmt.Errorf("cgroup pressure watcher: poll stats: %w", err)
	}

	psi := w.getPSI(stats.ParentStats)
	defer func() { w.lastPressure = psi.Some }()

	severity := w.classifySeverity(psi.Some.Avg60)

	if severity == PSIHealthy {
		w.hitThresholdAt = time.Time{}
		return noBackoff, nil
	}

	now := w.now()

	if psi.Some.Avg60 >= w.backoffThreshold {
		if w.hitThresholdAt.IsZero() {
			w.hitThresholdAt = now
		}
	} else if psi.Some.Avg60 < w.warningThreshold {
		w.hitThresholdAt = time.Time{}
	}

	sustained := !w.hitThresholdAt.IsZero() && now.Sub(w.hitThresholdAt) >= w.sustainDuration
	aboveThreshold10s := psi.Some.Avg10 >= w.backoffThreshold

	fallingRapidly := false
	if w.lastPressure.Avg10 > 0 {
		fallingRapidly = psi.Some.Avg10 < w.lastPressure.Avg10*w.fastFallRatio
	}

	fields := w.buildStats(stats.ParentStats, sustained, aboveThreshold10s, fallingRapidly, severity)

	switch severity {
	case PSISeverityCritical:
		w.logger.WithFields(fields).Error("Critical PSI pressure detected")
	case PSISeverityBackoff:
		w.logger.WithFields(fields).Warn("PSI pressure at backoff threshold")
	case PSISeverityWarning:
		w.logger.WithFields(fields).Warn("PSI pressure above warning threshold")
	}

	return noBackoff, nil
}

func (w *CgroupPressureWatcher) getPSI(stats cgroups.CgroupStats) cgroups.PSIMetrics {
	switch w.resource {
	case PressureResourceIO:
		return stats.IOPSI
	case PressureResourceCPU:
		return stats.CPUPSI
	default:
		return stats.MemoryPSI
	}
}

func (w *CgroupPressureWatcher) classifySeverity(avg60 float64) string {
	switch {
	case avg60 >= w.criticalThreshold:
		return PSISeverityCritical
	case avg60 >= w.backoffThreshold:
		return PSISeverityBackoff
	case avg60 >= w.warningThreshold:
		return PSISeverityWarning
	default:
		return PSIHealthy
	}
}

func (w *CgroupPressureWatcher) buildStats(
	stats cgroups.CgroupStats,
	sustained bool,
	aboveThreshold10s bool,
	fallingRapidly bool,
	severity string,
) map[string]any {
	m := buildBackoffStats(stats)
	m["psi_resource"] = string(w.resource)
	m["psi_severity"] = severity
	m["psi_threshold_backoff"] = w.backoffThreshold
	m["psi_threshold_critical"] = w.criticalThreshold
	m["psi_sustained"] = sustained
	m["psi_above_10s"] = aboveThreshold10s
	m["psi_falling_rapidly"] = fallingRapidly

	if !w.hitThresholdAt.IsZero() {
		m["psi_sustained_seconds"] = w.now().Sub(w.hitThresholdAt).Seconds()
	}

	return m
}
