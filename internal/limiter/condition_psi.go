package limiter

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	conditionCgroupPSI = "CgroupPressure"

	eventPSICritical = "condition_psi_critical"
	eventPSIBackoff  = "condition_psi_backoff"
)

// PressureResource identifies the cgroup resource type to monitor for PSI pressure.
type PressureResource string

// Supported PSI pressure resource types.
const (
	pressureResourceMemory PressureResource = "memory"
	pressureResourceIO     PressureResource = "io"
	pressureResourceCPU    PressureResource = "cpu"
)

// PSI severity levels.
const (
	pSISeverityCritical = "critical"
	pSISeverityBackoff  = "backoff"
	pSISeverityWarning  = "warning"
	pSIHealthy          = "healthy"
)

// CgroupPSIConditionBuilder builds a Condition to emit events when PSI data
// shows the cgroup is under pressure, based on some configurable threshold.
type CgroupPSIConditionBuilder struct {
	logger          log.Logger
	resource        PressureResource
	cfg             config.PSIResourceConfig
	sustainDuration time.Duration
	hitThresholdAt  time.Time
	timeFunc        func() time.Time
}

// newCgroupPressureConditionBuilder creates a builder for a new PSI condition
func newCgroupPressureConditionBuilder(cfg config.PSIResourceConfig, logger log.Logger, resource PressureResource) *CgroupPSIConditionBuilder {
	return &CgroupPSIConditionBuilder{
		logger:          logger,
		resource:        resource,
		cfg:             cfg,
		sustainDuration: time.Duration(cfg.SustainDurationSeconds) * time.Second,
	}
}

// Condition returns the condition
func (b *CgroupPSIConditionBuilder) Condition() loadmonitor.Condition {
	noop := func(_ context.Context, _, _ loadmonitor.Stats, _ time.Duration) (bool, string) {
		return false, ""
	}

	c := loadmonitor.Condition{
		Name: conditionCgroupPSI,
		Fn:   noop,
	}

	if b.cfg.Enabled {
		c.Fn = b.fn
	}

	return c
}

// Name returns the name of the watcher including the resource type.
func (b *CgroupPSIConditionBuilder) Name() string {
	return fmt.Sprintf("%s/%s", conditionCgroupPSI, b.resource)
}

func (b *CgroupPSIConditionBuilder) now() time.Time {
	if b.timeFunc != nil {
		return b.timeFunc()
	}
	return time.Now()
}

// Poll checks PSI pressure and logs at the appropriate severity.
func (b *CgroupPSIConditionBuilder) fn(_ context.Context, previous, current loadmonitor.Stats, _ time.Duration) (bool, string) {
	currentPsi := b.getPSI(current.CGroup.ParentStats)
	previousPsi := b.getPSI(previous.CGroup.ParentStats)
	severity := b.classifySeverity(currentPsi.Some.Avg60)

	if severity == pSIHealthy {
		b.hitThresholdAt = time.Time{}
		return false, ""
	}

	now := b.now()

	if currentPsi.Some.Avg60 >= b.cfg.BackoffThreshold {
		if b.hitThresholdAt.IsZero() {
			b.hitThresholdAt = now
		}
	} else if currentPsi.Some.Avg60 < b.cfg.WarningThreshold {
		b.hitThresholdAt = time.Time{}
	}

	sustained := !b.hitThresholdAt.IsZero() && now.Sub(b.hitThresholdAt) >= b.sustainDuration
	aboveThreshold10s := currentPsi.Some.Avg10 >= b.cfg.BackoffThreshold

	fallingRapidly := false
	if previousPsi.Some.Avg10 > 0 {
		fallingRapidly = currentPsi.Some.Avg10 < previousPsi.Some.Avg10*b.cfg.FastFallRatio
	}

	fields := b.buildStats(previous, current, sustained, aboveThreshold10s, fallingRapidly, severity)

	switch severity {
	case pSISeverityCritical:
		b.logger.WithFields(fields).Error("Critical PSI pressure detected")
		return true, eventPSICritical
	case pSISeverityBackoff:
		b.logger.WithFields(fields).Warn("PSI pressure at backoff threshold")
		return true, eventPSIBackoff
	case pSISeverityWarning:
		b.logger.WithFields(fields).Warn("PSI pressure above warning threshold")
	}

	return false, ""
}

func (b *CgroupPSIConditionBuilder) getPSI(stats cgroups.CgroupStats) cgroups.PSIMetrics {
	switch b.resource {
	case pressureResourceIO:
		return stats.IOPSI
	case pressureResourceCPU:
		return stats.CPUPSI
	default:
		return stats.MemoryPSI
	}
}

func (b *CgroupPSIConditionBuilder) classifySeverity(avg60 float64) string {
	switch {
	case avg60 >= b.cfg.CriticalThreshold:
		return pSISeverityCritical
	case avg60 >= b.cfg.BackoffThreshold:
		return pSISeverityBackoff
	case avg60 >= b.cfg.WarningThreshold:
		return pSISeverityWarning
	default:
		return pSIHealthy
	}
}

// buildStats builds the stats to be logged for this Condition.
func (b *CgroupPSIConditionBuilder) buildStats(previous, current loadmonitor.Stats, sustained bool, aboveThreshold10s bool, fallingRapidly bool, severity string) map[string]any {
	// Currently, the PSI Conditions runs in dry-run mode.
	// So we create a fake BackoffEvent in order to benefit from getting
	// the stats from `buildBackoffStats`.
	// Eventually, this will be called from the adaptative calculator
	// and wouldn't need this method.
	m := buildBackoffStats(Config{}, BackoffEvent{
		ConditionName: "",
		Reason:        "",
		ShouldBackoff: false,
		PreviousStats: previous,
		CurrentStats:  current,
	})
	m["psi_resource"] = string(b.resource)
	m["psi_severity"] = severity
	m["psi_threshold_backoff"] = b.cfg.BackoffThreshold
	m["psi_threshold_critical"] = b.cfg.CriticalThreshold
	m["psi_sustained"] = sustained
	m["psi_above_10s"] = aboveThreshold10s
	m["psi_falling_rapidly"] = fallingRapidly

	if !b.hitThresholdAt.IsZero() {
		m["psi_sustained_seconds"] = b.now().Sub(b.hitThresholdAt).Seconds()
	}
	return m
}
