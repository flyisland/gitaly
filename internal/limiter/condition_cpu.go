package limiter

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
)

const (
	conditionCgroupCPU            = "CgroupCpu"
	eventCPUThrottling            = "condition_cpu_throttling"
	defaultCPUThrottlingThreshold = 0.5
)

// newCPUThrottlingCondition returns a Condition that evaluates to true when CPU throttling occurred for more than 50% of the
// time between the last 2 polls.
func newCPUThrottlingCondition(threshold float64) loadmonitor.Condition {
	if threshold <= 0.0 {
		threshold = defaultCPUThrottlingThreshold
	}

	return loadmonitor.Condition{
		Name: conditionCgroupCPU,
		Fn: func(ctx context.Context, previous, current loadmonitor.Stats, pollInterval time.Duration) (bool, string) {
			cur, prev := current.CGroup.ParentStats, previous.CGroup.ParentStats

			// Somehow, cgroup stats are reset. It's usually the consequence of cgroup limits being changed.
			// Alternatively, they can be overridden by another program.
			// Either way, the watcher should update the stats accordingly.
			if cur.CPUThrottledCount < prev.CPUThrottledCount || cur.CPUThrottledDuration < prev.CPUThrottledDuration {
				return false, ""
			}

			throttledDuration := cur.CPUThrottledDuration - prev.CPUThrottledDuration

			// If the total throttled duration since the last poll exceeds 50%.
			if pollInterval > 0 && throttledDuration/pollInterval.Seconds() > threshold {
				return true, eventCPUThrottling
			}
			return false, ""
		},
	}
}
