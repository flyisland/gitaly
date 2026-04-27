package limiter

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
)

const (
	conditionCgroupMemory = "CgroupMemory"

	eventMemoryOOM  = "condition_memory_oom"
	eventMemoryAnon = "condition_memory_anon"

	defaultMemoryThreshold = 0.6
)

// newMemoryUsageCondition evaluates to `true` in 2 conditions:
// * The current memory usage exceeds a soft threshold.
// * The cgroup is under OOM.
func newMemoryUsageCondition(threshold float64) loadmonitor.Condition {
	if threshold <= 0.0 {
		threshold = defaultMemoryThreshold
	}

	return loadmonitor.Condition{
		Name: conditionCgroupMemory,
		Fn: func(ctx context.Context, previous, current loadmonitor.Stats, pollInterval time.Duration) (bool, string) {
			if current.CGroup.ParentStats.UnderOOM {
				return true, eventMemoryOOM
			}

			// This check is to avoid a division by 0 below
			if current.CGroup.ParentStats.MemoryLimit == 0 {
				return false, ""
			}

			anonRatio := float64(current.CGroup.ParentStats.TotalAnon) / float64(current.CGroup.ParentStats.MemoryLimit)
			if anonRatio >= threshold {
				return true, eventMemoryAnon
			}
			return false, ""
		},
	}
}
