package burdenmonitor

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
)

// psiResource identifies the cgroup resource a PSI condition watches.
type psiResource string

const (
	psiResourceCPU    psiResource = "cpu"
	psiResourceMemory psiResource = "memory"
	psiResourceIO     psiResource = "io"
)

// Event names emitted by the LoadShedder's conditions. These appear in the
// Description field of the loadmonitor.Event.
const (
	eventLoadShedderPSICritical = "loadshedder_psi_critical"
	eventLoadShedderOOMKill     = "loadshedder_oom_kill"
)

// newPSICriticalCondition returns a Condition that fires when the cgroup's
// some.avg60 PSI for the given resource is at or above the configured
// critical threshold. A disabled or unconfigured resource produces a no-op
// Condition that never fires.
func newPSICriticalCondition(resource psiResource, cfg config.PSIResourceConfig) loadmonitor.Condition {
	name := fmt.Sprintf("LoadShedderPSI/%s", resource)

	if !cfg.Enabled || cfg.CriticalThreshold <= 0 {
		return loadmonitor.Condition{
			Name: name,
			Fn: func(_ context.Context, _, _ loadmonitor.Stats, _ time.Duration) (bool, string) {
				return false, ""
			},
		}
	}

	return loadmonitor.Condition{
		Name: name,
		Fn: func(_ context.Context, _, current loadmonitor.Stats, _ time.Duration) (bool, string) {
			psi := psiFor(resource, current.CGroup.ParentStats)
			if psi.Some.Avg60 >= cfg.CriticalThreshold {
				return true, eventLoadShedderPSICritical
			}
			return false, ""
		},
	}
}

// newOOMKillCondition returns a Condition that fires when the cgroup's
// cumulative OOM-kill counter has increased since the previous poll.
func newOOMKillCondition() loadmonitor.Condition {
	return loadmonitor.Condition{
		Name: "LoadShedderOOMKill",
		Fn: func(_ context.Context, previous, current loadmonitor.Stats, _ time.Duration) (bool, string) {
			if current.CGroup.ParentStats.OOMKills > previous.CGroup.ParentStats.OOMKills {
				return true, eventLoadShedderOOMKill
			}
			return false, ""
		},
	}
}

func psiFor(resource psiResource, stats cgroups.CgroupStats) cgroups.PSIMetrics {
	switch resource {
	case psiResourceCPU:
		return stats.CPUPSI
	case psiResourceMemory:
		return stats.MemoryPSI
	case psiResourceIO:
		return stats.IOPSI
	}
	return cgroups.PSIMetrics{}
}
