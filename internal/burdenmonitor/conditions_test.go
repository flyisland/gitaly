package burdenmonitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func statsWithPSI(resource psiResource, avg60 float64) loadmonitor.Stats {
	psi := cgroups.PSIMetrics{Some: cgroups.PSIData{Avg60: avg60}}
	cs := cgroups.CgroupStats{}
	switch resource {
	case psiResourceCPU:
		cs.CPUPSI = psi
	case psiResourceMemory:
		cs.MemoryPSI = psi
	case psiResourceIO:
		cs.IOPSI = psi
	}
	return loadmonitor.Stats{CGroup: cgroups.Stats{ParentStats: cs}}
}

func statsWithOOMKills(count uint64) loadmonitor.Stats {
	return loadmonitor.Stats{CGroup: cgroups.Stats{
		ParentStats: cgroups.CgroupStats{OOMKills: count},
	}}
}

func TestPSICriticalCondition(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := config.PSIResourceConfig{Enabled: true, CriticalThreshold: 50.0}

	for _, tc := range []struct {
		name       string
		avg60      float64
		shouldFire bool
	}{
		{"below threshold", 49.9, false},
		{"at threshold", 50.0, true},
		{"above threshold", 75.0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cnd := newPSICriticalCondition(psiResourceCPU, cfg)
			require.Equal(t, "LoadShedderPSI/cpu", cnd.Name)

			fired, desc := cnd.Fn(ctx, loadmonitor.Stats{}, statsWithPSI(psiResourceCPU, tc.avg60), time.Second)
			require.Equal(t, tc.shouldFire, fired)
			if tc.shouldFire {
				require.Equal(t, eventLoadShedderPSICritical, desc)
			}
		})
	}
}

func TestPSICriticalCondition_Disabled(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		name string
		cfg  config.PSIResourceConfig
	}{
		{"disabled flag", config.PSIResourceConfig{Enabled: false, CriticalThreshold: 50.0}},
		{"zero threshold", config.PSIResourceConfig{Enabled: true, CriticalThreshold: 0}},
		{"negative threshold", config.PSIResourceConfig{Enabled: true, CriticalThreshold: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cnd := newPSICriticalCondition(psiResourceMemory, tc.cfg)
			fired, _ := cnd.Fn(ctx, loadmonitor.Stats{}, statsWithPSI(psiResourceMemory, 99.0), time.Second)
			require.False(t, fired)
		})
	}
}

func TestPSICriticalCondition_PerResource(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := config.PSIResourceConfig{Enabled: true, CriticalThreshold: 50.0}

	// Pressure on CPU should not trip the memory condition, and vice versa.
	memCnd := newPSICriticalCondition(psiResourceMemory, cfg)
	fired, _ := memCnd.Fn(ctx, loadmonitor.Stats{}, statsWithPSI(psiResourceCPU, 99.0), time.Second)
	require.False(t, fired)

	fired, _ = memCnd.Fn(ctx, loadmonitor.Stats{}, statsWithPSI(psiResourceMemory, 75.0), time.Second)
	require.True(t, fired)
}

func TestOOMKillCondition(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cnd := newOOMKillCondition()
	require.Equal(t, "LoadShedderOOMKill", cnd.Name)

	for _, tc := range []struct {
		name       string
		previous   uint64
		current    uint64
		shouldFire bool
	}{
		{"counter unchanged", 5, 5, false},
		{"counter increased", 5, 6, true},
		{"counter increased by many", 0, 10, true},
		{"counter reset (cgroup recreated)", 5, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fired, desc := cnd.Fn(ctx, statsWithOOMKills(tc.previous), statsWithOOMKills(tc.current), time.Second)
			require.Equal(t, tc.shouldFire, fired)
			if tc.shouldFire {
				require.Equal(t, eventLoadShedderOOMKill, desc)
			}
		})
	}
}
