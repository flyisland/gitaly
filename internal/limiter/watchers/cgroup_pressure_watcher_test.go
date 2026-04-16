package watchers

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestPSIResourceConfig_Defaults(t *testing.T) {
	t.Parallel()

	mem := config.PSIResourceConfig{}.FulfillDefaults("memory")
	require.Equal(t, 10.0, mem.WarningThreshold)
	require.Equal(t, 20.0, mem.BackoffThreshold)
	require.Equal(t, 40.0, mem.CriticalThreshold)
	require.Equal(t, 30, mem.SustainDurationSeconds)
	require.Equal(t, 0.85, mem.FastFallRatio)

	io := config.PSIResourceConfig{}.FulfillDefaults("io")
	require.Equal(t, 10.0, io.WarningThreshold)
	require.Equal(t, 20.0, io.BackoffThreshold)
	require.Equal(t, 40.0, io.CriticalThreshold)

	cpu := config.PSIResourceConfig{}.FulfillDefaults("cpu")
	require.Equal(t, 20.0, cpu.WarningThreshold)
	require.Equal(t, 25.0, cpu.BackoffThreshold)
	require.Equal(t, 50.0, cpu.CriticalThreshold)
}

func TestCgroupPressureWatcher_Poll(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	watcherName := "CgroupPressure/memory"
	memCfg := config.PSIResourceConfig{}.FulfillDefaults("memory")
	ioCfg := config.PSIResourceConfig{}.FulfillDefaults("io")
	cpuCfg := config.PSIResourceConfig{}.FulfillDefaults("cpu")

	t.Run("disabled watcher", func(t *testing.T) {
		w := NewCgroupPressureWatcher(
			&testCgroupManager{ready: false},
			testhelper.SharedLogger(t),
			PressureResourceMemory,
			memCfg,
		)
		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.Equal(t, &limiter.BackoffEvent{WatcherName: watcherName, ShouldBackoff: false}, event)
	})

	t.Run("stats error", func(t *testing.T) {
		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready:     true,
				statsErr:  fmt.Errorf("broken"),
				statsList: []cgroups.Stats{{}},
			},
			testhelper.SharedLogger(t),
			PressureResourceMemory,
			memCfg,
		)
		event, err := w.Poll(ctx)
		require.Error(t, err)
		require.Nil(t, event)
	})

	t.Run("severity classification", func(t *testing.T) {
		now := time.Now()
		for _, tc := range []struct {
			name        string
			avg60       float64
			wantLogs    int
			wantMsg     string
			wantSev     string
			wantBackoff bool
		}{
			{name: "healthy", avg60: 5.0, wantLogs: 0, wantSev: PSIHealthy},
			{name: "warning", avg60: 15.0, wantLogs: 1, wantMsg: "PSI pressure above warning threshold", wantSev: PSISeverityWarning},
			{name: "backoff", avg60: 25.0, wantLogs: 1, wantMsg: "PSI pressure at backoff threshold", wantSev: PSISeverityBackoff},
			{name: "critical", avg60: 45.0, wantLogs: 1, wantMsg: "Critical PSI pressure detected", wantSev: PSISeverityCritical, wantBackoff: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				logger := testhelper.NewLogger(t)
				hook := testhelper.AddLoggerHook(logger)

				w := NewCgroupPressureWatcher(
					&testCgroupManager{
						ready: true,
						statsList: []cgroups.Stats{{
							ParentStats: cgroups.CgroupStats{
								MemoryPSI: cgroups.PSIMetrics{
									Some: cgroups.PSIData{Avg10: tc.avg60, Avg60: tc.avg60},
								},
							},
						}},
					},
					logger,
					PressureResourceMemory,
					memCfg,
				)
				w.timeFunc = func() time.Time { return now }

				event, err := w.Poll(ctx)
				require.NoError(t, err)
				require.Equal(t, tc.wantBackoff, event.ShouldBackoff)
				require.Len(t, hook.AllEntries(), tc.wantLogs)
				if tc.wantLogs > 0 {
					require.Equal(t, tc.wantMsg, hook.LastEntry().Message)
					require.Equal(t, tc.wantSev, hook.LastEntry().Data["psi_severity"])
				}
			})
		}
	})

	t.Run("backoff-level pressure logs with 3-condition fields and all PSI metrics", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)
		now := time.Now()

		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{{
					ParentStats: cgroups.CgroupStats{
						MemoryPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 25.0, Avg60: 25.0, Avg300: 10.0},
							Full: cgroups.PSIData{Avg10: 3.0, Avg60: 2.0, Avg300: 0.8},
						},
						IOPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 5.0, Avg60: 3.0, Avg300: 1.2},
							Full: cgroups.PSIData{Avg10: 0.8, Avg60: 0.6, Avg300: 0.2},
						},
						CPUPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 8.0, Avg60: 6.0, Avg300: 2.5},
						},
					},
				}},
			},
			logger,
			PressureResourceMemory,
			memCfg,
		)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
		require.Len(t, hook.AllEntries(), 1)
		data := hook.LastEntry().Data

		require.Equal(t, "backoff", data["psi_severity"])
		require.Equal(t, "memory", data["psi_resource"])
		require.Contains(t, data, "psi_sustained")
		require.Contains(t, data, "psi_above_10s")
		require.Contains(t, data, "psi_falling_rapidly")

		require.Equal(t, 25.0, data["memory_pressure_some_avg10"])
		require.Equal(t, 25.0, data["memory_pressure_some_avg60"])
		require.Equal(t, 10.0, data["memory_pressure_some_avg300"])
		require.Equal(t, 3.0, data["memory_pressure_full_avg10"])
		require.Equal(t, 2.0, data["memory_pressure_full_avg60"])
		require.Equal(t, 0.8, data["memory_pressure_full_avg300"])

		require.Equal(t, 5.0, data["io_pressure_some_avg10"])
		require.Equal(t, 1.2, data["io_pressure_some_avg300"])
		require.Equal(t, 0.8, data["io_pressure_full_avg10"])
		require.Equal(t, 0.6, data["io_pressure_full_avg60"])
		require.Equal(t, 0.2, data["io_pressure_full_avg300"])

		require.Equal(t, 8.0, data["cpu_pressure_some_avg10"])
		require.Equal(t, 2.5, data["cpu_pressure_some_avg300"])

		require.Equal(t, 20.0, data["psi_threshold_backoff"])
		require.Equal(t, 40.0, data["psi_threshold_critical"])
	})

	t.Run("no backoff before sustain duration elapses", func(t *testing.T) {
		now := time.Now()
		mgr := &testCgroupManager{
			ready: true,
			statsList: []cgroups.Stats{
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 25.0, Avg60: 25.0},
					},
				}},
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 28.0, Avg60: 28.0},
					},
				}},
			},
		}

		w := NewCgroupPressureWatcher(mgr, testhelper.SharedLogger(t), PressureResourceMemory, memCfg)
		w.timeFunc = func() time.Time { return now }

		// threshold  is set but sustain window hasn't elapsed.
		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)

		// still at the same instant, sustain duration not met.
		event, err = w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
	})

	t.Run("immediate backoff at critical severity", func(t *testing.T) {
		mgr := &testCgroupManager{
			ready: true,
			statsList: []cgroups.Stats{
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 50.0, Avg60: 50.0},
					},
				}},
			},
		}

		w := NewCgroupPressureWatcher(mgr, testhelper.SharedLogger(t), PressureResourceMemory, memCfg)

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.True(t, event.ShouldBackoff)
		require.Equal(t, watcherName, event.WatcherName)
		require.Contains(t, event.Reason, "critical")
	})

	t.Run("Memory PSI backoff when all three conditions are met", func(t *testing.T) {
		start := time.Now()
		mgr := &testCgroupManager{
			ready: true,
			statsList: []cgroups.Stats{
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 25.0, Avg60: 25.0},
					},
				}},
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 30.0, Avg60: 30.0},
					},
				}},
			},
		}

		now := start
		w := NewCgroupPressureWatcher(mgr, testhelper.SharedLogger(t), PressureResourceMemory, memCfg)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)

		// Advance past sustain window (30s).
		now = start.Add(31 * time.Second)

		event, err = w.Poll(ctx)
		require.NoError(t, err)
		require.True(t, event.ShouldBackoff)
		require.Equal(t, "CgroupPressure/memory", event.WatcherName)
		require.Contains(t, event.Reason, "memory")
	})

	t.Run("CPU PSI backoff after sustained pressure", func(t *testing.T) {
		start := time.Now()
		mgr := &testCgroupManager{
			ready: true,
			statsList: []cgroups.Stats{
				// avg60 crosses CPU backoffThreshold (25.0) but stays below critical (50.0).
				{ParentStats: cgroups.CgroupStats{
					CPUPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 35.0, Avg60: 35.0},
					},
				}},
				// pressure still elevated after sustain window.
				{ParentStats: cgroups.CgroupStats{
					CPUPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 40.0, Avg60: 40.0},
					},
				}},
			},
		}

		now := start
		w := NewCgroupPressureWatcher(mgr, testhelper.SharedLogger(t), PressureResourceCPU, cpuCfg)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)

		// Advance past sustain window (30s).
		now = start.Add(31 * time.Second)

		event, err = w.Poll(ctx)
		require.NoError(t, err)
		require.True(t, event.ShouldBackoff)
		require.Equal(t, "CgroupPressure/cpu", event.WatcherName)
		require.Contains(t, event.Reason, "cpu")
	})

	t.Run("no backoff when pressure is falling rapidly", func(t *testing.T) {
		start := time.Now()
		mgr := &testCgroupManager{
			ready: true,
			statsList: []cgroups.Stats{
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 25.0, Avg60: 25.0},
					},
				}},
				// avg10 drops below fastFallRatio (0.85) * previous avg10 (25.0) = 21.25
				{ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 18.0, Avg60: 25.0},
					},
				}},
			},
		}

		now := start
		w := NewCgroupPressureWatcher(mgr, testhelper.SharedLogger(t), PressureResourceMemory, memCfg)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)

		now = start.Add(31 * time.Second)

		event, err = w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
	})

	t.Run("IO resource logs correctly", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{{
					ParentStats: cgroups.CgroupStats{
						IOPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 18.0, Avg60: 15.0, Avg300: 7.5},
						},
					},
				}},
			},
			logger,
			PressureResourceIO,
			ioCfg,
		)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
		require.Equal(t, "io", hook.LastEntry().Data["psi_resource"])
		require.Equal(t, 18.0, hook.LastEntry().Data["io_pressure_some_avg10"])

		require.Equal(t, 15.0, hook.LastEntry().Data["io_pressure_some_avg60"])
		require.Equal(t, 7.5, hook.LastEntry().Data["io_pressure_some_avg300"])

		require.Equal(t, "warning", hook.LastEntry().Data["psi_severity"])
		require.Equal(t, 20.0, hook.LastEntry().Data["psi_threshold_backoff"])
		require.Equal(t, 40.0, hook.LastEntry().Data["psi_threshold_critical"])
	})

	t.Run("CPU resource logs correctly", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{{
					ParentStats: cgroups.CgroupStats{
						CPUPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 28.0, Avg60: 25.0, Avg300: 15.0},
						},
					},
				}},
			},
			logger,
			PressureResourceCPU,
			cpuCfg,
		)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
		require.Equal(t, "CgroupPressure/cpu", w.Name())
		require.Equal(t, "cpu", hook.LastEntry().Data["psi_resource"])
		require.Equal(t, 28.0, hook.LastEntry().Data["cpu_pressure_some_avg10"])
		require.Equal(t, 25.0, hook.LastEntry().Data["cpu_pressure_some_avg60"])
		require.Equal(t, 15.0, hook.LastEntry().Data["cpu_pressure_some_avg300"])
	})

	t.Run("Memory resource logs correctly", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{{
					ParentStats: cgroups.CgroupStats{
						MemoryPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 15.0, Avg60: 15.0},
						},
					},
				}},
			},
			logger,
			PressureResourceMemory,
			memCfg,
		)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
		require.Len(t, hook.AllEntries(), 1)
		require.Equal(t, "PSI pressure above warning threshold", hook.LastEntry().Message)
		require.Equal(t, 20.0, hook.LastEntry().Data["psi_threshold_backoff"])
		require.Equal(t, 40.0, hook.LastEntry().Data["psi_threshold_critical"])
	})

	t.Run("config overrides apply", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		customCfg := config.PSIResourceConfig{
			WarningThreshold:       2.0,
			BackoffThreshold:       5.0,
			CriticalThreshold:      15.0,
			SustainDurationSeconds: 30,
			FastFallRatio:          0.9,
		}

		w := NewCgroupPressureWatcher(
			&testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{{
					ParentStats: cgroups.CgroupStats{
						MemoryPSI: cgroups.PSIMetrics{
							Some: cgroups.PSIData{Avg10: 6.0, Avg60: 6.0},
						},
					},
				}},
			},
			logger,
			PressureResourceMemory,
			customCfg.FulfillDefaults("memory"),
		)
		w.timeFunc = func() time.Time { return now }

		event, err := w.Poll(ctx)
		require.NoError(t, err)
		require.False(t, event.ShouldBackoff)
		require.Len(t, hook.AllEntries(), 1)
		require.Equal(t, "PSI pressure at backoff threshold", hook.LastEntry().Message)
		require.Equal(t, 5.0, hook.LastEntry().Data["psi_threshold_backoff"])
		require.Equal(t, 15.0, hook.LastEntry().Data["psi_threshold_critical"])
	})
}
