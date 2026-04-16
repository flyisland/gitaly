package limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
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

func TestCgroupPressureCondition_fn(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	watcherName := "CgroupPressure/memory"
	memCfg := config.PSIResourceConfig{Enabled: true}.FulfillDefaults("memory")
	ioCfg := config.PSIResourceConfig{Enabled: true}.FulfillDefaults("io")
	cpuCfg := config.PSIResourceConfig{Enabled: true}.FulfillDefaults("cpu")

	t.Run("severity classification", func(t *testing.T) {
		now := time.Now()
		for _, tc := range []struct {
			name          string
			avg60         float64
			wantLogs      int
			wantMsg       string
			wantSev       string
			wantEventName string
		}{
			{name: "healthy", avg60: 3.0, wantLogs: 0, wantSev: pSIHealthy},
			{name: "warning", avg60: 15.0, wantLogs: 1, wantMsg: "PSI pressure above warning threshold", wantSev: pSISeverityWarning},
			{name: "backoff", avg60: 25.0, wantLogs: 1, wantMsg: "PSI pressure at backoff threshold", wantSev: pSISeverityBackoff, wantEventName: eventPSIBackoff},
			{name: "critical", avg60: 45.0, wantLogs: 1, wantMsg: "Critical PSI pressure detected", wantSev: pSISeverityCritical, wantEventName: eventPSICritical},
		} {
			t.Run(tc.name, func(t *testing.T) {
				logger := testhelper.NewLogger(t)
				hook := testhelper.AddLoggerHook(logger)

				b := newCgroupPressureConditionBuilder(memCfg, logger, pressureResourceMemory)
				b.timeFunc = func() time.Time { return now }

				stats := loadmonitor.Stats{
					CGroup: cgroups.Stats{
						ParentStats: cgroups.CgroupStats{
							MemoryPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 15.0, Avg60: tc.avg60, Avg300: 4.5},
								Full: cgroups.PSIData{Avg10: 3.0, Avg60: tc.avg60, Avg300: 0.8},
							},
							IOPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 5.0, Avg60: tc.avg60, Avg300: 1.2},
								Full: cgroups.PSIData{Avg10: 0.8, Avg60: tc.avg60, Avg300: 0.2},
							},
							CPUPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 8.0, Avg60: tc.avg60, Avg300: 2.5},
							},
						},
					},
				}

				shouldEmit, eventName := b.Condition().Fn(ctx, stats, stats, time.Second)

				require.Equal(t, tc.wantEventName, eventName)
				require.Equal(t, watcherName, b.Name())
				if eventName == eventPSIBackoff || eventName == eventPSICritical {
					require.True(t, shouldEmit)
				} else {
					require.False(t, shouldEmit)
				}
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

		cgroupStats := cgroups.Stats{
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
		}

		previousStats := loadmonitor.Stats{CGroup: cgroupStats}
		currentStats := loadmonitor.Stats{CGroup: cgroupStats}

		b := newCgroupPressureConditionBuilder(memCfg, logger, pressureResourceMemory)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, eventName := b.Condition().Fn(ctx, previousStats, currentStats, time.Second)

		require.Equal(t, eventPSIBackoff, eventName)
		require.True(t, shouldEmit)
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

	t.Run("never sends backoff event", func(t *testing.T) {
		now := time.Now()
		previousStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 5.0, Avg60: 5.0},
					},
				},
			},
		}
		currentStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 5.0, Avg60: 5.0},
					},
				},
			},
		}

		b := newCgroupPressureConditionBuilder(memCfg, testhelper.NewLogger(t), pressureResourceMemory)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, _ := b.Condition().Fn(ctx, previousStats, currentStats, time.Second)
		require.False(t, shouldEmit)
	})

	t.Run("IO resource logs correctly", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		currentStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					IOPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 18.0, Avg60: 15.0, Avg300: 7.5},
					},
				},
			},
		}

		b := newCgroupPressureConditionBuilder(ioCfg, logger, pressureResourceIO)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, _ := b.Condition().Fn(ctx, currentStats, currentStats, time.Second)
		require.False(t, shouldEmit)

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
		currentStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					CPUPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 28.0, Avg60: 25.0, Avg300: 15.0},
					},
				},
			},
		}

		b := newCgroupPressureConditionBuilder(cpuCfg, logger, pressureResourceCPU)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, eventName := b.Condition().Fn(ctx, currentStats, currentStats, time.Second)
		require.Equal(t, eventPSIBackoff, eventName)
		require.True(t, shouldEmit)

		require.Equal(t, "CgroupPressure/cpu", b.Name())
		require.Equal(t, "cpu", hook.LastEntry().Data["psi_resource"])
		require.Equal(t, 28.0, hook.LastEntry().Data["cpu_pressure_some_avg10"])
		require.Equal(t, 25.0, hook.LastEntry().Data["cpu_pressure_some_avg60"])
		require.Equal(t, 15.0, hook.LastEntry().Data["cpu_pressure_some_avg300"])
	})

	t.Run("Memory resource logs correctly", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		currentStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 15.0, Avg60: 15.0},
					},
				},
			},
		}

		b := newCgroupPressureConditionBuilder(memCfg, logger, pressureResourceMemory)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, _ := b.Condition().Fn(ctx, currentStats, currentStats, time.Second)
		require.False(t, shouldEmit)
		require.Len(t, hook.AllEntries(), 1)
		require.Equal(t, "PSI pressure above warning threshold", hook.LastEntry().Message)
		require.Equal(t, 20.0, hook.LastEntry().Data["psi_threshold_backoff"])
		require.Equal(t, 40.0, hook.LastEntry().Data["psi_threshold_critical"])
	})

	t.Run("config overrides apply", func(t *testing.T) {
		logger := testhelper.NewLogger(t)
		hook := testhelper.AddLoggerHook(logger)

		now := time.Now()
		currentStats := loadmonitor.Stats{
			CGroup: cgroups.Stats{
				ParentStats: cgroups.CgroupStats{
					MemoryPSI: cgroups.PSIMetrics{
						Some: cgroups.PSIData{Avg10: 6.0, Avg60: 6.0},
					},
				},
			},
		}
		customCfg := config.PSIResourceConfig{
			Enabled:                true,
			WarningThreshold:       2.0,
			BackoffThreshold:       5.0,
			CriticalThreshold:      15.0,
			SustainDurationSeconds: 30,
			FastFallRatio:          0.9,
		}

		b := newCgroupPressureConditionBuilder(customCfg.FulfillDefaults("memory"), logger, pressureResourceMemory)
		b.timeFunc = func() time.Time { return now }

		shouldEmit, eventName := b.Condition().Fn(ctx, currentStats, currentStats, time.Second)
		require.Equal(t, eventPSIBackoff, eventName)
		require.True(t, shouldEmit)

		require.Len(t, hook.AllEntries(), 1)
		require.Equal(t, "PSI pressure at backoff threshold", hook.LastEntry().Message)
		require.Equal(t, 5.0, hook.LastEntry().Data["psi_threshold_backoff"])
		require.Equal(t, 15.0, hook.LastEntry().Data["psi_threshold_critical"])
	})
}
