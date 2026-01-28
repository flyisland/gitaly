package watchers

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestCgroupCPUWatcher_Name(t *testing.T) {
	t.Parallel()

	manager := NewCgroupCPUWatcher(&testCgroupManager{}, 0.5)
	require.Equal(t, cgroupCPUWatcherName, manager.Name())
}

func TestCgroupCPUWatcher_Poll(t *testing.T) {
	t.Parallel()

	type recentTimeFunc func() time.Time

	for _, tc := range []struct {
		desc           string
		manager        *testCgroupManager
		pollTimes      []recentTimeFunc
		cpuThreshold   float64
		expectedEvents []*limiter.BackoffEvent
		expectedErrs   []error
	}{
		{
			desc:    "disabled watcher",
			manager: &testCgroupManager{ready: false},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
			},
		},
		{
			desc: "cgroup stats return empty stats",
			manager: &testCgroupManager{
				ready:     true,
				statsList: []cgroups.Stats{{}},
			},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
			},
		},
		{
			desc: "cgroup stats query returns errors",
			manager: &testCgroupManager{
				ready:     true,
				statsErr:  fmt.Errorf("something goes wrong"),
				statsList: []cgroups.Stats{{}},
			},
			expectedErrs: []error{fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", fmt.Errorf("something goes wrong"))},
		},
		{
			desc: "cgroup stats query returns errors",
			manager: &testCgroupManager{
				ready:     true,
				statsErr:  fmt.Errorf("something goes wrong"),
				statsList: []cgroups.Stats{{}},
			},
			expectedErrs: []error{fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", fmt.Errorf("something goes wrong"))},
		},
		{
			desc: "watcher polls once",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(99, 99),
				},
			},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
			},
		},
		{
			desc: "cgroup is throttled less than 50% of the observation window",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 107),
					testCPUStat(3, 110),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
				mockRecentTime(t, "2023-01-01T11:00:30Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
			},
		},
		{
			desc: "cgroup is not throttled",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(1, 100),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
			},
		},
		{
			desc: "cgroup is throttled more than 50% of the observation window",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 108),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"), // 15 seconds apart
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 8.0, 0.5),
				},
			},
		},
		{
			desc: "cgroup is throttled more than 50% for multiple times",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 108), // 8 ceonds
					testCPUStat(3, 123), // 15 seconds
					testCPUStat(3, 123), // no throttling
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
				mockRecentTime(t, "2023-01-01T11:00:30Z"),
				mockRecentTime(t, "2023-01-01T11:00:45Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 8.0, 0.5),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 15.0, 0.5),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
			},
		},
		{
			desc: "cgroup is throttled more than the observation window",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					// As this stat is provided by cgroup, there is a possibility
					// that the throttled time is greater than the recorded
					// observation window.
					testCPUStat(2, 200),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 100.0, 0.5),
				},
			},
		},
		{
			desc: "cgroup is throttled more than the observation window (duplicate)",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					// As this stat is provided by cgroup, there is a possibility
					// that the throttled time is greater than the recorded
					// observation window.
					testCPUStat(2, 200),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 100.0, 0.5),
				},
			},
		},
		{
			desc: "the observation window is zero",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 107),
					testCPUStat(3, 115),
					testCPUStat(4, 120),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
				mockRecentTime(t, "2023-01-01T11:00:30Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 8.0, 0.5),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
			},
		},
		{
			desc: "the cgroup stat is reset in the middle",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(5, 107),
					testCPUStat(3, 108), // Reset 1
					testCPUStat(5, 109),
					testCPUStat(7, 100), // Reset 2
					testCPUStat(9, 109),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
				mockRecentTime(t, "2023-01-01T11:00:30Z"),
				mockRecentTime(t, "2023-01-01T11:00:45Z"),
				mockRecentTime(t, "2023-01-01T11:01:00Z"),
				mockRecentTime(t, "2023-01-01T11:01:15Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{WatcherName: cgroupCPUWatcherName, ShouldBackoff: false},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 9.0, 0.5),
				},
			},
		},
		{
			desc: "the observation windows unequal",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 107),
					testCPUStat(3, 123),
					testCPUStat(4, 154),
					testCPUStat(5, 184),
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"), // 15 seconds
				mockRecentTime(t, "2023-01-01T11:00:45Z"), // 30 seconds
				mockRecentTime(t, "2023-01-01T11:01:45Z"), // 60 seconds
				mockRecentTime(t, "2023-01-01T11:03:45Z"), // 120 seconds
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(30.0, 16.0, 0.5),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(60.0, 31.0, 0.5),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
			},
		},
		{
			desc: "customized CPU threshold",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					testCPUStat(1, 100),
					testCPUStat(2, 108), // 8 seconds - okay
					testCPUStat(3, 123), // 15 seconds - 15 over 15, exceeding 90%
					testCPUStat(4, 136), // 13 seconds - fine
				},
			},
			cpuThreshold: 0.9,
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
				mockRecentTime(t, "2023-01-01T11:00:30Z"),
				mockRecentTime(t, "2023-01-01T11:00:45Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats:         expectedCPUBackoffStats(15.0, 15.0, 0.9),
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
			},
		},
		{
			desc: "backoff event includes PSI and memory stats",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							CPUThrottledCount:    1,
							CPUThrottledDuration: 100,
						},
					},
					{
						ParentStats: cgroups.CgroupStats{
							CPUThrottledCount:    2,
							CPUThrottledDuration: 108,
							MemoryUsage:          1500000000,
							MemoryLimit:          2000000000,
							TotalAnon:            1200000000,
							TotalInactiveFile:    300000000,
							MemoryHighEvents:     5,
							MemoryMaxEvents:      2,
							OOMKills:             1,
							MemoryPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 25.5, Avg60: 18.3},
								Full: cgroups.PSIData{Avg10: 12.1, Avg60: 8.7},
							},
							IOPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 15.2, Avg60: 10.5},
								Full: cgroups.PSIData{Avg10: 5.8, Avg60: 3.2},
							},
							PgMajFault: 1500,
						},
					},
				},
			},
			pollTimes: []recentTimeFunc{
				mockRecentTime(t, "2023-01-01T11:00:00Z"),
				mockRecentTime(t, "2023-01-01T11:00:15Z"),
			},
			expectedEvents: []*limiter.BackoffEvent{
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats: map[string]any{
						"time_diff":                  15.0,
						"throttled_duration":         8.0,
						"throttled_threshold":        0.5,
						"memory_usage":               uint64(1500000000),
						"memory_limit":               uint64(2000000000),
						"anon":                       uint64(1200000000),
						"inactive_file":              uint64(300000000),
						"memory_high_events":         uint64(5),
						"memory_max_events":          uint64(2),
						"oom_kills":                  uint64(1),
						"memory_pressure_some_avg10": 25.5,
						"memory_pressure_some_avg60": 18.3,
						"memory_pressure_full_avg10": 12.1,
						"memory_pressure_full_avg60": 8.7,
						"io_pressure_some_avg10":     15.2,
						"io_pressure_some_avg60":     10.5,
						"io_pressure_full_avg10":     5.8,
						"io_pressure_full_avg60":     3.2,
						"pgmajfault":                 uint64(1500),
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			watcher := NewCgroupCPUWatcher(tc.manager, tc.cpuThreshold)

			if tc.pollTimes != nil {
				require.Equal(t, len(tc.expectedEvents), len(tc.pollTimes), "poll times set up incorrectly")
			}

			for i, expectedEvent := range tc.expectedEvents {
				if tc.pollTimes != nil {
					watcher.currentTime = tc.pollTimes[i]
				}
				event, err := watcher.Poll(testhelper.Context(t))

				var expectedErr error
				if tc.expectedErrs != nil {
					expectedErr = tc.expectedErrs[i]
				}
				if expectedErr != nil {
					require.Equal(t, expectedErr, err)
					require.Nil(t, event)
				} else {
					require.NoError(t, err)
					require.Equal(t, expectedEvent, event)
				}
			}
		})
	}
}

func mockRecentTime(t *testing.T, timeStr string) func() time.Time {
	return func() time.Time {
		current, err := time.Parse("2006-01-02T15:04:05Z", timeStr)
		require.NoError(t, err)
		return current
	}
}

func testCPUStat(count uint64, duration float64) cgroups.Stats {
	return cgroups.Stats{
		ParentStats: cgroups.CgroupStats{
			CPUThrottledCount:    count,
			CPUThrottledDuration: duration,
		},
	}
}

func expectedCPUBackoffStats(timeDiff, throttledDuration, throttledThreshold float64) map[string]any {
	return map[string]any{
		"time_diff":                  timeDiff,
		"throttled_duration":         throttledDuration,
		"throttled_threshold":        throttledThreshold,
		"memory_usage":               uint64(0),
		"memory_limit":               uint64(0),
		"anon":                       uint64(0),
		"inactive_file":              uint64(0),
		"memory_high_events":         uint64(0),
		"memory_max_events":          uint64(0),
		"oom_kills":                  uint64(0),
		"memory_pressure_some_avg10": float64(0),
		"memory_pressure_some_avg60": float64(0),
		"memory_pressure_full_avg10": float64(0),
		"memory_pressure_full_avg60": float64(0),
		"io_pressure_some_avg10":     float64(0),
		"io_pressure_some_avg60":     float64(0),
		"io_pressure_full_avg10":     float64(0),
		"io_pressure_full_avg60":     float64(0),
		"pgmajfault":                 uint64(0),
	}
}
