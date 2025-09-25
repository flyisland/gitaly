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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  8.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  8.0,
						"throttled_threshold": 0.5,
					},
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  15.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  100.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  100.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  8.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  9.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           30.0,
						"throttled_duration":  16.0,
						"throttled_threshold": 0.5,
					},
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: true,
					Reason:        "cgroup CPU throttled too much",
					Stats: map[string]any{
						"time_diff":           60.0,
						"throttled_duration":  31.0,
						"throttled_threshold": 0.5,
					},
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
					Stats: map[string]any{
						"time_diff":           15.0,
						"throttled_duration":  15.0,
						"throttled_threshold": 0.9,
					},
				},
				{
					WatcherName:   cgroupCPUWatcherName,
					ShouldBackoff: false,
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
