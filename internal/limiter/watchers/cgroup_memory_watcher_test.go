package watchers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestCgroupMemoryWatcher_Name(t *testing.T) {
	t.Parallel()

	manager := NewCgroupMemoryWatcher(&testCgroupManager{}, 0.9)
	require.Equal(t, cgroupMemoryWatcherName, manager.Name())
}

func TestCgroupMemoryWatcher_Poll(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc            string
		manager         *testCgroupManager
		memoryThreshold float64
		expectedEvent   *limiter.BackoffEvent
		expectedErr     error
	}{
		{
			desc:    "disabled watcher",
			manager: &testCgroupManager{ready: false},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup stats return empty stats",
			manager: &testCgroupManager{
				ready:     true,
				statsList: []cgroups.Stats{{}},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
		},
		{
			desc: "cgroup stats query returns errors",
			manager: &testCgroupManager{
				ready:     true,
				statsErr:  fmt.Errorf("something goes wrong"),
				statsList: []cgroups.Stats{{}},
			},
			expectedErr: fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", fmt.Errorf("something goes wrong")),
		},
		{
			desc: "cgroup memory usage is more than or equal to 90%",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1800000000,
							MemoryLimit: 2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup memory exceeds threshold",
				Stats: map[string]any{
					"memory_usage":               uint64(1800000000),
					"memory_limit":               uint64(2000000000),
					"memory_threshold":           0.9,
					"inactive_file":              uint64(0),
					"anon":                       uint64(0),
					"anon_ratio":                 float64(0),
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
				},
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup memory usage excluding inactive file is greater than or equal to 90%",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1900000000,
							TotalInactiveFile: 100000000,
							MemoryLimit:       2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup memory exceeds threshold",
				Stats: map[string]any{
					"memory_usage":               uint64(1900000000),
					"memory_limit":               uint64(2000000000),
					"memory_threshold":           0.9,
					"inactive_file":              uint64(100000000),
					"anon":                       uint64(0),
					"anon_ratio":                 float64(0),
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
				},
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup is under OOM",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1900000000,
							MemoryLimit: 2000000000,
							UnderOOM:    true,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup is under OOM",
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup memory usage normal",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1700000000,
							MemoryLimit: 2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup memory usage excluding inactive file is normal",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1900000000,
							TotalInactiveFile: 200000000,
							MemoryLimit:       2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
			expectedErr: nil,
		},
		{
			desc: "customized memory threshold",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1200000000,
							TotalInactiveFile: 100000000,
							MemoryLimit:       2000000000,
						},
					},
				},
			},
			memoryThreshold: 0.5,
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup memory exceeds threshold",
				Stats: map[string]any{
					"memory_usage":               uint64(1200000000),
					"memory_limit":               uint64(2000000000),
					"memory_threshold":           0.5,
					"inactive_file":              uint64(100000000),
					"anon":                       uint64(0),
					"anon_ratio":                 float64(0),
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
				},
			},
			expectedErr: nil,
		},
		{
			desc: "backoff event includes PSI and memory stats",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1850000000,
							MemoryLimit:       2000000000,
							TotalAnon:         1400000000,
							TotalInactiveFile: 50000000,
							MemoryHighEvents:  10,
							MemoryMaxEvents:   3,
							OOMKills:          2,
							MemoryPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 35.5, Avg60: 22.8},
								Full: cgroups.PSIData{Avg10: 18.2, Avg60: 12.5},
							},
							IOPSI: cgroups.PSIMetrics{
								Some: cgroups.PSIData{Avg10: 20.3, Avg60: 14.7},
								Full: cgroups.PSIData{Avg10: 8.9, Avg60: 5.1},
							},
							PgMajFault: 2500,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup memory exceeds threshold",
				Stats: map[string]any{
					"memory_usage":               uint64(1850000000),
					"memory_limit":               uint64(2000000000),
					"memory_threshold":           0.9,
					"inactive_file":              uint64(50000000),
					"anon":                       uint64(1400000000),
					"anon_ratio":                 float64(0.7),
					"memory_high_events":         uint64(10),
					"memory_max_events":          uint64(3),
					"oom_kills":                  uint64(2),
					"memory_pressure_some_avg10": 35.5,
					"memory_pressure_some_avg60": 22.8,
					"memory_pressure_full_avg10": 18.2,
					"memory_pressure_full_avg60": 12.5,
					"io_pressure_some_avg10":     20.3,
					"io_pressure_some_avg60":     14.7,
					"io_pressure_full_avg10":     8.9,
					"io_pressure_full_avg60":     5.1,
					"pgmajfault":                 uint64(2500),
				},
			},
			expectedErr: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			watcher := NewCgroupMemoryWatcher(tc.manager, tc.memoryThreshold)
			event, err := watcher.Poll(testhelper.Context(t))

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				require.Nil(t, event)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedEvent, event)
			}
		})
	}
}

func TestCgroupMemoryWatcher_AnonMemory(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		manager     *testCgroupManager
		expectEvent bool
		expectLog   bool
	}{
		{
			desc: "no backoff + no anon pressure",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       850000000,
							MemoryLimit:       2000000000,
							TotalAnon:         500000000,
							TotalInactiveFile: 50000000,
						},
					},
				},
			},
			expectEvent: false,
			expectLog:   false,
		},
		{
			desc: "no backoff + anon pressure",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       850000000,
							MemoryLimit:       2000000000,
							TotalAnon:         1500000000,
							TotalInactiveFile: 50000000,
						},
					},
				},
			},
			expectEvent: false,
			expectLog:   true,
		},
		{
			desc: "backoff + no anon pressure",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1850000000,
							MemoryLimit:       2000000000,
							TotalAnon:         500000000,
							TotalInactiveFile: 50000000,
						},
					},
				},
			},
			expectEvent: true,
			expectLog:   false,
		},
		{
			desc: "backoff + anon pressure",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage:       1850000000,
							MemoryLimit:       2000000000,
							TotalAnon:         1500000000,
							TotalInactiveFile: 50000000,
						},
					},
				},
			},
			expectEvent: true,
			expectLog:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			watcher := NewCgroupMemoryWatcher(tc.manager, 0.9).WithLogger(logger)
			event, err := watcher.Poll(testhelper.Context(t))
			require.Nil(t, err)

			require.Equal(t, tc.expectEvent, event.ShouldBackoff)

			if tc.expectLog {
				for _, logEntry := range hook.AllEntries() {
					require.Contains(t, logEntry.Message, "Anonymous memory pressure detected", "should have anon memory log")
				}
			}
		})
	}
}
