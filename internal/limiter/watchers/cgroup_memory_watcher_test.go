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
					"memory_usage":     uint64(1800000000),
					"inactive_file":    uint64(0),
					"memory_limit":     uint64(2000000000),
					"memory_threshold": 0.9,
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
					"memory_usage":     uint64(1900000000),
					"inactive_file":    uint64(100000000),
					"memory_limit":     uint64(2000000000),
					"memory_threshold": 0.9,
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
					"memory_usage":     uint64(1200000000),
					"inactive_file":    uint64(100000000),
					"memory_limit":     uint64(2000000000),
					"memory_threshold": 0.5,
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
