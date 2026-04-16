package limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestNewCpuThrottlingCondition(t *testing.T) {
	tests := []struct {
		name          string
		threshold     float64
		shouldEmit    bool
		previousStats loadmonitor.Stats
		currentStats  loadmonitor.Stats
		description   string
	}{
		{
			name:       "when throttled count is decreasing, it should return false",
			threshold:  0.8,
			shouldEmit: false,
			previousStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledCount: 10,
					},
				},
			},
			currentStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledCount: 5,
					},
				},
			},
			description: "",
		},
		{
			name:       "when throttled duration is decreasing, it should return false",
			threshold:  0.8,
			shouldEmit: false,
			previousStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 10,
					},
				},
			},
			currentStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 5,
					},
				},
			},
			description: "",
		},
		{
			name:       "if throttling exceeds threshold, it should return true",
			threshold:  0.2,
			shouldEmit: true,
			previousStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 0.25,
					},
				},
			},
			currentStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 0.5,
					},
				},
			},
			description: eventCPUThrottling,
		},
		{
			name:       "if throttling exceeds threshold, it should return true",
			threshold:  0.5,
			shouldEmit: true,
			previousStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 5,
					},
				},
			},
			currentStats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						CPUThrottledDuration: 20,
					},
				},
			},
			description: eventCPUThrottling,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cnd := newCPUThrottlingCondition(tt.threshold)
			require.Equal(t, "CgroupCpu", cnd.Name)
			shouldEmit, description := cnd.Fn(ctx, tt.previousStats, tt.currentStats, time.Second)
			require.Equal(t, tt.shouldEmit, shouldEmit)
			require.Equal(t, tt.description, description)
		})
	}
}
