package limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestNewMemoryUsageCondition(t *testing.T) {
	tests := []struct {
		name        string
		threshold   float64
		shouldEmit  bool
		stats       loadmonitor.Stats
		description string
	}{
		{
			name:       "if OOM events, it should return true",
			threshold:  1.0,
			shouldEmit: true,
			stats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						UnderOOM: true,
					},
				},
			},
			description: eventMemoryOOM,
		},
		{
			name:       "if memory limit is 0, it should return false",
			threshold:  1.0,
			shouldEmit: false,
			stats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						MemoryLimit: 0,
					},
				},
			},
			description: "",
		},
		{
			name:       "if total anonymous memory ratio exceeds threshold, it should return true",
			threshold:  0.2,
			shouldEmit: true,
			stats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						TotalAnon:   10,
						MemoryLimit: 1,
					},
				},
			},
			description: eventMemoryAnon,
		},
		{
			name:       "if total anonymous memory ratio does not exceeds threshold, it should return false",
			threshold:  0.5,
			shouldEmit: false,
			stats: loadmonitor.Stats{
				CGroup: cgroups.Stats{
					ParentStats: cgroups.CgroupStats{
						TotalAnon:   1,
						MemoryLimit: 10,
					},
				},
			},
			description: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cnd := newMemoryUsageCondition(tt.threshold)
			require.Equal(t, "CgroupMemory", cnd.Name)
			shouldEmit, description := cnd.Fn(ctx, tt.stats, tt.stats, time.Second)
			require.Equal(t, tt.shouldEmit, shouldEmit)
			require.Equal(t, tt.description, description)
		})
	}
}
