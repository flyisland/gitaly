package cgroups

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/errors/cfgerror"
)

func TestFallbackToOldVersion(t *testing.T) {
	testCases := []struct {
		desc         string
		configBefore Config
		configAfter  Config
	}{
		{
			desc:         "empty config",
			configBefore: Config{},
			configAfter:  Config{},
		},
		{
			desc: "new format",
			configBefore: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Repositories: Repositories{
					Count:       100,
					MemoryBytes: 1024,
					CPUShares:   16,
				},
			},
			configAfter: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Repositories: Repositories{
					Count:       100,
					MemoryBytes: 1024,
					CPUShares:   16,
				},
			},
		},
		{
			desc: "old format",
			configBefore: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: true,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: true,
					Shares:  16,
				},
			},
			configAfter: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: true,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: true,
					Shares:  16,
				},
				Repositories: Repositories{
					Count:       100,
					MemoryBytes: 1024,
					CPUShares:   16,
				},
			},
		},
		{
			desc: "old format, memory only",
			configBefore: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: true,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: false,
					Shares:  16,
				},
			},
			configAfter: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: true,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: false,
					Shares:  16,
				},

				Repositories: Repositories{
					Count:       100,
					MemoryBytes: 1024,
				},
			},
		},
		{
			desc: "old format, cpu only",
			configBefore: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: false,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: true,
					Shares:  16,
				},
			},
			configAfter: Config{
				Mountpoint:    "some/mountpoint",
				HierarchyRoot: "gitaly",
				Count:         100,
				Memory: Memory{
					Enabled: false,
					Limit:   1024,
				},
				CPU: CPU{
					Enabled: true,
					Shares:  16,
				},
				Repositories: Repositories{
					Count:     100,
					CPUShares: 16,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.configBefore.FallbackToOldVersion()
			assert.Equal(t, tc.configAfter, tc.configBefore)
		})
	}
}

func TestRepositories_Validate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		repositories Repositories
		memBytes     int64
		cpuShares    uint64
		cpuQuotaUs   int64
		expectedErr  error
	}{
		{
			name: "empty",
		},
		{
			name: "valid with zero limits",
			repositories: Repositories{
				Count:             1,
				MaxCgroupsPerRepo: 1,
				MemoryBytes:       0,
				CPUShares:         0,
				CPUQuotaUs:        0,
			},
		},
		{
			name: "valid",
			repositories: Repositories{
				Count:             2,
				MaxCgroupsPerRepo: 2,
				MemoryBytes:       1024,
				CPUShares:         16,
			},
			memBytes:  2048,
			cpuShares: 32,
		},
		{
			name: "valid",
			repositories: Repositories{
				Count:             2,
				MaxCgroupsPerRepo: 1,
				MemoryBytes:       1024,
				CPUShares:         16,
			},
			memBytes:  2048,
			cpuShares: 32,
		},
		{
			name: "invalid",
			repositories: Repositories{
				Count:             2,
				MaxCgroupsPerRepo: 3,
				MemoryBytes:       1024,
				CPUShares:         16,
				CPUQuotaUs:        32000,
			},
			memBytes:   256,
			cpuShares:  2,
			cpuQuotaUs: 16000,
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("%w: 3 out of [0, 2]", cfgerror.ErrNotInRange),
					"max_cgroups_per_repo",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: 1024 out of [0, 256]", cfgerror.ErrNotInRange),
					"memory_bytes",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: 16 out of [0, 2]", cfgerror.ErrNotInRange),
					"cpu_shares",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: 32000 out of [0, 16000]", cfgerror.ErrNotInRange),
					"cpu_quota_us",
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.repositories.Validate(tc.memBytes, tc.cpuShares, tc.cpuQuotaUs)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		config      Config
		expectedErr error
	}{
		{
			name: "empty",
		},
		{
			name: "valid",
			config: Config{
				Mountpoint:    "/usr/gitaly",
				HierarchyRoot: "cgroups",
				Repositories: Repositories{
					MemoryBytes: 512,
					CPUShares:   8,
				},
				MemoryBytes: 1024,
				CPUShares:   32,
			},
		},
		{
			name: "invalid",
			config: Config{
				Repositories: Repositories{
					CPUShares: 8,
				},
				CPUShares: 4,
			},
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("%w: 8 out of [0, 4]", cfgerror.ErrNotInRange),
					"repositories", "cpu_shares",
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
