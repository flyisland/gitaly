//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// cmdArgs are Command arguments used processes to be added to a cgroup.
var cmdArgs = []string{"ls", "-hal", "."}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// setupMockCgroup configures some defaults for the mocks
func setupMockCgroup(t *testing.T, version int) (mockCgroup, cgroups.Config) {
	mock := newMock(t, version)
	config := defaultCgroupsConfig()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16
	config.HierarchyRoot = "gitaly"
	config.Mountpoint = mock.rootPath()
	return mock, config
}

// pidExistsInCgroupProcs validates that an expected process id has been added to the cgroup.procs file
func pidExistsInCgroupProcs(t *testing.T, mock *mockCgroup, pid int, expectedPID int, shard uint) {
	for _, path := range (*mock).repoPaths(pid, shard) {
		procsPath := filepath.Join(path, "cgroup.procs")
		content := string(readCgroupFile(t, procsPath))

		if content != "" {
			actualPID, err := strconv.Atoi(content)
			require.NoError(t, err, "Could not parse content's PID to an int")

			require.Equal(t, expectedPID, actualPID,
				fmt.Sprintf("Process id %d should be added to shard %d's cgroup.procs. Content has procs: %d", expectedPID, shard, actualPID))
		}
	}
}
