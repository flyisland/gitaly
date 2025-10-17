package cgroups

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// mockRand is a mock for random number generator that implements randomizer interface of cgroups package.
type mockRand struct {
	t   *testing.T
	max int
	n   int
}

func newMockRand(t *testing.T, max int) *mockRand {
	return &mockRand{t: t, max: max, n: -1}
}

// Intn returns 0 to inputMax-1 in turn then back to 0. It also verifies if the input argument matched the initialized max.
func (r *mockRand) Intn(inputMax int) int {
	require.Equalf(r.t, r.max, inputMax, "unexpected rand.Intn() argument, expected %d, actual %d", r.max, inputMax)

	r.n++
	if r.n >= r.max {
		r.n = 0
	}
	return r.n
}

func TestCloneIntoCgroup(t *testing.T) {
	mountPoint := t.TempDir()
	hierarchyRoot := filepath.Join(mountPoint, "gitaly")

	// Create the files we expect the manager to open.
	require.NoError(t, os.MkdirAll(filepath.Join(hierarchyRoot, "gitaly-1"), fs.ModePerm))
	for i := 0; i < 10; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(hierarchyRoot, "gitaly-1", fmt.Sprintf("repos-%d", i)), fs.ModePerm))
	}
	require.NoError(t, os.WriteFile(filepath.Join(hierarchyRoot, "gitaly-1", "cgroup.subtree_control"), nil, fs.ModePerm))

	t.Run("command args used as key", func(t *testing.T) {
		mgr := NewManager(cgroups.Config{
			Mountpoint:    mountPoint,
			HierarchyRoot: "gitaly",
			Repositories: cgroups.Repositories{
				Count: 10,
			},
		}, testhelper.NewLogger(t), 1)
		for _, tc := range []struct {
			desc                string
			existingSysProcAttr *syscall.SysProcAttr
			expectedSysProcAttr *syscall.SysProcAttr
		}{
			{
				desc: "nil SysProcAttr",
				expectedSysProcAttr: &syscall.SysProcAttr{
					UseCgroupFD: true,
				},
			},
			{
				desc: "existing SysProcAttr",
				existingSysProcAttr: &syscall.SysProcAttr{
					Chroot: "preserved",
				},
				expectedSysProcAttr: &syscall.SysProcAttr{
					Chroot:      "preserved",
					UseCgroupFD: true,
				},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				command := exec.Command("command", "arg")
				command.SysProcAttr = tc.existingSysProcAttr

				path, closer, err := mgr.CloneIntoCgroup(command)
				require.NoError(t, err)
				defer testhelper.MustClose(t, closer)
				require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-5"), path)

				// CgroupFD may vary so just set it in the assertion after verifying it is set.
				require.NotEqual(t, 0, command.SysProcAttr.UseCgroupFD)
				tc.expectedSysProcAttr.CgroupFD = command.SysProcAttr.CgroupFD

				require.Equal(t, tc.expectedSysProcAttr, command.SysProcAttr)
			})
		}
	})

	t.Run("key provided", func(t *testing.T) {
		mgr := NewManager(cgroups.Config{
			Mountpoint:    mountPoint,
			HierarchyRoot: "gitaly",
			Repositories: cgroups.Repositories{
				Count: 10,
			},
		}, testhelper.NewLogger(t), 1)

		commandWithKey := exec.Command("command", "arg")
		pathWithKey, closeWithKey, err := mgr.CloneIntoCgroup(commandWithKey, WithCgroupKey("some-key"))
		require.NoError(t, err)
		defer testhelper.MustClose(t, closeWithKey)
		require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-3"), pathWithKey)
		require.True(t, commandWithKey.SysProcAttr.UseCgroupFD)
		require.NotEqual(t, 0, commandWithKey.SysProcAttr.UseCgroupFD)

		commandWithoutKey := exec.Command("command", "arg")
		pathWithoutKey, closeWithoutKey, err := mgr.CloneIntoCgroup(commandWithoutKey)
		require.NoError(t, err)
		defer testhelper.MustClose(t, closeWithoutKey)
		require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-5"), pathWithoutKey)
		require.True(t, commandWithoutKey.SysProcAttr.UseCgroupFD)
		require.NotEqual(t, 0, commandWithoutKey.SysProcAttr.UseCgroupFD)

		require.NotEqual(t, pathWithKey, pathWithoutKey, "commands should be placed in different groups")
		require.NotEqual(t, commandWithKey.SysProcAttr.CgroupFD, commandWithoutKey.SysProcAttr.CgroupFD)
	})

	t.Run("AllocationCount is set", func(t *testing.T) {
		mgr := NewManager(cgroups.Config{
			Mountpoint:    mountPoint,
			HierarchyRoot: "gitaly",
			Repositories: cgroups.Repositories{
				Count:             10,
				MaxCgroupsPerRepo: 3,
			},
		}, testhelper.NewLogger(t), 1)

		assertSpawnedCommand := func(key string, groupID string) {
			commandWithKey := exec.Command("command", "arg")
			pathWithKey, closeWithKey, err := mgr.CloneIntoCgroup(commandWithKey, WithCgroupKey(key))
			require.NoError(t, err)
			defer testhelper.MustClose(t, closeWithKey)
			require.Equal(t, filepath.Join(hierarchyRoot, fmt.Sprintf("gitaly-1/%s", groupID)), pathWithKey)
			require.True(t, commandWithKey.SysProcAttr.UseCgroupFD)
			require.NotEqual(t, 0, commandWithKey.SysProcAttr.UseCgroupFD)
		}

		mgr.(*CGroupManager).rand = newMockRand(t, 3)
		// Starting point is 3 (similar to the above test)
		assertSpawnedCommand("some-key", "repos-3")
		assertSpawnedCommand("some-key", "repos-4")
		assertSpawnedCommand("some-key", "repos-5")

		mgr.(*CGroupManager).rand = newMockRand(t, 3)
		// Starting point is 8
		assertSpawnedCommand("key-1", "repos-8")
		assertSpawnedCommand("key-1", "repos-9")
		assertSpawnedCommand("key-1", "repos-0") // Wrap-around
	})
}

func TestNewCgroupStatus(t *testing.T) {
	cfg := cgroups.Config{Repositories: cgroups.Repositories{Count: 1000}}
	repoPath := func(i int) string {
		return filepath.Join("gitaly/gitaly-1", fmt.Sprintf("repos-%d", i))
	}

	status := newCgroupStatus(cfg, repoPath)

	for i := 0; i < int(cfg.Repositories.Count); i++ {
		cgroupPath := repoPath(i)
		cgLock, ok := status.m[cgroupPath]
		require.True(t, ok)
		require.NotNil(t, cgLock)
		require.False(t, cgLock.isCreated())
	}
}
