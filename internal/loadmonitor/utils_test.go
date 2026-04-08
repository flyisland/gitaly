package loadmonitor

import (
	"io"
	"os/exec"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
)

type mockCgroupManager struct {
	stats cgroups.Stats
	err   error
	m     sync.Mutex
}

func (m *mockCgroupManager) Setup() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockCgroupManager) Ready() bool {
	// TODO implement me
	panic("implement me")
}

func (m *mockCgroupManager) AddCommand(cmd *exec.Cmd, option ...cgroups.AddCommandOption) (string, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockCgroupManager) SupportsCloneIntoCgroup() bool {
	// TODO implement me
	panic("implement me")
}

func (m *mockCgroupManager) CloneIntoCgroup(cmd *exec.Cmd, option ...cgroups.AddCommandOption) (string, io.Closer, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockCgroupManager) Stats() (cgroups.Stats, error) {
	m.m.Lock()
	defer m.m.Unlock()
	return m.stats, m.err
}

func (m *mockCgroupManager) setStats(stats cgroups.Stats) {
	m.m.Lock()
	defer m.m.Unlock()
	m.stats = stats
}

func (m *mockCgroupManager) setError(err error) {
	m.m.Lock()
	defer m.m.Unlock()
	m.err = err
}
