package watchers

import (
	"io"
	"os/exec"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type testCgroupManager struct {
	ready      bool
	statsErr   error
	statsList  []cgroups.Stats
	statsIndex int
}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func (m *testCgroupManager) Ready() bool { return m.ready }
func (m *testCgroupManager) Stats() (cgroups.Stats, error) {
	m.statsIndex++
	return m.statsList[m.statsIndex-1], m.statsErr
}
func (m *testCgroupManager) Setup() error                        { return nil }
func (m *testCgroupManager) Describe(ch chan<- *prometheus.Desc) {}
func (m *testCgroupManager) Collect(ch chan<- prometheus.Metric) {}
func (m *testCgroupManager) AddCommand(*exec.Cmd, ...cgroups.AddCommandOption) (string, error) {
	return "", nil
}

func (m *testCgroupManager) SupportsCloneIntoCgroup() bool {
	return true
}

func (m *testCgroupManager) CloneIntoCgroup(*exec.Cmd, ...cgroups.AddCommandOption) (string, io.Closer, error) {
	return "", io.NopCloser(nil), nil
}
