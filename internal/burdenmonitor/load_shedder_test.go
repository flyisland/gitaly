package burdenmonitor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

type snipeCall struct {
	n      int
	sortBy SortBy
}

type mockRPCKiller struct {
	calls []snipeCall
}

func (m *mockRPCKiller) SnipeTopN(n int, sortBy SortBy) int {
	m.calls = append(m.calls, snipeCall{n: n, sortBy: sortBy})
	return n
}

func TestLoadShedder_SystemDegradedTriggersSnipe(t *testing.T) {
	t.Parallel()

	killer := &mockRPCKiller{}
	ch := make(chan loadmonitor.Event, 1)
	ls := NewLoadShedder(testhelper.SharedLogger(t), killer, ch)

	ch <- loadmonitor.Event{Name: loadmonitor.EventSystemDegraded}
	close(ch)

	ls.run(testhelper.Context(t))

	require.Equal(t, []snipeCall{{n: 10, sortBy: SortByCPU}}, killer.calls)
}

func TestLoadShedder_NonSystemDegradedEventIgnored(t *testing.T) {
	t.Parallel()

	killer := &mockRPCKiller{}
	ch := make(chan loadmonitor.Event, 1)
	ls := NewLoadShedder(testhelper.SharedLogger(t), killer, ch)

	ch <- loadmonitor.Event{Name: "SomeOtherEvent"}
	close(ch)

	ls.run(testhelper.Context(t))

	require.Empty(t, killer.calls)
}

func TestLoadShedder_ClosedChannelExitsGoroutine(t *testing.T) {
	t.Parallel()

	killer := &mockRPCKiller{}
	ch := make(chan loadmonitor.Event)
	ls := NewLoadShedder(testhelper.SharedLogger(t), killer, ch)

	close(ch)

	ls.run(testhelper.Context(t))

	require.Empty(t, killer.calls)
}
