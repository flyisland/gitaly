package burdenmonitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockLoadMonitor struct {
	ch         chan loadmonitor.Event
	notifyErr  error
	conditions []loadmonitor.Condition
}

func newMockLoadMonitor() *mockLoadMonitor {
	return &mockLoadMonitor{ch: make(chan loadmonitor.Event, 4)}
}

func (m *mockLoadMonitor) Start(context.Context) error { return nil }

func (m *mockLoadMonitor) Stop() {}

func (m *mockLoadMonitor) NotifyOn(conditions ...loadmonitor.Condition) (<-chan loadmonitor.Event, error) {
	if m.notifyErr != nil {
		return nil, m.notifyErr
	}
	m.conditions = conditions
	return m.ch, nil
}

func enabledShedderConfig() LoadShedderConfig {
	resource := func(critical float64) config.PSIResourceConfig {
		return config.PSIResourceConfig{Enabled: true, CriticalThreshold: critical}
	}
	return LoadShedderConfig{
		PSI: config.PSIPressureConfig{
			CPU:    resource(50.0),
			Memory: resource(40.0),
			IO:     resource(40.0),
		},
	}
}

func TestNewLoadShedder_RegistersConditions(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)
	require.NotNil(t, ls)

	// Three PSI critical conditions + one OOM kill condition.
	require.Len(t, lm.conditions, 4)

	var names []string
	for _, c := range lm.conditions {
		names = append(names, c.Name)
	}
	require.ElementsMatch(t, []string{
		"LoadShedderPSI/cpu",
		"LoadShedderPSI/memory",
		"LoadShedderPSI/io",
		"LoadShedderOOMKill",
	}, names)
}

func TestNewLoadShedder_NotifyOnError(t *testing.T) {
	t.Parallel()

	lm := &mockLoadMonitor{notifyErr: errors.New("monitor not running")}
	bm := New(testhelper.SharedLogger(t))

	_, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.ErrorContains(t, err, "subscribing to load monitor")
}

func TestLoadShedder_EventCancelsTopEntriesByCPU(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))
	ctx := testhelper.Context(t)

	// Register 12 entries with strictly increasing CPU times so the top
	// shedTopN (10) are deterministic.
	entries := make([]*RPCEntry, 12)
	for i := range entries {
		_, e := bm.RegisterRPC(ctx, "/foo.Service/A")
		e.RegisterCommand(100+i, "git rev-list", time.Now())
		e.MarkCommandCompleted(100+i, time.Duration(i+1)*time.Millisecond, 0)
		entries[i] = e
	}

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	lm.ch <- loadmonitor.Event{
		ConditionName: "LoadShedderPSI/cpu",
		Description:   eventLoadShedderPSICritical,
	}
	close(lm.ch)

	ls.run(ctx)

	// The 10 highest-CPU entries (indices 2..11) are cancelled.
	for _, e := range entries[2:] {
		require.Error(t, e.Context.Err(), "expected entry to be cancelled")
	}
	// The two lowest-CPU entries are untouched.
	require.NoError(t, entries[0].Context.Err())
	require.NoError(t, entries[1].Context.Err())
}

func TestLoadShedder_ClosedChannelExitsGoroutine(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	close(lm.ch)
	ls.run(testhelper.Context(t))
}

func TestLoadShedder_StartProcessesEventsAsynchronously(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	defer cancel()

	_, entry := bm.RegisterRPC(ctx, "/foo.Service/A")
	entry.RegisterCommand(101, "git rev-list", time.Now())
	entry.MarkCommandCompleted(101, 10*time.Millisecond, 0)

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	ls.Start(ctx)

	lm.ch <- loadmonitor.Event{
		ConditionName: "LoadShedderOOMKill",
		Description:   eventLoadShedderOOMKill,
	}

	// The shedder runs in a goroutine, so wait for the cancellation to
	// propagate to the entry's context.
	select {
	case <-entry.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("entry was not cancelled by the LoadShedder goroutine within 1s")
	}

	// The Done channel fires for any cancellation; verify the cause came
	// from the shedder by checking it is a RESOURCE_EXHAUSTED structerr
	// referencing the shedder, not test/parent context cleanup.
	cause := context.Cause(entry.Context)
	require.Error(t, cause)
	require.ErrorContains(t, cause, "RPC cancelled by load shedder")
	require.Equal(t, codes.ResourceExhausted, status.Code(cause))
}

func TestLoadShedder_ContextCancellationExitsGoroutine(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ls.run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LoadShedder.run did not exit after context cancellation")
	}
}
