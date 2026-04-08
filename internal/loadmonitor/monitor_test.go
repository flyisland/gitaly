package loadmonitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

var defaultConditionFalse = Condition{
	Description: "test_false",
	Fn: func(_ context.Context, previous, current Stats, pollInterval time.Duration) (bool, string) {
		return false, "test_false"
	},
}

var defaultConditionTrue = Condition{
	Description: "test_true",
	Fn: func(_ context.Context, previous, current Stats, pollInterval time.Duration) (bool, string) {
		return true, "test_true"
	},
}

func Test_defaultMonitor_Start(t *testing.T) {
	cgroupManager := &mockCgroupManager{}
	monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, monitor.Start(ctx))

	require.False(t, monitor.isShuttingDown)
	require.True(t, monitor.isRunning())

	// Calling Start twice should return an error
	require.ErrorIs(t, monitor.Start(ctx), ErrAlreadyStarted)
}

func Test_defaultMonitor_Stop(t *testing.T) {
	t.Parallel()

	t.Run("calling stop should close all consumers", func(t *testing.T) {
		cgroupManager := &mockCgroupManager{}
		monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		require.NoError(t, monitor.Start(ctx))
		eventCh, _ := monitor.NotifyOn(defaultConditionFalse)

		// Here we stop the monitor by calling `Stop`
		monitor.Stop()
		require.Equal(t, Event{}, <-eventCh)

		require.True(t, monitor.isShuttingDown)
		require.False(t, monitor.isRunning())
	})

	t.Run("stop can be called after context cancellation", func(t *testing.T) {
		cgroupManager := &mockCgroupManager{}
		monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)

		ctx, cancel := context.WithCancel(context.Background())

		require.NoError(t, monitor.Start(ctx))

		eventCh, _ := monitor.NotifyOn(defaultConditionFalse)

		// Here we stop the monitor by cancelling the context
		cancel()
		require.Equal(t, Event{}, <-eventCh)

		require.True(t, monitor.isShuttingDown)
		require.False(t, monitor.isRunning())
	})
}

func Test_defaultMonitor_NotifyOn(t *testing.T) {
	t.Parallel()

	t.Run("when manager is running it should return a consumer channel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ticker := helper.NewManualTicker()
		cgroupManager := &mockCgroupManager{}

		monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)
		monitor.pollTicker = ticker

		// Start the manager
		require.NoError(t, monitor.Start(ctx))

		// Call NotifyOn
		eventCh, err := monitor.NotifyOn(defaultConditionFalse, defaultConditionTrue)

		// Expect no error and a consumer event channel
		require.NoError(t, err)
		require.Len(t, monitor.state.consumers, 1)

		monitor.Stop()
		require.Equal(t, Event{}, <-eventCh)
	})

	t.Run("when manager is stopped, it should return an error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ticker := helper.NewManualTicker()
		cgroupManager := &mockCgroupManager{}

		monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)
		monitor.pollTicker = ticker

		// Start the manager
		require.NoError(t, monitor.Start(ctx))

		// Close it
		monitor.Stop()

		// Then call NotifyOn, and expect an error.
		_, err := monitor.NotifyOn(defaultConditionFalse, defaultConditionTrue)
		require.ErrorIs(t, err, ErrNotRunning)
	})

	t.Run("when manager is not running, it should return an error", func(t *testing.T) {
		ticker := helper.NewManualTicker()
		cgroupManager := &mockCgroupManager{}

		monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)
		monitor.pollTicker = ticker

		// Don't start the manager

		// Then call NotifyOn, and expect an error.
		_, err := monitor.NotifyOn(defaultConditionFalse, defaultConditionTrue)
		require.ErrorIs(t, err, ErrNotRunning)
	})
}

func Test_defaultMonitor_poll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := helper.NewManualTicker()
	cgroupManager := &mockCgroupManager{}

	monitor := NewLoadMonitor(Config{}, testhelper.NewLogger(t), cgroupManager)
	monitor.pollTicker = ticker

	require.NoError(t, monitor.Start(ctx))

	// Set the first stats on the cgroupManager for next poll
	firstStats := cgroups.Stats{
		ParentStats: cgroups.CgroupStats{
			CPUThrottledCount: 200,
		},
	}
	cgroupManager.setStats(firstStats)

	// First poll
	assert.NoError(t, monitor.poll())

	// Since this is the first poll, previous and current should be the same
	assert.Equal(t, firstStats, monitor.state.currentStats.CGroup)
	assert.Equal(t, firstStats, monitor.state.previousStats.CGroup)

	// Set the first stats on the cgroupManager for next poll
	secondStats := cgroups.Stats{
		ParentStats: cgroups.CgroupStats{
			CPUThrottledCount: 200,
		},
	}
	cgroupManager.setStats(secondStats)

	// Second poll
	assert.NoError(t, monitor.poll())

	assert.Equal(t, secondStats, monitor.state.currentStats.CGroup)
	assert.Equal(t, firstStats, monitor.state.previousStats.CGroup)

	cgroupManager.setError(errors.New("test error polling"))

	// Third poll. Here we expect an error since we called `setError`
	assert.ErrorContains(t, monitor.poll(), "test error polling")
	monitor.Stop()
}

func Test_defaultMonitor_notify(t *testing.T) {
	const eventName = "testEvent"

	defaultCurrentStats := Stats{
		CGroup: cgroups.Stats{ParentStats: cgroups.CgroupStats{
			CPUThrottledCount:    200,
			CPUThrottledDuration: 300,
		}},
	}
	defaultPreviousStats := Stats{
		CGroup: cgroups.Stats{ParentStats: cgroups.CgroupStats{
			CPUThrottledCount:    400,
			CPUThrottledDuration: 500,
		}},
	}

	testCases := []struct {
		name          string
		expectedEvent Event
		condition     conditionFn
	}{
		{
			name:          "conditions should receive expected stats",
			expectedEvent: Event{},
			condition: func(ctx context.Context, previous, current Stats, _ time.Duration) (bool, string) {
				require.Equal(t, defaultPreviousStats, previous)
				require.Equal(t, defaultCurrentStats, current)
				return false, eventName
			},
		},
		{
			name: "when a condition evaluates to true, an event should be emitted",
			expectedEvent: Event{
				Name:         eventName,
				CurrentStats: defaultCurrentStats,
			},
			condition: func(ctx context.Context, previous, current Stats, _ time.Duration) (bool, string) {
				return true, eventName
			},
		},
		{
			name:          "when a condition evaluates to false, an event should not be emitted",
			expectedEvent: Event{},
			condition: func(ctx context.Context, previous, current Stats, _ time.Duration) (bool, string) {
				return false, eventName
			},
		},
		{
			name:          "a blocking condition should not send event after timeout",
			expectedEvent: Event{},
			condition: func(ctx context.Context, previous, current Stats, _ time.Duration) (bool, string) {
				<-ctx.Done()
				return true, eventName
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ticker := helper.NewManualTicker()
			cgroupManager := &mockCgroupManager{}

			cfg := Config{
				PollInterval:  time.Millisecond * 100,
				NotifyTimeout: time.Millisecond * 50,
			}
			monitor := NewLoadMonitor(cfg, testhelper.NewLogger(t), cgroupManager)
			monitor.pollTicker = ticker

			// Set the stats on the state
			monitor.state.currentStats = defaultCurrentStats
			monitor.state.previousStats = defaultPreviousStats

			// Start the monitor
			require.NoError(t, monitor.Start(ctx))

			// Register the Condition
			eventCh, _ := monitor.NotifyOn(Condition{
				Description: "test",
				Fn:          tt.condition,
			})

			// Call the notify method to notify all consumers
			monitor.notify(ctx)

			// Stop the monitor. When stopping the monitor, all consumer channels
			// gets closed.
			monitor.Stop()

			// If the Condition returned true, this channel still has the Event in the
			// queue, because it has a buffer of 1. If not, then because we stopped the m
			// monitor, this channel is now closed so it should return an empty Event{}.
			require.Equal(t, tt.expectedEvent, <-eventCh)
		})
	}
}
