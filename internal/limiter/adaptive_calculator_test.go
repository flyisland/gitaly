package limiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestAdaptiveCalculator_alreadyStarted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		CalibrationInterval: 10 * time.Millisecond,
	}
	calculator := NewAdaptiveCalculator(cfg, testhelper.SharedLogger(t), &mockLoadMonitor{}, nil)

	stop, err := calculator.Start(testhelper.Context(t))
	require.NoError(t, err)

	stop2, err := calculator.Start(testhelper.Context(t))
	require.Errorf(t, err, "adaptive calculator: already started")
	require.Nil(t, stop2)

	stop()
}

// TestAdaptiveCalculator tests the generic behavior of the Start
// method. It makes sure that everything is hooked accordingly
// and that the limit calibration occurs.
// For more fine-grained test scenarios involving the
// calibration of metrics, see the test for `calibrateLimits`.
func TestAdaptiveCalculator_Start(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc             string
		lastBackoffEvent *BackoffEvent
		limits           []AdaptiveLimiter
		mainLoopCount    int
		expectedLimits   map[string][]int
	}{
		{
			desc:             "No limits should not cause any issues",
			mainLoopCount:    5,
			limits:           []AdaptiveLimiter{},
			lastBackoffEvent: nil,
			expectedLimits:   map[string][]int{},
		},
		{
			desc:          "Single limit should be calibrated",
			mainLoopCount: 5,
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 25, 100, 10, 0.5),
			},
			lastBackoffEvent: nil,
			expectedLimits: map[string][]int{
				"testLimit": {25, 26, 27, 28, 29, 30},
			},
		},
		{
			desc:          "Multiple limits limit should be calibrated",
			mainLoopCount: 5,
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 25, 100, 10, 0.5),
				newTestLimit("testLimit2", 35, 30, 10, 0.8),
			},
			lastBackoffEvent: &BackoffEvent{},
			expectedLimits: map[string][]int{
				"testLimit":  {25, 12, 13, 14, 15, 16},
				"testLimit2": {35, 28, 29, 30, 30, 30},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			mainLoopDoneCh := make(chan struct{})

			logger := testhelper.NewLogger(t)
			ticker := helper.NewCountTicker(tc.mainLoopCount, func() { close(mainLoopDoneCh) })

			cfg := Config{
				CalibrationInterval: 1 * time.Hour,
			}

			loadMonitor := &mockLoadMonitor{}
			_ = loadMonitor.Start(ctx)

			calculator := NewAdaptiveCalculator(cfg, logger, loadMonitor, tc.limits)
			calculator.tickerCreator = func(duration time.Duration) helper.Ticker { return ticker }
			calculator.lastBackoffEvent = tc.lastBackoffEvent

			// Start the calculator
			stop, err := calculator.Start(testhelper.Context(t))
			require.NoError(t, err)

			// Wait for all loops to finish
			<-mainLoopDoneCh

			// This select statement ensures that during a call to `Start()`
			// the channel returned by `loadmonitor.NotifyOn` has a listener.
			// In other words it ensures `waitForEvents` has been called.
			select {
			case loadMonitor.ch <- loadmonitor.Event{}:
			default:
				t.Fatalf("the channel returned by the load monitor on Start should have a listener")
			}

			// Assert that the last backoff event is reset to nil
			// after each main loop run.
			calculator.mu.Lock()
			require.Nil(t, calculator.lastBackoffEvent)
			calculator.mu.Unlock()

			// Stop the monitor
			stop()

			// Validate each limits
			for name, expectedLimits := range tc.expectedLimits {
				limit := findLimitWithName(tc.limits, name)
				require.NotNil(t, limit, "not found limit with name %q", name)
				require.Equal(t, expectedLimits, limit.currents[:tc.mainLoopCount+1])
			}
		})
	}
}

func TestAdaptiveCalculator_calibrateLimits(t *testing.T) {
	tests := []struct {
		name string
		// backoffSequence is the sequence to follow to set the
		// last backoff event. The evolution of each limit (additive
		// increase, multiplicative decrease) should follow this sequence.
		// Example: nil, nil, {}, nil, {}
		// Means: increase, increase, decrease, increase, decrease.
		backoffSequence []*BackoffEvent
		limits          []AdaptiveLimiter
		expectedLimits  map[string][]int
		expectedLogs    []string
	}{
		{
			name: "simple monotonic sequence",
			backoffSequence: []*BackoffEvent{
				nil, nil, nil, nil, nil,
			},
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 25, 100, 10, 0.5),
			},
			expectedLimits: map[string][]int{
				"testLimit": {25, 26, 27, 28, 29, 30},
			},
			expectedLogs: []string{
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=26 previous_limit=25`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=27 previous_limit=26`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=28 previous_limit=27`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=29 previous_limit=28`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=30 previous_limit=29`,
			},
		},
		{
			name: "minimum value should be honoured",
			backoffSequence: []*BackoffEvent{
				nil, nil, {}, nil, {},
			},
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 25, 100, 10, 0.5),
			},
			expectedLimits: map[string][]int{
				// the last-1 value is 14. Halving this value should
				// return 7, but here the limit's minimum is 10.
				// This test validates that the last value is indeed 10.
				"testLimit": {25, 26, 27, 13, 14, 10},
			},
			expectedLogs: []string{
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=26 previous_limit=25`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=27 previous_limit=26`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=13 previous_limit=27 reason=test`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=14 previous_limit=13`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=14 reason=test`,
			},
		},
		{
			name: "maximum value should be honoured",
			backoffSequence: []*BackoffEvent{
				nil, nil, nil, nil, nil,
			},
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 98, 100, 10, 0.5),
			},
			expectedLimits: map[string][]int{
				// make sure we never increase past the max of the limit
				"testLimit": {98, 99, 100, 100, 100, 100},
			},
			expectedLogs: []string{
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=99 previous_limit=98`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=99`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=100`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=100`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=100`,
			},
		},
		{
			name: "when minimum value is same as initial",
			backoffSequence: []*BackoffEvent{
				{}, nil, nil, nil, nil,
			},
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 98, 100, 98, 0.5),
			},
			expectedLimits: map[string][]int{
				// make sure we never increase past the max of the limit
				"testLimit": {98, 98, 99, 100, 100, 100},
			},
			expectedLogs: []string{
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=98`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=99 previous_limit=98`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=99`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=100`,
				`level=debug msg="Additive increase" limit_rpc=testLimit new_limit=100 previous_limit=100`,
			},
		},
		{
			name: "a series of backoff events",
			backoffSequence: []*BackoffEvent{
				{}, {}, {}, {}, {},
			},
			limits: []AdaptiveLimiter{
				newTestLimit("testLimit", 20, 100, 10, 0.5),
			},
			expectedLimits: map[string][]int{
				// make sure we never increase past the max of the limit
				"testLimit": {20, 10, 10, 10, 10, 10},
			},
			expectedLogs: []string{
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=20`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=10`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=10`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=10`,
				`level=info msg="Multiplicative decrease" condition=test limit_rpc=testLimit new_limit=10 previous_limit=10`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testhelper.Context(t)
			logger := testhelper.NewLogger(t, testhelper.WithLevel(logrus.DebugLevel))
			hook := testhelper.AddLoggerHook(logger)

			c := &AdaptiveCalculator{
				limits:          tc.limits,
				logger:          logger,
				currentLimitVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"limit"}),
			}

			// Set limits to initial value
			for _, limit := range tc.limits {
				c.updateLimit(limit, limit.Setting().Initial)
			}

			// Simulate a sequence of backoff events by setting the
			// lastBackoffEvent and then calibrating the limits based
			// on this event (nil or non-nil).
			for _, event := range tc.backoffSequence {
				if event != nil {
					event.ConditionName = "test"
					event.Reason = "test"
				}
				c.lastBackoffEvent = event
				c.calibrateLimits(ctx)
			}

			// Assert the limits value
			for name, expectedLimits := range tc.expectedLimits {
				limit := findLimitWithName(tc.limits, name)
				require.NotNil(t, limit, "not found limit with name %q", name)
				require.Equal(t, expectedLimits, limit.currents[:len(tc.backoffSequence)+1])
			}

			assertLogs(t, tc.expectedLogs, hook.AllEntries())
		})
	}
}

func assertLogs(t *testing.T, expectedLogs []string, entries []*logrus.Entry) {
	require.Equal(t, len(expectedLogs), len(entries))
	for index, expectedLog := range expectedLogs {
		msg, err := entries[index].String()
		require.NoError(t, err)
		require.Contains(t, msg, expectedLog)
	}
}

func findLimitWithName(limits []AdaptiveLimiter, name string) *testLimit {
	for _, l := range limits {
		limit := l.(*testLimit)
		if limit.name == name {
			return limit
		}
	}
	return nil
}

type testLimit struct {
	sync.Mutex

	currents       []int
	name           string
	initial        int
	max            int
	min            int
	backoffBackoff float64
}

func newTestLimit(name string, initial int, max int, min int, backoff float64) *testLimit {
	return &testLimit{name: name, initial: initial, max: max, min: min, backoffBackoff: backoff}
}

func (l *testLimit) Name() string { return l.name }
func (l *testLimit) Current() int {
	l.Lock()
	defer l.Unlock()

	if len(l.currents) == 0 {
		return 0
	}
	return l.currents[len(l.currents)-1]
}

func (l *testLimit) Update(val int) {
	l.Lock()
	defer l.Unlock()

	l.currents = append(l.currents, val)
}

func (*testLimit) AfterUpdate(_ AfterUpdateHook) {}

func (l *testLimit) Setting() AdaptiveSetting {
	return AdaptiveSetting{
		Initial:       l.initial,
		Max:           l.max,
		Min:           l.min,
		BackoffFactor: l.backoffBackoff,
	}
}

type mockLoadMonitor struct {
	ch chan loadmonitor.Event
}

func (m *mockLoadMonitor) Start(ctx context.Context) error {
	m.ch = make(chan loadmonitor.Event, 1)
	return nil
}

func (m *mockLoadMonitor) Stop() {
	close(m.ch)
}

func (m *mockLoadMonitor) NotifyOn(conditions ...loadmonitor.Condition) (<-chan loadmonitor.Event, error) {
	return m.ch, nil
}
