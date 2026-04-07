package loadmonitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// EventSystemDegraded is the event name emitted when the system is under
// degraded conditions and load shedding should be triggered.
const EventSystemDegraded = "SystemDegraded"

const (
	// defaultPollInterval is the default poll interval to poll resource usage stats
	defaultPollInterval = time.Second * 10

	// defaultConditionTimeout is the timeout given to a Condition
	defaultConditionTimeout = time.Second * 5
)

// ErrAlreadyStarted is returned when `Start` is called when the manager
// has already been started.
var ErrAlreadyStarted = errors.New("monitor already started")

// ErrNotRunning is returned when an operation on a non-running Monitor is performed.
var ErrNotRunning = errors.New("monitor not running")

// conditionFn is the type for a Condition function
type conditionFn func(ctx context.Context, previous, current Stats, interval time.Duration) (emitEvent bool, name string)

// Condition is a type that represent a condition to be evaluated by the LoadMonitor
// on every poll event.
type Condition struct {
	// Name is the name of the Condition. This is used to identify a Condition.
	// Ideally names should be unique across the application.
	Name string

	// Fn is a function type that takes the previous and current system Stats struct as arguments. A user
	// can define a function to compare system stats between two polling event and make a decision an event
	// should be sent or not.
	// If a particular Event occurred, as defined in the Condition, Fn must return true, along with the name
	// of the event. Else it must return false, and the name can be any string. an empty string is fine.
	// The duration between the two polled stats is also provided.
	// Implementation of this type must honor the context cancellation.
	Fn conditionFn
}

// Monitor is the base interface for the load monitor.
type Monitor interface {
	// Start starts the Monitor. When `ctx` is canceled, the Monitor will terminate
	// all polling and notifying activities.
	Start(ctx context.Context) error

	// Stop will stop the Monitor and wait for all shutdown operations to complete
	// before returning. It is an alternative to cancelling the `ctx` passed
	// to `Start`. It is safe to both cancel the context and call `Stop`.
	Stop()

	// NotifyOn takes a list of custom Condition as input and returns an Event channel.
	// The Monitor evaluates the conditions at a periodic interval. When a condition is
	// true, an event is sent down the Event channel. The returned channel should not be
	// closed by the receiver. It must be closed by the sender (Monitor).
	NotifyOn(conditions ...Condition) (<-chan Event, error)
}

// Event is the struct that is sent in an Event channel when a  condition evaluates to `true`.
// It contains information about the condition that was violated.
type Event struct {
	// ConditionName is the name of the condition that triggered this event
	ConditionName string
	// Description is the string value returned from a Condition.
	// It contains the reason why this event was fired.
	Description string
	// CurrentStats are the current statistics that were evaluated by the Condition
	// prior to firing this event.
	CurrentStats Stats
	// PreviousStats are the previous statistics that were evaluated by the Condition
	// prior to firing this event.
	PreviousStats Stats
}

// NewSystemDegradedEvent constructs a SystemDegraded Event with the given cgroup stats.
func NewSystemDegradedEvent(stats cgroups.Stats) Event {
	return Event{
		Name:         EventSystemDegraded,
		CurrentStats: Stats{CGroup: stats},
	}
}

// consumer is a struct representing a consumer of the Monitor.
type eventConsumer struct {
	out        chan Event
	conditions []Condition
}

// Config holds the various configurations to configure the behavior
// of the Monitor. It does not hold its dependencies.
type Config struct {
	// PollInterval defines at which interval to poll for resource stats
	PollInterval time.Duration

	// NotifyTimeout is the timeout before the Monitor abort evaluating conditions
	// after a poll. This is to prevent blocked or long-running Conditions from
	// blocking the main loop.
	NotifyTimeout time.Duration
}

type monitorState struct {
	// previousStats are the stats of the previous polling event
	previousStats Stats

	// currentStats are the current stats
	currentStats Stats

	// consumers is a list of eventConsumer to notify when their
	// condition is reached
	consumers []eventConsumer

	// lastPoll is the time of the last poll
	lastPoll time.Time

	// lastPollInterval is the interval between the two last poll.
	// Value is 0 when 1 or 0 poll occurred.
	lastPollInterval time.Duration
}

// DefaultMonitor is the default implementation of a LoadMonitor
type DefaultMonitor struct {
	// cfg is the configuration for a manager's instance
	cfg Config

	// metrics holds the various metrics this monitor emits
	metrics *monitorMetrics

	// logger the logger used in the monitor
	logger log.Logger

	// cgroupManager is the cgroup manager from which cgroup
	// stats are polled.
	cgroupManager cgroups.Manager

	// state holds the current state of the Monitor
	state monitorState

	// stateLock synchronize access to the state
	stateLock sync.Mutex

	// pollTicker is the ticker used to synchronize polling
	// to get new (up-to-date) resource usage data
	pollTicker helper.Ticker

	// started is used to determine if the Monitor is started or not
	// Value is `false` by default.
	started atomic.Bool

	// stopOnce is used to make sure Stop is executed only once
	stopOnce sync.Once

	// shutdownOnce is used to make sure the shutdown logic is executed only once
	// This is needed in addition to `stopOnce` because the Monitor can be stopped
	// in two ways:
	//   * Calling Stop()
	//   * Cancelling the context passed in `Start()`
	shutdownOnce sync.Once

	// isShuttingDown is set to true as soon as the Monitor is shutting down
	isShuttingDown bool

	// shutdownMutex synchronize access to `isShuttingDown` variable.
	// This ensures no new operations are performed while the Monitor is
	// shutting down.
	shutdownMutex sync.Mutex

	// stopCh is used by Stop to signal to the main loop
	// that is has to stop.
	stopCh chan struct{}

	// doneCh is used by the main loop to signal to `Stop` that
	// the Monitor is now fully and properly stopped.
	doneCh chan struct{}
}

var _ Monitor = (*DefaultMonitor)(nil)

// NewLoadMonitor creates a new Monitor using the defaultMonitor implementation.
func NewLoadMonitor(cfg Config, logger log.Logger, cgroupManager cgroups.Manager) *DefaultMonitor {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.NotifyTimeout == 0 {
		cfg.NotifyTimeout = defaultConditionTimeout
	}

	m := &DefaultMonitor{
		cfg:           cfg,
		logger:        logger,
		metrics:       newMonitorMetrics(),
		cgroupManager: cgroupManager,
		state: monitorState{
			consumers: make([]eventConsumer, 0),
		},
		pollTicker: helper.NewTimerTicker(cfg.PollInterval),
		stopCh:     make(chan struct{}, 1),
		doneCh:     make(chan struct{}),
	}
	return m
}

// Start starts the monitor main loop.
// It polls the stats provider at every `PollInterval` and then calls
// `notify()` to evaluate all Conditions against the newly polled stats.
func (m *DefaultMonitor) Start(ctx context.Context) error {
	if !m.markAsRunning() {
		return ErrAlreadyStarted
	}

	go func() {
		defer close(m.doneCh)

		for {
			// Reset the ticker before each loop to account
			// for the poll/notify/collect phase.
			m.pollTicker.Reset()

			select {
			// This case stops the Monitor when m.Stop() is called
			case <-m.stopCh:
				m.shutdown()
				return
			// This case stops the Monitor when the context is canceled
			case <-ctx.Done():
				m.shutdown()
				return
			case <-m.pollTicker.C():
				// We do not notify consumers when polled failed because we do not
				// have the latest stats to validate the Conditions
				err := m.poll()
				if err != nil {
					m.logger.WithError(err).Error("poll failed")
					continue
				}

				m.notify(ctx)
				m.setMetrics()
			}
		}
	}()
	return nil
}

// Stop will stop the Monitor and wait for a graceful shutdown to
// occur before returning.
func (m *DefaultMonitor) Stop() {
	// Return immediately if the Monitor is not running yet
	if !m.isRunning() {
		return
	}

	m.stopOnce.Do(func() {
		// m.stopCh should be available for sending a value
		// unless the context has been canceled, in which case
		// `Start` has already returned and `stopCh` has no more
		// listener.
		select {

		// If the monitor has already been closed by means of context cancellation
		// then we exit here. nothing else to do.
		case <-m.doneCh:
			return
		case m.stopCh <- struct{}{}:
			<-m.doneCh
		}
	})
}

// NotifyOn registers a list of Condition on the Monitor.
// The Conditions are evaluated at every PollInterval.
// The Monitor will notify the returned Event channel
// only when a condition is met.
func (m *DefaultMonitor) NotifyOn(conditions ...Condition) (<-chan Event, error) {
	// Make sure we do not create a consumer while the Monitor is shutting down
	// as this could lead to this channel not being closed on shutdown.
	m.shutdownMutex.Lock()
	defer m.shutdownMutex.Unlock()

	if m.isShuttingDown || !m.isRunning() {
		return nil, ErrNotRunning
	}

	// This is a bit arbitrary, but using a buffer of 2 to allow
	// for the previous value and the current one.
	out := make(chan Event, 2)

	// NotifyOn can be called concurrently from many places.
	// Hence, we must synchronize the writes here.
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	m.state.consumers = append(m.state.consumers, eventConsumer{
		out:        out,
		conditions: conditions,
	})
	return out, nil
}

// Describe is used to generate description information for each emitted Prometheus metrics
func (m *DefaultMonitor) Describe(ch chan<- *prometheus.Desc) {
	m.metrics.Describe(ch)
}

// Collect is used to collect the current values of each emitted Prometheus metrics
func (m *DefaultMonitor) Collect(ch chan<- prometheus.Metric) {
	m.metrics.Collect(ch)
}

// poll Polls the StatsProvider to get resource usage information.
// It then sets the previous and current Stats on the Monitor, which
// will be used to evaluate the consumer's Condition.
// This method is not thread safe
func (m *DefaultMonitor) poll() error {
	// Record the time right before polling
	now := time.Now()

	// Poll the cgroups stats
	cgroupStats, err := m.cgroupManager.Stats()
	if err != nil {
		return fmt.Errorf("polling cgroup stats failed: %w", err)
	}

	// There is no need to lock access to the state here because poll is called
	// synchronously before notify
	if m.state.lastPoll.IsZero() {
		// This is the first poll, set the previous state to the current state.
		m.state.previousStats.CGroup = cgroupStats
	} else {
		m.state.previousStats = m.state.currentStats
		m.state.lastPollInterval = now.Sub(m.state.lastPoll)
	}

	m.state.currentStats.CGroup = cgroupStats
	m.state.currentStats.pollTime = now
	m.state.lastPoll = now
	return nil
}

// notify calls each consumer in a loop and evaluate their
// condition. If the condition evaluates to `true`, an Event
// is sent down the consumer channel. To avoid the case where a
// consumer does not read properly its channel, which would block
// the current function, we discard the event if the channel is
// blocked for writing. It is the consumer's responsibility to
// read the channel properly.
func (m *DefaultMonitor) notify(ctx context.Context) (timeoutExpired bool) {
	// Consumers can be added via `NotifyOn()` concurrently
	// to this method. Let's make a copy of the consumers and
	// work with the copy. That way we don't hold the lock for
	// too long.
	m.stateLock.Lock()
	consumers := make([]eventConsumer, len(m.state.consumers))
	copy(consumers, m.state.consumers)
	m.stateLock.Unlock()

	// This is fine without lock, as `notify` is called synchronously after poll.
	prev, curr := m.state.previousStats, m.state.currentStats
	lastPoll := m.state.lastPollInterval

	notifyCtx, cancelNotify := context.WithTimeout(ctx, m.cfg.NotifyTimeout)
	// We don't use the `cancel` function. This call is only to release context's resources.
	// We let the timeout set on the context to expire, which also cancels the context.
	defer cancelNotify()

	wg := sync.WaitGroup{}
	for _, cs := range consumers {
		for _, cd := range cs.conditions {
			wg.Add(1)
			go func(consumer eventConsumer, condition Condition) {
				defer wg.Done()

				// This is for monitoring purposes. We want to log each Condition
				// that has not returned when the NotifyTimeout was reached.
				doneCh := make(chan struct{}, 1)
				go func() {
					select {
					case <-doneCh:
					case <-notifyCtx.Done():
						t := m.cfg.NotifyTimeout.String()
						d := condition.Name
						msg := fmt.Sprintf("LoadMonitor notify timeout (%q) reached! Condition %q did not complete in time.", t, d)
						m.logger.Warn(msg)
					}
				}()

				if ok, desc := condition.Fn(notifyCtx, prev, curr, lastPoll); ok {
					// The shutdown logic closes all consumers channel.
					// To avoid sending on a closed channel, we hold the
					// shutdown lock while sending an Event. Shutdown cannot
					// occur while we hold this lock.
					m.shutdownMutex.Lock()
					if !m.isShuttingDown && notifyCtx.Err() == nil {
						select {
						case consumer.out <- Event{
							ConditionName: condition.Name,
							Description:   desc,
							CurrentStats:  curr,
							PreviousStats: prev,
						}:
						default:
							// Discard the Event if the channel is full
							// or does not have a reader.
						}
					}
					m.shutdownMutex.Unlock()
				}
				doneCh <- struct{}{}
			}(cs, cd)
		}
	}

	wgDoneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDoneCh)
	}()

	select {
	case <-wgDoneCh:
		return false
	case <-notifyCtx.Done():
		t := m.cfg.NotifyTimeout.String()
		msg := fmt.Sprintf("LoadMonitor notify timeout (%q) reached: some conditions might not have finished in time.", t)
		m.logger.Warn(msg)
		return true
	}
}

// collect collects Prometheus metrics from the polled stats
func (m *DefaultMonitor) setMetrics() {
	m.metrics.setStats(m.state.currentStats)
}

// shutdown closes all consumer's channel to notify
// them that the Manager has been closed.
func (m *DefaultMonitor) shutdown() {
	m.shutdownOnce.Do(func() {
		m.started.Store(false)

		m.shutdownMutex.Lock()
		m.isShuttingDown = true
		m.stateLock.Lock()

		for _, c := range m.state.consumers {
			close(c.out)
		}

		m.stateLock.Unlock()
		m.shutdownMutex.Unlock()
	})
}

// markAsRunning returns true if the Monitor can be started, false otherwise.
// Currently, it only makes sure Start is not called twice using an atomic value.
func (m *DefaultMonitor) markAsRunning() bool {
	return m.started.CompareAndSwap(false, true)
}

// isRunning returns true if the Monitor is already running
func (m *DefaultMonitor) isRunning() bool {
	return m.started.Load()
}
