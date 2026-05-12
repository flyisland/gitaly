package limiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	// MaximumWatcherTimeout is the number of maximum allowed timeout when polling backoff events from watchers.
	// When this threshold is reached, a timeout polling is treated as a backoff event.
	MaximumWatcherTimeout = 5
	// DefaultCalibrateFrequency is the default time period between two calibrations.
	DefaultCalibrateFrequency = 30 * time.Second
	// DefaultBackoffFactor is the default recommended backoff factor when the concurrency decreases. By default,
	// the factor is 0.5, meaning the limit is cut off by half when a backoff event occurs.
	DefaultBackoffFactor = 0.5
)

// BackoffEvent is a signal that the current system is under pressure. It's returned by the watchers under the
// management of the AdaptiveCalculator at calibration points.
type BackoffEvent struct {
	ConditionName string
	Reason        string
	ShouldBackoff bool
	PreviousStats loadmonitor.Stats
	CurrentStats  loadmonitor.Stats
}

// Config is the configuration needed to create an AdaptativeCalculator
type Config struct {
	CalibrationInterval time.Duration
	CPUThreshold        float64
	MemoryThreshold     float64
	PSIConfig           config.PSIPressureConfig
}

// AdaptiveCalculator is responsible for calculating the adaptive limits based on additive increase/multiplicative
// decrease (AIMD) algorithm. This method involves gradually increasing the limit during normal process functioning
// but quickly reducing it when an issue (backoff event) occurs. It receives a list of AdaptiveLimiter and a list of
// ResourceWatcher. Although the limits may have different settings (Initial, Min, Max, BackoffFactor), they all move
// as a whole. The caller accesses the current limits via AdaptiveLimiter.Current method.
//
// When the calculator starts, each limit value is set to its Initial limit. Periodically, the calculator polls the
// backoff events from the watchers. The current value of each limit is re-calibrated as follows:
// * limit = limit + 1 if there is no backoff event since the last calibration. The new limit cannot exceed max limit.
// * limit = limit * BackoffFactor otherwise. The new limit cannot be lower than min limit.
//
// A watcher returning an error is treated as a no backoff event.
type AdaptiveCalculator struct {
	// cfg is the configuration for the calculator
	cfg Config
	// logger is the logger used to log backoff events and other related
	// event happening in the calculator
	logger log.Logger
	// started tells whether the calculator already starts. One calculator is allowed to be used once.
	started bool
	// limits are the list of adaptive limits managed by this calculator.
	limits []AdaptiveLimiter
	// lastBackoffEvent stores the last backoff event collected from the watchers.
	lastBackoffEvent *BackoffEvent
	// tickerCreator is a custom function that returns a Ticker. It's mostly used in test the manual ticker
	tickerCreator func(duration time.Duration) helper.Ticker
	// currentLimitVec is the gauge of current limit value of an adaptive concurrency limit
	currentLimitVec *prometheus.GaugeVec
	// backoffEventsVec is the counter of the total number of backoff events
	backoffEventsVec *prometheus.CounterVec
	// mu is used to synchronize access to the calculator state
	stateMu sync.Mutex
	// backoffEventMu is a mutex to synchronize access solely to the lastBackoffEvent
	backoffEventMu sync.Mutex
	// loadMonitor is the load monitor used to get resource usage events
	loadMonitor loadmonitor.Monitor
	// eventCh is a channel returned by the load monitor used to listen on
	// resource usage event.
	eventCh <-chan loadmonitor.Event
}

// NewAdaptiveCalculator constructs a AdaptiveCalculator object. It's the responsibility of the caller to validate
// the correctness of input AdaptiveLimiter and ResourceWatcher.
func NewAdaptiveCalculator(cfg Config, logger log.Logger, loadMonitor loadmonitor.Monitor, limits []AdaptiveLimiter) *AdaptiveCalculator {
	return &AdaptiveCalculator{
		cfg:         cfg,
		logger:      logger,
		loadMonitor: loadMonitor,
		limits:      limits,
		currentLimitVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_concurrency_limiting_current_limit",
				Help: "The current limit value of an adaptive concurrency limit",
			},
			[]string{"limit"},
		),
		backoffEventsVec: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_concurrency_limiting_backoff_events_total",
				Help: "Counter of the total number of backoff events",
			},
			[]string{"watcher"},
		),
	}
}

// Start resets the current limit values and start a goroutine to poll the backoff events. This method exits after the
// mentioned goroutine starts.
func (c *AdaptiveCalculator) Start(ctx context.Context) (func(), error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	if c.started {
		return nil, fmt.Errorf("adaptive calculator: already started")
	}
	c.started = true

	// Reset all limits to their initial limits
	for _, limit := range c.limits {
		c.updateLimit(limit, limit.Setting().Initial)
	}

	done := make(chan struct{})
	started := make(chan struct{})
	completed := make(chan struct{})

	eventCh, err := c.loadMonitor.NotifyOn(
		newCPUThrottlingCondition(c.cfg.CPUThreshold),
		newMemoryUsageCondition(c.cfg.MemoryThreshold),
		newCgroupPressureConditionBuilder(c.cfg.PSIConfig.CPU, c.logger, pressureResourceCPU).Condition(),
		newCgroupPressureConditionBuilder(c.cfg.PSIConfig.Memory, c.logger, pressureResourceMemory).Condition(),
		newCgroupPressureConditionBuilder(c.cfg.PSIConfig.IO, c.logger, pressureResourceIO).Condition(),
	)
	if err != nil {
		return nil, fmt.Errorf("adaptive calculator: error registering conditions: %w", err)
	}

	c.eventCh = eventCh

	go func(ctx context.Context) {
		defer close(completed)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		tickerCreator := c.tickerCreator
		if tickerCreator == nil {
			tickerCreator = helper.NewTimerTicker
		}
		timer := tickerCreator(c.cfg.CalibrationInterval)

		// This function will wait on the Condition Event channel and set a backoff event
		// everytime an event is emitted.
		// Usually, resources are highly correlated. When the memory level raises too high,
		// the CPU usage also increases due to page faulting, memory reclaim, GC activities, etc.
		// We might also have multiple Conditions for the same resources, for example, memory
		// usage and page fault. Hence, re-calibrating after each event will
		// cut the limits too aggressively.
		// To avoid cutting the limits too aggressively for a surge of co-related event, the
		// for loop below will evaluate backoff events only at some interval.
		go c.waitForEvents(ctx)

		// Let's signal the start after initializing local variables
		close(started)

		for {
			// Reset the timer to the next calibration point.
			// It accounts for the resource polling latencies.
			timer.Reset()
			select {
			case <-timer.C():
				c.calibrateLimits(ctx)
				c.setLastBackoffEvent(nil)
			case <-done:
				timer.Stop()
				return
			}
		}
	}(ctx)

	<-started
	return func() {
		close(done)
		<-completed
	}, nil
}

// Describe is used to describe Prometheus metrics.
func (c *AdaptiveCalculator) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *AdaptiveCalculator) Collect(metrics chan<- prometheus.Metric) {
	c.currentLimitVec.Collect(metrics)
	c.backoffEventsVec.Collect(metrics)
}

// waitForEvents listens on the event channel from the load monitor
// and sets the last backoff event only when an event is received.
func (c *AdaptiveCalculator) waitForEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case e := <-c.eventCh:
			c.setLastBackoffEvent(&BackoffEvent{
				ConditionName: e.ConditionName,
				Reason:        e.Description,
				ShouldBackoff: true,
				CurrentStats:  e.CurrentStats,
				PreviousStats: e.PreviousStats,
			})
		}
	}
}

// calibrateLimits reads the lastBackoffEvent and calibrates the limits accordingly.
func (c *AdaptiveCalculator) calibrateLimits(ctx context.Context) {
	// Make a copy of the last backoff event and unlock
	// the mutex so that we don't block `waitForEvents`
	// when a new event comes in.
	var backoffEvent *BackoffEvent
	c.backoffEventMu.Lock()
	backoffEvent = c.lastBackoffEvent
	c.backoffEventMu.Unlock()

	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	if ctx.Err() != nil {
		return
	}

	for _, limit := range c.limits {
		setting := limit.Setting()

		var newLimit int
		logger := c.logger.WithField("limit_rpc", limit.Name())

		if backoffEvent == nil {
			// Additive increase, one unit at a time
			newLimit = limit.Current() + 1
			if newLimit > setting.Max {
				newLimit = setting.Max
			}
			logger.WithFields(map[string]interface{}{
				"previous_limit": limit.Current(),
				"new_limit":      newLimit,
			}).Debug("Additive increase")
		} else {
			// Multiplicative decrease
			newLimit = int(math.Floor(float64(limit.Current()) * setting.BackoffFactor))
			if newLimit < setting.Min {
				newLimit = setting.Min
			}
			fields := map[string]interface{}{
				"previous_limit": limit.Current(),
				"new_limit":      newLimit,
				"condition":      backoffEvent.ConditionName,
				"reason":         backoffEvent.Reason,
			}
			loggingStats := buildBackoffStats(c.cfg, *backoffEvent)
			for key, value := range loggingStats {
				fields[fmt.Sprintf("stats.%s", key)] = value
			}
			logger.WithFields(fields).Info("Multiplicative decrease")
		}
		c.updateLimit(limit, newLimit)
	}
}

func (c *AdaptiveCalculator) setLastBackoffEvent(event *BackoffEvent) {
	c.backoffEventMu.Lock()
	c.lastBackoffEvent = event
	c.backoffEventMu.Unlock()

	if event != nil && event.ShouldBackoff {
		c.backoffEventsVec.WithLabelValues(event.ConditionName).Inc()
	}
}

func (c *AdaptiveCalculator) updateLimit(limit AdaptiveLimiter, newLimit int) {
	limit.Update(newLimit)
	c.currentLimitVec.WithLabelValues(limit.Name()).Set(float64(newLimit))
}

func buildBackoffStats(cfg Config, event BackoffEvent) map[string]any {
	anonRatio := 0.0
	stats := event.CurrentStats.CGroup.ParentStats
	if event.CurrentStats.CGroup.ParentStats.MemoryLimit > 0 {
		anonRatio = float64(stats.TotalAnon) / float64(stats.MemoryLimit)
	}

	m := map[string]any{
		"memory_usage":                stats.MemoryUsage,
		"memory_limit":                stats.MemoryLimit,
		"inactive_file":               stats.TotalInactiveFile,
		"anon":                        stats.TotalAnon,
		"anon_ratio":                  anonRatio,
		"memory_high_events":          stats.MemoryHighEvents,
		"memory_max_events":           stats.MemoryMaxEvents,
		"oom_kills":                   stats.OOMKills,
		"memory_pressure_some_avg10":  stats.MemoryPSI.Some.Avg10,
		"memory_pressure_some_avg60":  stats.MemoryPSI.Some.Avg60,
		"memory_pressure_some_avg300": stats.MemoryPSI.Some.Avg300,
		"memory_pressure_full_avg10":  stats.MemoryPSI.Full.Avg10,
		"memory_pressure_full_avg60":  stats.MemoryPSI.Full.Avg60,
		"memory_pressure_full_avg300": stats.MemoryPSI.Full.Avg300,
		"io_pressure_some_avg10":      stats.IOPSI.Some.Avg10,
		"io_pressure_some_avg60":      stats.IOPSI.Some.Avg60,
		"io_pressure_some_avg300":     stats.IOPSI.Some.Avg300,
		"io_pressure_full_avg10":      stats.IOPSI.Full.Avg10,
		"io_pressure_full_avg60":      stats.IOPSI.Full.Avg60,
		"io_pressure_full_avg300":     stats.IOPSI.Full.Avg300,
		"cpu_pressure_some_avg10":     stats.CPUPSI.Some.Avg10,
		"cpu_pressure_some_avg60":     stats.CPUPSI.Some.Avg60,
		"cpu_pressure_some_avg300":    stats.CPUPSI.Some.Avg300,
		"pgmajfault":                  stats.PgMajFault,
	}

	switch event.ConditionName {
	case conditionCgroupCPU:
		previous, current := event.PreviousStats, event.CurrentStats
		throttledDuration := current.CGroup.ParentStats.CPUThrottledDuration - previous.CGroup.ParentStats.CPUThrottledDuration
		timeDiff := current.PollTime().Sub(previous.PollTime()).Abs().Seconds()

		m["time_diff"] = timeDiff
		m["throttled_duration"] = throttledDuration
		m["throttled_threshold"] = cfg.CPUThreshold
	case conditionCgroupMemory:
		m["memory_threshold"] = cfg.MemoryThreshold
	}

	return m
}
