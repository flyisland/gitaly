package offloading

import (
	"math/rand"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backoff"
)

var (
	// defaultOverallTimeout is the default overall timeout for the sink.
	defaultOverallTimeout = 1 * time.Minute
	// defaultMaxRetry is the default max backoffStrategy for the sink.
	defaultMaxRetry = uint(2)
	// defaultRetryTimeout is the default backoffStrategy timeout for the sink.
	defaultRetryTimeout = 15 * time.Second
	// defaultBackoffStrategy is the default backoff strategy for the sink.
	defaultBackoffStrategy = backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano())))
)

type sinkCfg struct {
	overallTimeout  time.Duration
	maxRetry        uint
	noRetry         bool
	retryTimeout    time.Duration
	backoffStrategy backoff.Strategy
}

// SinkOption modifies a sink configuration.
type SinkOption func(*sinkCfg)

// WithOverallTimout sets the overallTimeout.
// The overallTimeout specifies the maximum allowed duration for the entire operation,
// including all retries. This ensures the operation completes or fails within a bounded time.
func WithOverallTimout(timeout time.Duration) SinkOption {
	return func(s *sinkCfg) {
		s.overallTimeout = timeout
	}
}

// WithMaxRetry sets the max backoffStrategy attempt. Use WithNoRetry if you do not want any retry.
// If the operation supports retries, it will attempt the operation up to maxRetry times
// with retryTimeout intervals between each attempt. Once the overallTimeout is exceeded,
// the operation will be terminated even if maxRetry attempts not been exhausted.
func WithMaxRetry(max uint) SinkOption {
	return func(s *sinkCfg) {
		s.maxRetry = max
	}
}

// WithNoRetry disables the backoffStrategy for the sink.
func WithNoRetry() SinkOption {
	return func(s *sinkCfg) {
		s.maxRetry = 0
		s.noRetry = true
	}
}

// WithRetryTimeout sets the retryTimeout.
// The retryTimeout specifies the maximum time to wait for each individual backoffStrategy attempt.
// This is useful for handling transient failures in a single operation while respecting
// the overall timeout.
func WithRetryTimeout(duration time.Duration) SinkOption {
	return func(s *sinkCfg) {
		s.retryTimeout = duration
	}
}

// WithBackoffStrategy sets the backoff strategy for retrying failed operations.
func WithBackoffStrategy(strategy backoff.Strategy) SinkOption {
	return func(s *sinkCfg) {
		s.backoffStrategy = strategy
	}
}
