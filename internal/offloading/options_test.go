package offloading

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOffloadingSinkOptions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		options     []SinkOption
		expectedCfg sinkCfg
	}{
		{
			desc: "with default values",
			options: []SinkOption{
				WithOverallTimout(defaultOverallTimeout),
				WithMaxRetry(defaultMaxRetry),
				WithRetryTimeout(defaultRetryTimeout),
				WithBackoffStrategy(defaultBackoffStrategy),
			},
			expectedCfg: sinkCfg{
				overallTimeout:  defaultOverallTimeout,
				maxRetry:        defaultMaxRetry,
				retryTimeout:    defaultRetryTimeout,
				backoffStrategy: defaultBackoffStrategy,
			},
		},
		{
			desc: "with no backoffStrategy",
			options: []SinkOption{
				WithNoRetry(),
			},
			expectedCfg: sinkCfg{
				maxRetry: 0,
				noRetry:  true,
			},
		},
		{
			desc: "with customized values",
			options: []SinkOption{
				WithOverallTimout(20 * time.Second),
				WithMaxRetry(100),
				WithRetryTimeout(2 * time.Second),
				WithBackoffStrategy(&backoffStrategyInTest),
			},
			expectedCfg: sinkCfg{
				overallTimeout:  20 * time.Second,
				maxRetry:        100,
				retryTimeout:    2 * time.Second,
				backoffStrategy: &backoffStrategyInTest,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := sinkCfg{}
			for _, apply := range tc.options {
				apply(&cfg)
			}
			require.Equal(t, tc.expectedCfg, cfg)
		})
	}
}
