package prometheus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/duration"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		cfg         Config
		expectedErr error
	}{
		{
			name: "empty",
			cfg:  Config{},
		},
		{
			name: "no buckets",
			cfg:  Config{ScrapeTimeout: duration.Duration(-1)},
		},
		{
			name: "invalid",
			cfg: Config{
				ScrapeTimeout:      duration.Duration(-1),
				GRPCLatencyBuckets: []float64{10, -4.2},
			},
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("%w: -1ns is not greater than or equal to 0s", cfgerror.ErrNotInRange),
					"scrape_timeout",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: expected asc: [10 -4.2]", cfgerror.ErrBadOrder),
					"grpc_latency_buckets",
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
