package limiter

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server/auth"
)

// DualLimiter is a limiter that contains two separate limiters: one for authenticated
// requests and one for unauthenticated requests. It determines which limiter to use
// based on whether the request is authenticated and whether the LimitUnauthenticated
// feature flag is enabled.
type DualLimiter struct {
	authenticated   Limiter
	unauthenticated Limiter
}

// NewDualLimiter creates a new DualLimiter with separate limiters for authenticated
// and unauthenticated requests.
func NewDualLimiter(authenticated, unauthenticated Limiter) *DualLimiter {
	return &DualLimiter{
		authenticated:   authenticated,
		unauthenticated: unauthenticated,
	}
}

// Limit implements the Limiter interface. It selects the appropriate limiter based on
// whether the LimitUnauthenticated feature flag is enabled, and if so, whether the
// request is authenticated. It then delegates the limiting to the selected limiter.
func (d *DualLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	limiter := d.authenticated

	// Only use the unauthenticated limiter if the feature flag is enabled
	if featureflag.LimitUnauthenticated.IsEnabled(ctx) && !auth.IsAuthenticated(ctx) {
		limiter = d.unauthenticated
	}

	return limiter.Limit(ctx, lockKey, f)
}
