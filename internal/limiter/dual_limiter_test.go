package limiter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

type mockLimiter struct {
	limitFunc func(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error)
}

func (m *mockLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	if m.limitFunc != nil {
		return m.limitFunc(ctx, lockKey, f)
	}
	return f()
}

func TestDualLimiter_Limit(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		featureFlagEnabled  bool
		setupContext        func(t *testing.T) context.Context
		expectedLimiterUsed string
	}{
		{
			name:               "authenticated request uses authenticated limiter with feature flag enabled",
			featureFlagEnabled: true,
			setupContext: func(t *testing.T) context.Context {
				ctx := testhelper.Context(t)
				// Set up authenticated context with username in metadata
				md := metadata.New(map[string]string{
					"username": "test-user",
				})
				return metadata.NewIncomingContext(ctx, md)
			},
			expectedLimiterUsed: "authenticated",
		},
		{
			name:               "unauthenticated request uses unauthenticated limiter with feature flag enabled",
			featureFlagEnabled: true,
			setupContext: func(t *testing.T) context.Context {
				// Context without authentication (no username in metadata)
				return testhelper.Context(t)
			},
			expectedLimiterUsed: "unauthenticated",
		},
		{
			name:               "authenticated request uses authenticated limiter with feature flag disabled",
			featureFlagEnabled: false,
			setupContext: func(t *testing.T) context.Context {
				ctx := testhelper.Context(t)
				md := metadata.New(map[string]string{
					"username": "test-user",
				})
				return metadata.NewIncomingContext(ctx, md)
			},
			expectedLimiterUsed: "authenticated",
		},
		{
			name:               "unauthenticated request uses authenticated limiter with feature flag disabled",
			featureFlagEnabled: false,
			setupContext: func(t *testing.T) context.Context {
				return testhelper.Context(t)
			},
			expectedLimiterUsed: "authenticated",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var usedLimiter string

			authenticatedLimiter := &mockLimiter{
				limitFunc: func(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
					usedLimiter = "authenticated"
					return f()
				},
			}

			unauthenticatedLimiter := &mockLimiter{
				limitFunc: func(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
					usedLimiter = "unauthenticated"
					return f()
				},
			}

			dualLimiter := NewDualLimiter(authenticatedLimiter, unauthenticatedLimiter)

			ctx := tc.setupContext(t)
			ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.LimitUnauthenticated, tc.featureFlagEnabled)

			result, err := dualLimiter.Limit(ctx, "test-key", func() (interface{}, error) {
				return "success", nil
			})

			require.NoError(t, err)
			require.Equal(t, "success", result)
			require.Equal(t, tc.expectedLimiterUsed, usedLimiter)
		})
	}
}

func TestDualLimiter_PropagatesErrors(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("limiter error")

	t.Run("authenticated limiter error", func(t *testing.T) {
		t.Parallel()

		authenticatedLimiter := &mockLimiter{
			limitFunc: func(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
				return nil, expectedErr
			},
		}

		unauthenticatedLimiter := &mockLimiter{}

		dualLimiter := NewDualLimiter(authenticatedLimiter, unauthenticatedLimiter)

		ctx := testhelper.Context(t)
		ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.LimitUnauthenticated, true)
		md := metadata.New(map[string]string{
			"username": "test-user",
		})
		ctx = metadata.NewIncomingContext(ctx, md)

		result, err := dualLimiter.Limit(ctx, "test-key", func() (interface{}, error) {
			return "should not be called", nil
		})

		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, result)
	})

	t.Run("unauthenticated limiter error", func(t *testing.T) {
		t.Parallel()

		authenticatedLimiter := &mockLimiter{}

		unauthenticatedLimiter := &mockLimiter{
			limitFunc: func(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
				return nil, expectedErr
			},
		}

		dualLimiter := NewDualLimiter(authenticatedLimiter, unauthenticatedLimiter)

		ctx := testhelper.Context(t)
		ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.LimitUnauthenticated, true)

		result, err := dualLimiter.Limit(ctx, "test-key", func() (interface{}, error) {
			return "should not be called", nil
		})

		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, result)
	})
}
