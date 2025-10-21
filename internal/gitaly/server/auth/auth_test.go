package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestIsAuthenticated(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc          string
		setupContext  func() context.Context
		authenticated bool
	}{
		{
			desc: "authenticated with username in metadata",
			setupContext: func() context.Context {
				ctx := testhelper.Context(t)
				md := metadata.New(map[string]string{
					"username": "test-user",
				})
				return metadata.NewIncomingContext(ctx, md)
			},
			authenticated: true,
		},
		{
			desc: "authenticated with multiple usernames in metadata",
			setupContext: func() context.Context {
				ctx := testhelper.Context(t)
				md := metadata.MD{
					"username": []string{"user1", "user2"},
				}
				return metadata.NewIncomingContext(ctx, md)
			},
			authenticated: true,
		},
		{
			desc: "not authenticated without username in metadata",
			setupContext: func() context.Context {
				ctx := testhelper.Context(t)
				md := metadata.New(map[string]string{
					"other-key": "other-value",
				})
				return metadata.NewIncomingContext(ctx, md)
			},
			authenticated: false,
		},
		{
			desc: "not authenticated with empty username",
			setupContext: func() context.Context {
				ctx := testhelper.Context(t)
				md := metadata.MD{
					"username": []string{},
				}
				return metadata.NewIncomingContext(ctx, md)
			},
			authenticated: false,
		},
		{
			desc: "not authenticated with empty metadata",
			setupContext: func() context.Context {
				ctx := testhelper.Context(t)
				md := metadata.New(map[string]string{})
				return metadata.NewIncomingContext(ctx, md)
			},
			authenticated: false,
		},
		{
			desc: "not authenticated without metadata",
			setupContext: func() context.Context {
				return testhelper.Context(t)
			},
			authenticated: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := tc.setupContext()
			result := IsAuthenticated(ctx)

			require.Equal(t, tc.authenticated, result)
		})
	}
}
