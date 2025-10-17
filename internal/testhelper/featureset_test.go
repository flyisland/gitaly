package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ff "gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"google.golang.org/grpc/metadata"
)

func TestFeatureSets_Run(t *testing.T) {
	// Define two default-enabled feature flags. Note that with `NewFeatureFlag()`, we
	// automatically add them to the list of defined feature flags. While this is stateful and
	// would theoretically also impact other tests, we don't really need to mind that given
	// that we use test-specific names for the flags here.
	featureFlagA := ff.NewFeatureFlag("global_feature_flag_a", "", "", true)
	featureFlagB := ff.NewFeatureFlag("global_feature_flag_b", "", "", true)

	var featureFlags [][2]bool
	NewFeatureSets(featureFlagB, featureFlagA).Run(t, func(t *testing.T, ctx context.Context) {
		incomingMD, ok := metadata.FromIncomingContext(ctx)
		require.True(t, ok)

		outgoingMD, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)

		require.Equal(t, incomingMD, outgoingMD)

		featureFlags = append(featureFlags, [2]bool{
			featureFlagA.IsEnabled(ctx), featureFlagB.IsEnabled(ctx),
		})
	})

	require.Equal(t, [][2]bool{
		{false, false},
		{false, true},
		{true, false},
		{true, true},
	}, featureFlags)
}
