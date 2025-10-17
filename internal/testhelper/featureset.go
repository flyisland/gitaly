package testhelper

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
)

// FeatureSet is a representation of a set of features that should be disabled.
// This is useful in situations where a test needs to test any combination of features toggled on and off.
// It is designed to disable features as all features are enabled by default, please see: testhelper.Context()
type FeatureSet struct {
	features map[featureflag.FeatureFlag]bool
}

// Desc describes the feature such that it is suitable as a testcase description.
func (f FeatureSet) Desc() string {
	features := make([]string, 0, len(f.features))

	for feature, enabled := range f.features {
		features = append(features, fmt.Sprintf("%s=%s", feature.Name, strconv.FormatBool(enabled)))
	}

	sort.Strings(features)

	return strings.Join(features, ",")
}

// Apply applies all feature flags in the given FeatureSet to the given context.
func (f FeatureSet) Apply(ctx context.Context) context.Context {
	for feature, enabled := range f.features {
		ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, feature, enabled)
		ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, feature, enabled)
	}
	return ctx
}

// FeatureSets is a slice containing many FeatureSets
type FeatureSets []FeatureSet

// NewFeatureSets takes Go feature flags and returns the combination of FeatureSets.
func NewFeatureSets(features ...featureflag.FeatureFlag) FeatureSets {
	// We want to generate all combinations of feature flags, which is 2^len(flags).
	// To do so, we simply iterate through all indices. For each iteration, a
	// feature flag is set if its corresponding bit at the current index is 1,
	// otherwise it's left out of the set.
	length := 1 << len(features)
	sets := make(FeatureSets, length)

	for i := range sets {
		featureMap := make(map[featureflag.FeatureFlag]bool)

		for j, feature := range features {
			featureMap[feature] = ((uint(i) >> uint(j)) & 1) == 1
		}

		sets[i].features = featureMap
	}

	return sets
}

// Run executes the given test function for each of the FeatureSets. The passed in context has the
// feature flags set accordingly.
func (s FeatureSets) Run(t *testing.T, test func(t *testing.T, ctx context.Context)) {
	t.Helper()

	for _, featureSet := range s {
		t.Run(featureSet.Desc(), func(t *testing.T) {
			test(t, featureSet.Apply(Context(t)))
		})
	}
}

// Bench executes the given benchmarking function for each of the FeatureSets. The passed in
// context has the feature flags set accordingly.
func (s FeatureSets) Bench(b *testing.B, test func(b *testing.B, ctx context.Context)) {
	b.Helper()

	for _, featureSet := range s {
		b.Run(featureSet.Desc(), func(b *testing.B) {
			test(b, featureSet.Apply(Context(b)))
		})
	}
}

// EnabledOrDisabledFlag returns either the enabled value or the disabled value depending on the
// feature flag's state.
func EnabledOrDisabledFlag[T any](ctx context.Context, flag featureflag.FeatureFlag, enabled, disabled T) T {
	if flag.IsEnabled(ctx) {
		return enabled
	}
	return disabled
}
