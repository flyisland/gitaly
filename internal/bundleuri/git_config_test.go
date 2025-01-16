package bundleuri

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestCapabilitiesGitConfig(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.BundleURI,
	).Run(t, testCapabilitiesGitConfig)
}

func testCapabilitiesGitConfig(t *testing.T, ctx context.Context) {
	tests := []struct {
		desc     string
		enabled  bool
		expected []gitcmd.ConfigPair
	}{
		{
			desc:    "when bundle-uri enabled",
			enabled: true,
			expected: []gitcmd.ConfigPair{
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "true",
				},
			},
		},
		{
			desc:    "when bundle-uri disabled",
			enabled: false,
			expected: []gitcmd.ConfigPair{
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "false",
				},
			},
		},
	}
	for _, tt := range tests {
		got := CapabilitiesGitConfig(ctx, tt.enabled)
		if featureflag.BundleURI.IsEnabled(ctx) {
			require.ElementsMatch(t, tt.expected, got)
		} else {
			require.ElementsMatch(t, []gitcmd.ConfigPair{
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "false",
				},
			}, got)
		}

	}
}

func TestUploadPackGitConfig(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.BundleURI,
	).Run(t, testUploadPackGitConfig)
}

func testUploadPackGitConfig(t *testing.T, ctx context.Context) {
	tests := []struct {
		desc     string
		uri      string
		expected []gitcmd.ConfigPair
	}{
		{
			desc: "should return all config",
			uri:  "https://example.com/bundle.git",
			expected: []gitcmd.ConfigPair{
				{
					Key:   "bundle.version",
					Value: "1",
				},
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "true",
				},
				{
					Key:   "bundle.mode",
					Value: "all",
				},
				{
					Key:   "bundle.heuristic",
					Value: "creationToken",
				},
				{
					Key:   "bundle.default.uri",
					Value: "https://example.com/bundle.git",
				},
				{
					Key:   "bundle.default.creationToken",
					Value: "1",
				},
			},
		},
		{
			desc: "empty uri should disable advertising bundle",
			uri:  "",
			expected: []gitcmd.ConfigPair{
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "false",
				},
			},
		},
	}
	for _, tt := range tests {
		got := UploadPackGitConfig(ctx, tt.uri)
		if featureflag.BundleURI.IsEnabled(ctx) {
			require.ElementsMatch(t, tt.expected, got)
		} else {
			require.ElementsMatch(t, []gitcmd.ConfigPair{
				{
					Key:   "uploadpack.advertiseBundleURIs",
					Value: "false",
				},
			}, got)
		}
	}
}
