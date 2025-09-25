package bundleuri

import (
	"context"
	"fmt"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
)

// CapabilitiesGitConfig returns a slice of gitcmd.ConfigPairs that can be injected
// into the Git config to enable or disable bundle-uri capability.
//
// If the feature flag is OFF, this config disables bundle-uri regardless of the `enable` parameter.
//
// Explicitly disabling bundle-uri capability is used during calls to git-upload-pack(1)
// when no bundle exists for the given repository. This prevents the client
// from requesting a non-existing URI.
//
// This function is also used when spawning git-upload-pack(1) --advertise-refs in
// response to the GET /info/refs request.
func CapabilitiesGitConfig(ctx context.Context, enable bool) []gitcmd.ConfigPair {
	cfg := gitcmd.ConfigPair{
		Key:   "uploadpack.advertiseBundleURIs",
		Value: strconv.FormatBool(enable),
	}

	if featureflag.BundleURI.IsDisabled(ctx) {
		cfg.Value = strconv.FormatBool(false)
	}
	return []gitcmd.ConfigPair{cfg}
}

// UploadPackGitConfig return a slice of gitcmd.ConfigPairs you can inject into the
// call to git-upload-pack(1) to advertise the available bundle to the client
// who clones/fetches from the repository.
func UploadPackGitConfig(ctx context.Context, signedURL string) []gitcmd.ConfigPair {
	if featureflag.BundleURI.IsDisabled(ctx) || signedURL == "" {
		return CapabilitiesGitConfig(ctx, false)
	}

	config := []gitcmd.ConfigPair{
		{
			Key:   "bundle.version",
			Value: "1",
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
			Key:   fmt.Sprintf("bundle.%s.uri", defaultBundle),
			Value: signedURL,
		},
		{
			// Gitaly uses only one bundle URI bundle, and it's a
			// bundle that contains the full history up to the point
			// when the bundle was generated. When the client is
			// enabled to download bundle URIs on git-fetch(1)
			// (see https://gitlab.com/gitlab-org/git/-/issues/271),
			// we want to avoid it to download that bundle, because
			// the client probably has most of the history already.
			//
			// Thus we set `creationToken` to a fixed value "1".
			// When Git fetches the bundle URI, it saved this value
			// to `fetch.bundleCreationToken` in the repository's
			// git config. The next time Git attempts to fetch
			// bundles, it ignores all bundles with `creationToken`
			// lower or equal to the saved value.
			Key:   fmt.Sprintf("bundle.%s.creationToken", defaultBundle),
			Value: "1",
		},
	}
	return append(config, CapabilitiesGitConfig(ctx, true)...)
}
