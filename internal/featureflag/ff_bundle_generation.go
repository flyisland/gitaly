package featureflag

// BundleGeneration enables bundle generation
var BundleGeneration = NewFeatureFlag(
	"bundle_generation",
	"v17.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6542",
	false,
)
