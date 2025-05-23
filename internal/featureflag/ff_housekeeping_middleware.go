package featureflag

// HousekeepingMiddleware enables the housekeeping scheduler middleware.
var HousekeepingMiddleware = NewFeatureFlag(
	"housekeeping_middleware",
	"v18.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6761",
	false,
)
