package featureflag

// LimitUnauthenticated allows the concurrency limiter to limit unauthenticated
// requests separately from authenticated requests.
var LimitUnauthenticated = NewFeatureFlag(
	"limit_unauthenticated",
	"v18.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6955",
	true,
)
