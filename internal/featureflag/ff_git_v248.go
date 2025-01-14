package featureflag

// GitV248 enables the use Git v2.48.
var GitV248 = NewFeatureFlag(
	"git_v248",
	"v17.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6577",
	false,
)
