package featureflag

// GitV250 enables the use Git v2.50.
var GitV250 = NewFeatureFlag(
	"git_v250",
	"v18.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6827",
	true,
)
