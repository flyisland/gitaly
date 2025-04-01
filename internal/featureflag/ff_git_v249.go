package featureflag

// GitV249 enables the use Git v2.49.
var GitV249 = NewFeatureFlag(
	"git_v249",
	"v17.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6690",
	false,
)
