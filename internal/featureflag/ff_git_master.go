package featureflag

// GitMaster enables the use of git's master version in Gitaly.
var GitMaster = NewFeatureFlag(
	"git_master",
	"v18.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6868",
	false,
)
