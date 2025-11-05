package featureflag

// GitLastModified enables the use of native git-last-modified(1)
var GitLastModified = NewFeatureFlag(
	"git_last_modified",
	"v18.5",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6911",
	false,
)
