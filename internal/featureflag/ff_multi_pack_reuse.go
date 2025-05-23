package featureflag

// MultiPackReuse enables reusing objects across multiple packfiles. This feature should decrease the time required to
// compute packfiles.
var MultiPackReuse = NewFeatureFlag(
	"multi_pack_reuse",
	"v18.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6764",
	false,
)
