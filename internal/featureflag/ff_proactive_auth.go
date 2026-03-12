package featureflag

// FetchRemoteProactiveAuth enables proactive HTTP basic authentication for FetchRemote when credentials are embedded in the URL.
var FetchRemoteProactiveAuth = NewFeatureFlag(
	"fetch_remote_proactive_auth",
	"v18.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/work_items/7115",
	false,
)
