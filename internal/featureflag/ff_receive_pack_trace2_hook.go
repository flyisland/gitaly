package featureflag

// ReceivePackTrace2Hook enables the receive-pack trace2 hook to record detailed
// information during receive-pack.
var ReceivePackTrace2Hook = NewFeatureFlag(
	"receive_pack_trace2_hook",
	"v18.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6875",
	false,
)
