package featureflag

// PraefectSerializedWrite enables the write serialization in Praefect
var PraefectSerializedWrite = NewFeatureFlag(
	"praefect_serialized_write",
	"v19.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/7059",
	false,
)
