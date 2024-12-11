package featureflag

// VerbatimPatchID enables the use of --verbatim option so that whitespaces would not be stripped when generating a patch-id.
var VerbatimPatchID = NewFeatureFlag(
	"verbatim_patch_id",
	"v17.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6537",
	false,
)
