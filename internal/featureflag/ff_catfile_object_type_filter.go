package featureflag

// CatfileObjectTypeFilter enables git-cat-file(1) to filter objects via the new `--filter=object:type` option. This
// should lead to a significant speedup for certain RPCs.
var CatfileObjectTypeFilter = NewFeatureFlag(
	"catfile_object_type_filter",
	"v18.0.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6720",
	true,
)
