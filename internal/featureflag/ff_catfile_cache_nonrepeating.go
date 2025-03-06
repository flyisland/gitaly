package featureflag

// CatfileCacheNonrepeating generates a non-repeating cat-file cache keys.
var CatfileCacheNonrepeating = NewFeatureFlag(
	"catfile_cache_nonrepeating",
	"v17.10.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6666",
	false,
)
