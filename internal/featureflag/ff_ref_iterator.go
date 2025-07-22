package featureflag

// RefIterator enables the use of a ref iterator that parses the output of
// git-for-each-ref --format, instead of calling git cat-file to get individual
// commit information.
var RefIterator = NewFeatureFlag(
	"ref_iterator",
	"v18.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6839",
	true,
)
