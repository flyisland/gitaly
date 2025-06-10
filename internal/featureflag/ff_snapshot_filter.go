package featureflag

// SnapshotFilter enables the snapshot filtering feature,
// which determines which repository files should be included in a snapshot.
var SnapshotFilter = NewFeatureFlag(
	"snapshot_filter",
	"v18.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5737",
	false,
)
