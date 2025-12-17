package featureflag

// SnapshotDryRunStats enables the dry-run statistics middleware.
var SnapshotDryRunStats = NewFeatureFlag(
	"snapshot_dry_run_stats",
	"v18.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/7018",
	false,
)
