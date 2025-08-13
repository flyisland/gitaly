package featureflag

// SnapshotDryRunStats enables the dry-run mode for snapshot statistics collection.
// This feature allows collecting snapshot statistics (file_count, directory_count)
// without creating actual transaction snapshots, reducing performance overhead.
// It only runs when transactions are disabled and this feature flag is enabled.
var SnapshotDryRunStats = NewFeatureFlag(
	"snapshot_dry_run_stats",
	"v18.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6865",
	false,
)
