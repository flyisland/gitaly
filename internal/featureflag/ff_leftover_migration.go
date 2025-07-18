package featureflag

// LeftoverMigration enables the feature to delete garbage files in repositories that Gitaly doesn't use.
var LeftoverMigration = NewFeatureFlag(
	"leftover_migration",
	"v18.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5737",
	false,
)
