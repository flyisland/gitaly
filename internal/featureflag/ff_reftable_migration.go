package featureflag

// ReftableMigration enables the reftable migration.
var ReftableMigration = NewFeatureFlag(
	"reftable_migration",
	"v17.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6521",
	false,
)
