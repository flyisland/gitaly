package featureflag

// DryRunMigrations is used to enable the dry-run of migrations.
var DryRunMigrations = NewFeatureFlag(
	"dryrun_migrations",
	"v17.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/6557",
	false,
)
