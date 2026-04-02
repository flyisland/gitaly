package featureflag

// BurdenMonitorTrackCommands enables tracking of spawned commands for burden monitoring purposes.
var BurdenMonitorTrackCommands = NewFeatureFlag(
	"burdenmonitor_track_commands",
	"v18.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/7089",
	false,
)
