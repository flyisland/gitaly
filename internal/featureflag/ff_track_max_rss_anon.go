package featureflag

// TrackMaxRssAnon enables background polling to track peak anonymous memory usage of spawned commands.
var TrackMaxRssAnon = NewFeatureFlag(
	"track_max_rss_anon",
	"v18.0.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/7084",
	false,
)
