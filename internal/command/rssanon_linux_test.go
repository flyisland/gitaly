//go:build linux

package command

import (
	"context"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestGetRssAnon(t *testing.T) {
	t.Parallel()

	rssAnon, err := getRssAnon(os.Getpid())
	require.NoError(t, err)
	require.Greater(t, rssAnon, int64(0))
	require.Equal(t, int64(0), rssAnon%1024, "RssAnon should be in bytes (multiple of 1024)")
}

func TestGetRssAnon_invalidPid(t *testing.T) {
	t.Parallel()

	_, err := getRssAnon(-1)
	require.Error(t, err)
}

func TestCommand_logMaxRssAnon(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TrackMaxRssAnon).Run(t, testCommandLogMaxRssAnon)
}

func testCommandLogMaxRssAnon(t *testing.T, ctx context.Context) {
	t.Helper()

	logger := testhelper.NewLogger(t)
	logger.LogrusEntry().Logger.SetLevel(logrus.DebugLevel) //nolint:staticcheck
	hook := testhelper.AddLoggerHook(logger)

	cmd, err := New(ctx, logger, []string{"sleep", "0.3"})
	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	logEntry := hook.LastEntry()
	require.NotNil(t, logEntry)

	if featureflag.TrackMaxRssAnon.IsEnabled(ctx) {
		if maxRssAnon, ok := logEntry.Data["command.maxrssanon_bytes"]; ok {
			assert.Greater(t, maxRssAnon, int64(0))
		}
	} else {
		assert.NotContains(t, logEntry.Data, "command.maxrssanon_bytes")
	}
}
