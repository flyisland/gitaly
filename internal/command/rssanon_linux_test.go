//go:build linux

package command

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	ctx := testhelper.Context(t)
	logger := testhelper.NewLogger(t)
	logger.LogrusEntry().Logger.SetLevel(logrus.DebugLevel) //nolint:staticcheck
	hook := testhelper.AddLoggerHook(logger)

	cmd, err := New(ctx, logger, []string{"sleep", "0.3"})
	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	logEntry := hook.LastEntry()
	require.NotNil(t, logEntry)

	if maxRssAnon, ok := logEntry.Data["command.maxrssanon_bytes"]; ok {
		assert.Greater(t, maxRssAnon, int64(0))
	}
}
