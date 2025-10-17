package gittest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

// IsGitVersionLessThan checks if the Git version in use is less than then specified version.
func IsGitVersionLessThan(tb testing.TB, ctx context.Context, cfg config.Cfg, version git.Version) bool {
	cmdFactory, clean, err := gitcmd.NewExecCommandFactory(cfg, testhelper.SharedLogger(tb))
	require.NoError(tb, err)
	defer clean()

	actual, err := cmdFactory.GitVersion(ctx)
	require.NoError(tb, err)

	return actual.LessThan(version)
}

// SkipIfGitVersionLessThan skips the test if the Git version in use is less than
// expected. The reason is printed out when skipping the test
func SkipIfGitVersionLessThan(tb testing.TB, ctx context.Context, cfg config.Cfg, expected git.Version, reason string) {
	cmdFactory, clean, err := gitcmd.NewExecCommandFactory(cfg, testhelper.SharedLogger(tb))
	require.NoError(tb, err)
	defer clean()

	actual, err := cmdFactory.GitVersion(ctx)
	require.NoError(tb, err)

	if actual.LessThan(expected) {
		tb.Skipf("Unsupported Git version %q, expected minimum %q: %q", actual, expected, reason)
	}
}

// SkipIfGitVersion skips the test if the Git version is the same.
func SkipIfGitVersion(tb testing.TB, ctx context.Context, cfg config.Cfg, expected git.Version, reason string) {
	cmdFactory, clean, err := gitcmd.NewExecCommandFactory(cfg, testhelper.SharedLogger(tb))
	require.NoError(tb, err)
	defer clean()

	actual, err := cmdFactory.GitVersion(ctx)
	require.NoError(tb, err)

	if actual.Equal(expected) {
		tb.Skipf("Unsupported Git version %q, expected %q: %q", actual, expected, reason)
	}
}
