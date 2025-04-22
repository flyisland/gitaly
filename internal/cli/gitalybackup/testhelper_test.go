package gitalybackup

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func runGitalyBackup(tb testing.TB, ctx context.Context, cfg config.Cfg, stdin io.Reader, args ...string) (string, string, int) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, testcfg.BuildGitalyBackup(tb, cfg), args...)
	cmd.Stdin = stdin
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		require.ErrorAs(tb, err, &exitErr)
		exitCode = exitErr.ExitCode()
	}

	return stdout.String(), stderr.String(), exitCode
}
