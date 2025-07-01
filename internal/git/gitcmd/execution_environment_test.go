package gitcmd_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestDistributedGitEnvironmentConstructor(t *testing.T) {
	constructor := gitcmd.DistributedGitEnvironmentConstructor{}

	testhelper.Unsetenv(t, "GITALY_TESTING_GIT_BINARY")

	t.Run("empty configuration fails", func(t *testing.T) {
		_, err := constructor.Construct(config.Cfg{})
		require.Equal(t, gitcmd.ErrNotConfigured, err)
	})

	t.Run("empty configuration with environment override", func(t *testing.T) {
		t.Setenv("GITALY_TESTING_GIT_BINARY", "/foo/bar")

		execEnv, err := constructor.Construct(config.Cfg{})
		require.NoError(t, err)
		defer func() { require.NoError(t, execEnv.Cleanup()) }()

		require.Equal(t, "/foo/bar", execEnv.BinaryPath)
		require.Equal(t, []string{
			"NO_SET_GIT_TEMPLATE_DIR=YesPlease",
		}, execEnv.EnvironmentVariables)
	})
}

func TestBundledGitEnvironmentConstructor(t *testing.T) {
	testhelper.Unsetenv(t, "GITALY_TESTING_GIT_BINARY")

	constructor := gitcmd.BundledGitConstructors[0]

	t.Run("complete binary directory succeeds", func(t *testing.T) {
		cfg := testcfg.Build(t)
		execEnv, err := constructor.Construct(cfg)
		require.NoError(t, err)
		defer func() { require.NoError(t, execEnv.Cleanup()) }()

		// We create a temporary directory where the symlinks are created, and we cannot
		// predict its exact path.
		require.Equal(t, "git", filepath.Base(execEnv.BinaryPath))

		execPrefix := filepath.Dir(execEnv.BinaryPath)
		require.Equal(t, []string{
			"GIT_EXEC_PATH=" + execPrefix,
		}, execEnv.EnvironmentVariables)

		for _, binary := range []string{"git", "git-remote-http", "git-http-backend"} {
			target, err := filepath.EvalSymlinks(filepath.Join(execPrefix, binary))
			require.NoError(t, err)
			require.Equal(t, filepath.Join(testhelper.SourceRoot(t), "_build", "bin", fmt.Sprintf("gitaly-%s%s", binary, constructor.Suffix)), target)
		}
	})
}

func TestFallbackGitEnvironmentConstructor(t *testing.T) {
	constructor := gitcmd.FallbackGitEnvironmentConstructor{}

	t.Run("failing lookup of executable causes failure", func(t *testing.T) {
		t.Setenv("PATH", "/does/not/exist")

		_, err := constructor.Construct(config.Cfg{})
		require.Equal(t, fmt.Errorf("%w: no git executable found in PATH", gitcmd.ErrNotConfigured), err)
	})

	t.Run("successfully resolved executable", func(t *testing.T) {
		tempDir := testhelper.TempDir(t)
		gitPath := filepath.Join(tempDir, "git")
		require.NoError(t, os.WriteFile(gitPath, nil, mode.Executable))

		t.Setenv("PATH", "/does/not/exist:"+tempDir)

		execEnv, err := constructor.Construct(config.Cfg{})
		require.NoError(t, err)
		defer func() { require.NoError(t, execEnv.Cleanup()) }()

		require.Equal(t, gitPath, execEnv.BinaryPath)
		require.Equal(t, []string(nil), execEnv.EnvironmentVariables)
	})
}
