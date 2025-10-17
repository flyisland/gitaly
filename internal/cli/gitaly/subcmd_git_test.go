package gitaly

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestGitalyGitCommand(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitaly(t, cfg)
	_, repo := gittest.CreateRepository(t, ctx, cfg,
		gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

	commitID := gittest.WriteCommit(t, cfg, repo,
		gittest.WithBranch("main"),
		gittest.WithMessage("First commit"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README.md", Content: "# Initial content"},
		),
	)

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	tests := []struct {
		name           string
		args           []string
		input          string
		pipeCommand    *exec.Cmd
		exitCode       int
		expectedOutput string
		expectedError  string
	}{
		{
			name:           "git log",
			args:           []string{"--", "log", "-1", "--pretty=format:%s"},
			expectedOutput: "First commit",
		},
		{
			name:           "git rev-list with --stdin",
			args:           []string{"--", "rev-list", "--stdin"},
			input:          "HEAD",
			expectedOutput: commitID.String() + "\n",
		},
		{
			name:           "git rev-parse --is-inside-work-tree",
			args:           []string{"--", "rev-parse", "--is-inside-work-tree"},
			expectedOutput: "false\n",
		},
		{
			name:          "invalid git command",
			args:          []string{"invalid-command"},
			exitCode:      1,
			expectedError: "git: 'invalid-command' is not a git command. See 'git --help'.\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := append([]string{"git", "--config", configPath}, tt.args...)
			cmd := exec.Command(cfg.BinaryPath("gitaly"), args...)

			var stdout, stderr bytes.Buffer
			cmd.Dir = repo
			cmd.Stderr = &stderr
			cmd.Stdout = &stdout

			if tt.input != "" {
				cmd.Stdin = strings.NewReader(tt.input)
			}

			// Override these environment variables to avoid interference with the test.
			cmd.Env = []string{
				"GIT_CONFIG_GLOBAL=/dev/null",
				"GIT_CONFIG_SYSTEM=/dev/null",
				"XDG_CONFIG_HOME=/dev/null",
			}

			err := cmd.Run()

			if tt.expectedError != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err, stderr.String())
			}

			require.Equal(t, tt.expectedError, stderr.String())
			require.Equal(t, tt.exitCode, cmd.ProcessState.ExitCode())
			require.Equal(t, tt.expectedOutput, stdout.String())
		})
	}
}
