package gitaly

import (
	"bytes"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestConfiguration(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitaly(t, cfg)

	t.Run("validate", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			exitCode int
			stdin    func(t *testing.T) io.Reader
			stderr   string
			stdout   string
		}{
			{
				name:     "ok",
				exitCode: 0,
				stdin: func(*testing.T) io.Reader {
					t.Helper()
					var stdin bytes.Buffer
					require.NoError(t, toml.NewEncoder(&stdin).Encode(cfg))
					return &stdin
				},
			},
			{
				name:     "bad toml format",
				exitCode: 2,
				stdin: func(*testing.T) io.Reader {
					return strings.NewReader(`graceful_restart_timeout = "bad value"`)
				},
				stdout: `{
  "errors": [
    {
      "message": "line 1 column 28: toml: time: invalid duration \"bad value\""
    }
  ]
}
`,
			},
			{
				name:     "validation failures",
				exitCode: 2,
				stdin: func(t *testing.T) io.Reader {
					cfg := cfg
					cfg.Git.Config = []config.GitConfig{{Key: "bad"}}
					cfg.Storages = []config.Storage{{Name: "", Path: cfg.Storages[0].Path}}
					data, err := toml.Marshal(cfg)
					require.NoError(t, err)
					return bytes.NewReader(data)
				},
				stdout: `{
  "errors": [
    {
      "key": [
        "git",
        "config",
        "key"
      ],
      "message": "key \"bad\" must contain at least one section"
    },
    {
      "key": [
        "storage",
        "[0]",
        "name"
      ],
      "message": "not set"
    }
  ]
}
`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				cmd := exec.Command(cfg.BinaryPath("gitaly"), "configuration", "validate")
				var stderr, stdout bytes.Buffer
				cmd.Stderr = &stderr
				cmd.Stdout = &stdout
				cmd.Stdin = tc.stdin(t)

				err := cmd.Run()
				if tc.exitCode != 0 {
					status, ok := command.ExitStatus(err)
					require.Truef(t, ok, "%T: %v", err, err)
					assert.Equal(t, tc.exitCode, status)
				}
				assert.Equal(t, tc.stderr, stderr.String())
				assert.Equal(t, tc.stdout, stdout.String())
			})
		}
	})
}
