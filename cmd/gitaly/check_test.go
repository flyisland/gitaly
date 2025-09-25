package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func createTestServer(t *testing.T) (user, password, url string, cleanup func()) {
	user, password = "user123", "password321"

	c := gitlab.TestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 "",
		GLRepository:                "",
		Changes:                     "",
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
	}

	serverURL, cleanup := gitlab.NewTestServer(t, c)

	return user, password, serverURL, cleanup
}

func TestCheckOK(t *testing.T) {
	user, password, serverURL, cleanup := createTestServer(t)
	defer cleanup()

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Gitlab: config.Gitlab{
			URL: serverURL,
			HTTPSettings: config.HTTPSettings{
				User:     user,
				Password: password,
			},
			SecretFile: gitlab.WriteShellSecretFile(t, testhelper.TempDir(t), "the secret"),
		},
	}), testcfg.WithDisableBundledGitSymlinking())
	testcfg.BuildGitaly(t, cfg)

	// BuildGitaly() above will manipulate cfg.RuntimeDir to point to the Gitaly test server's runtime dir, which
	// will contain a `sock.d` directory. When this exists, PruneOldGitalyProcessDirectories() will complain as
	// it's not recognised as a directory of the form `gitaly-<pid>`, and it emits a warning on stderr which fails
	// one of the test assertions below. This is okay however, and only happens during tests.
	//
	// We reset `cfg.RuntimeDir` to an empty string which allows the subcommand to configure a fresh runtime
	// directory independent of the one configured above.
	cfg.RuntimeDir = ""

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	cmd := exec.Command(cfg.BinaryPath("gitaly"), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	err := cmd.Run()
	require.NoError(t, err)
	require.Empty(t, stderr.String())

	output := stdout.String()
	require.Contains(t, output, "Checking GitLab API access: OK")
	require.Contains(t, output, "Redis reachable for GitLab: true")
}

func TestCheckBadCreds(t *testing.T) {
	_, password, serverURL, cleanup := createTestServer(t)
	defer cleanup()

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Gitlab: config.Gitlab{
			URL: serverURL,
			HTTPSettings: config.HTTPSettings{
				User:     "wrong",
				Password: password,
			},
			SecretFile: gitlab.WriteShellSecretFile(t, testhelper.TempDir(t), "the secret"),
		},
	}), testcfg.WithDisableBundledGitSymlinking())
	testcfg.BuildGitaly(t, cfg)

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	cmd := exec.Command(cfg.BinaryPath("gitaly"), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.Error(t, cmd.Run())
	require.Contains(t, stdout.String(), `Checking GitLab API access: FAILED`)
	require.Contains(t, stderr.String(), "HTTP GET to GitLab endpoint /check failed: authorization failed")
	require.Regexp(t, `.* level=error msg="Internal API error" .* error="authorization failed" method=GET pid=[0-9]+ status=401 url="http://127.0.0.1:[0-9]+/api/v4/internal/check"`, stderr.String())
}

func TestCheckAuxiliaryBinaries(t *testing.T) {
	user, password, serverURL, cleanup := createTestServer(t)
	defer cleanup()

	runtimeDir, err := os.MkdirTemp(os.TempDir(), "")
	require.NoError(t, err)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Gitlab: config.Gitlab{
			URL: serverURL,
			HTTPSettings: config.HTTPSettings{
				User:     user,
				Password: password,
			},
			SecretFile: gitlab.WriteShellSecretFile(t, testhelper.TempDir(t), "the secret"),
		},
		RuntimeDir: runtimeDir,
	}), testcfg.WithDisableBundledGitSymlinking())
	testcfg.BuildGitaly(t, cfg)

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	cmd := exec.Command(cfg.BinaryPath("gitaly"), "check", configPath)
	require.NoError(t, cmd.Run())

	// When Gitaly starts, it needs to unpack embedded binaries to the configured runtime directory.
	// It is expected that these binaries are cleanup when the program completes.
	f, err := os.Open(runtimeDir)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Readdirnames(1)
	require.ErrorIs(t, err, io.EOF)
}
