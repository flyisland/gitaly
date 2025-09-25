package main

import (
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func runTestServer(t *testing.T, options gitlab.TestServerOptions) (config.Gitlab, func()) {
	tempDir := testhelper.TempDir(t)

	gitlab.WriteShellSecretFile(t, tempDir, secretToken)
	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")

	serverURL, serverCleanup := gitlab.NewTestServer(t, options)

	var certPath string
	if options.ServerCertificate != nil {
		certPath = options.ServerCertificate.CertPath
	}

	c := config.Gitlab{URL: serverURL, SecretFile: secretFilePath, HTTPSettings: config.HTTPSettings{
		CAFile: certPath,
	}}

	return c, func() {
		serverCleanup()
	}
}
