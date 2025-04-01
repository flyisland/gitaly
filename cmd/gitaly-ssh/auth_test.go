package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestConnectivity(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

	cwd, err := os.Getwd()
	require.NoError(t, err)

	tempDir := testhelper.TempDir(t)

	relativeSocketPath, err := filepath.Rel(cwd, filepath.Join(tempDir, "gitaly.socket"))
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(relativeSocketPath))
	require.NoError(t, os.Symlink(cfg.SocketPath, relativeSocketPath))

	runGitaly := func(tb testing.TB, cfg config.Cfg) string {
		tb.Helper()
		return testserver.RunGitalyServer(tb, cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	}

	testCases := []struct {
		name  string
		setup func(t *testing.T, cfg config.Cfg) (addr string, certPath string, envVars []string)
	}{
		{
			name: "tcp",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				cfg.ListenAddr = "localhost:0"
				return runGitaly(t, cfg), "", nil
			},
		},
		{
			name: "tcp with no_proxy",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				cfg.ListenAddr = "localhost:0"

				addr := runGitaly(t, cfg)

				return addr, "", []string{
					"http_proxy=http://invalid:1234",
					"https_proxy=https://invalid:1234",
					fmt.Sprintf("no_proxy=%s", strings.TrimPrefix(addr, "tcp://")),
				}
			},
		},
		{
			name: "unix absolute",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				return runGitaly(t, cfg), "", nil
			},
		},
		{
			name: "unix abs with proxy",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				return runGitaly(t, cfg), "", []string{
					"http_proxy=http://invalid:1234",
					"https_proxy=https://invalid:1234",
				}
			},
		},
		{
			name: "unix relative",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg), "", nil
			},
		},
		{
			name: "unix relative with proxy",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg), "", []string{
					"http_proxy=http://invalid:1234",
					"https_proxy=https://invalid:1234",
				}
			},
		},
		{
			name: "tls",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				certificate := testhelper.GenerateCertificate(t)
				t.Setenv(x509.SSLCertFile, certificate.CertPath)

				cfg.TLSListenAddr = "localhost:0"
				cfg.TLS = config.TLS{
					CertPath: certificate.CertPath,
					KeyPath:  certificate.KeyPath,
				}
				return runGitaly(t, cfg), certificate.CertPath, nil
			},
		},
		{
			name: "dns",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				// Configure a Gitaly server that listens over TCP.
				cfg.ListenAddr = "localhost:0"
				gitalyAddr := runGitaly(t, cfg)
				gitalyPort := strings.Split(gitalyAddr, ":")[2]

				// Start a DNS server that responds to anything with the loopback address.
				dnsServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(host string) []string {
					return []string{"127.0.0.1"}
				}).Start()

				return fmt.Sprintf("dns://%s/%s", dnsServer.Addr(), "localhost:"+gitalyPort), "", nil
			},
		},
		{
			name: "dns (no authority)",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				// Configure a Gitaly server that listens over TCP.
				cfg.ListenAddr = "localhost:0"
				gitalyAddr := runGitaly(t, cfg)
				gitalyPort := strings.Split(gitalyAddr, ":")[2]

				return "dns:///localhost:" + gitalyPort, "", nil
			},
		},
		{
			name: "tcp with dns address (no authority)",
			setup: func(t *testing.T, cfg config.Cfg) (string, string, []string) {
				// Configure a Gitaly server that listens over TCP.
				cfg.ListenAddr = "localhost:0"
				gitalyAddr := runGitaly(t, cfg)
				gitalyPort := strings.Split(gitalyAddr, ":")[2]

				return fmt.Sprintf("tcp://localhost:%s", gitalyPort), "", nil
			},
		},
	}

	payload, err := protojson.Marshal(&gitalypb.SSHUploadPackWithSidechannelRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			addr, certFile, envVars := testcase.setup(t, cfg)

			env := []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=%s", addr),
				fmt.Sprintf("GITALY_WD=%s", cwd),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", cfg.BinaryPath("gitaly-ssh")),
				fmt.Sprintf("SSL_CERT_FILE=%s", certFile),
			}

			if envVars != nil {
				env = append(env, envVars...)
			}

			output := gittest.ExecOpts(t, cfg, gittest.ExecConfig{
				Env: env,
			}, "ls-remote", "git@localhost:test/test.git", git.DefaultRef.String())
			require.True(t, strings.HasSuffix(strings.TrimSpace(string(output)), git.DefaultRef.String()))
		})
	}
}
