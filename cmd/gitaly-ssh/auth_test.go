package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	serverservice "gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/server"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestConnectivity(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// It's important that the certificate referenced by SSL_CERT_FILE remains valid
	// for the lifetime of the Go process that consumes it (i.e. the test binary). This
	// is because the standard library (https://github.com/golang/go/blob/release-branch.go1.23/src/crypto/x509/cert_pool.go?name=release#L117-L123)
	// only loads the root pool once (see the systemRootsPool() function that gets
	// called), so any changes to SSL_CERT_FILE won't be picked up by the same process.
	//
	// Since we have two TLS-enabled tests in the table test below, the second test will
	// always hang during the TLS handshake if we replace the SSL_CERT_FILE.
	certificate := testhelper.GenerateCertificate(t)
	t.Setenv(x509.SSLCertFile, certificate.CertPath)

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

	// The ALPN tests are a subtest of TestConnectivity because we can only generate TLS certificates _once_ per
	// package under Linux. This isn't elegant, but correctly tests the functionality.
	t.Run("ALPN tests", func(t *testing.T) {
		ctx := testhelper.Context(t)
		cfg := testcfg.Build(t)

		var wg sync.WaitGroup
		defer wg.Wait()

		setupDummyServer := func(t *testing.T) (string, func()) {
			logger := testhelper.SharedLogger(t)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			serverCert, err := tls.LoadX509KeyPair(certificate.CertPath, certificate.KeyPath)
			require.NoError(t, err)

			tlsListener := tls.NewListener(listener, &tls.Config{
				Certificates: []tls.Certificate{serverCert},
				NextProtos:   []string{},
			})

			serverAddr := tlsListener.Addr().String()

			srv := grpc.NewServer()

			// Register the server service
			gitalypb.RegisterServerServiceServer(srv, serverservice.NewServer(&service.Dependencies{
				Logger:        logger,
				GitCmdFactory: gittest.NewCommandFactory(t, cfg),
				Cfg:           cfg,
			}))

			wg.Add(1)
			go func() {
				defer wg.Done()
				testhelper.MustServe(t, srv, tlsListener)
			}()

			return serverAddr, srv.Stop
		}

		setupGitalyServer := func(t *testing.T) (string, func()) {
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			cfg.TLS = config.TLS{
				CertPath: certificate.CertPath,
				KeyPath:  certificate.KeyPath,
			}

			serverAddr := listener.Addr().String()
			sf := server.NewGitalyServerFactory(
				cfg,
				testhelper.SharedLogger(t),
				backchannel.NewRegistry(),
				cache.New(cfg, config.NewLocator(cfg), testhelper.SharedLogger(t)),
				nil,
				nil,
				server.TransactionMiddleware{},
			)
			t.Cleanup(sf.Stop)

			externalServer, err := sf.CreateExternal(true)
			require.NoError(t, err)

			gitalypb.RegisterServerServiceServer(externalServer, serverservice.NewServer(&service.Dependencies{
				Logger:        testhelper.SharedLogger(t),
				GitCmdFactory: gittest.NewCommandFactory(t, cfg),
				Cfg:           cfg,
			}))

			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, externalServer.Serve(listener), "failure to serve external gRPC")
			}()

			return serverAddr, sf.Stop
		}

		callWithDummyClient := func(t *testing.T, serverAddr string) (func(), error) {
			certPool, err := x509.SystemCertPool()
			require.NoError(t, err)

			cert := testhelper.MustReadFile(t, certificate.CertPath)
			ok := certPool.AppendCertsFromPEM(cert)
			require.True(t, ok)

			tlsConfig := &tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
				NextProtos: []string{},
			}

			conn, err := grpc.NewClient(
				serverAddr,
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			)
			require.NoError(t, err)

			c := gitalypb.NewServerServiceClient(conn)
			_, err = c.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

			return func() { require.NoError(t, conn.Close()) }, err
		}

		callWithGitalyDialer := func(t *testing.T, serverAddr string) (func(), error) {
			conn, err := client.Dial(fmt.Sprintf("tls://%s", serverAddr), []grpc.DialOption{})
			require.NoError(t, err)

			// Verify connection works by making a simple RPC call
			c := gitalypb.NewServerServiceClient(conn)

			_, err = c.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

			return func() { require.NoError(t, conn.Close()) }, err
		}

		for _, tc := range []struct {
			desc        string
			setupServer func(*testing.T) (string, func())
			clientCall  func(*testing.T, string) (func(), error)
			errorString string
		}{
			{
				desc:        "Client (No ALPN) -> Server (No ALPN dummy)",
				setupServer: setupDummyServer,
				clientCall:  callWithDummyClient,
				errorString: "ALPN enforcement",
			},
			{
				desc:        "Client (client.Dial) -> Server (No ALPN dummy)",
				setupServer: setupDummyServer,
				clientCall:  callWithGitalyDialer,
			},
			{
				desc:        "Client (No ALPN) -> Server (Gitaly)",
				setupServer: setupGitalyServer,
				clientCall:  callWithDummyClient,
			},
			{
				desc:        "Client (client.Dial) -> Server (Gitaly)",
				setupServer: setupGitalyServer,
				clientCall:  callWithGitalyDialer,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				serverAddr, stopServer := tc.setupServer(t)
				defer stopServer()

				closeClient, err := tc.clientCall(t, serverAddr)
				defer closeClient()

				if len(tc.errorString) == 0 {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.ErrorContains(t, err, tc.errorString)
				}
			})
		}
	})
}
