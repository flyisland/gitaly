package dnsresolver

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/resolver"
)

func TestDnsResolver(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		setup          func(*testhelper.FakeDNSServer, *Builder) *fakeClientConn
		address        string
		builder        *Builder
		expectedStates []resolver.State
		expectedErrors []error
	}{
		{
			name: "resolver updates a single IPv4 each resolution",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"1.2.3.4"},
					{"1.2.3.5"},
					{"1.2.3.6"},
				}}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})

				return newFakeClientConn(3, 0)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.4:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.5:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:50051"}}},
			},
		},
		{
			name: "resolver updates multiple IPv4 each resolution",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"1.2.3.4", "1.2.3.5"},
					{"1.2.3.6"},
					{"1.2.3.7", "1.2.3.8"},
				}}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})

				return newFakeClientConn(3, 0)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.4:50051"}, {Addr: "1.2.3.5:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.7:50051"}, {Addr: "1.2.3.8:50051"}}},
			},
		},
		{
			name: "resolver updates multiple IPv6 each resolution",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"::1", "::2"},
					{"::3", "::4"},
					{"::5", "::6"},
				}}
				server.WithHandler(dns.TypeAAAA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})

				return newFakeClientConn(3, 0)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "[::1]:50051"}, {Addr: "[::2]:50051"}}},
				{Addresses: []resolver.Address{{Addr: "[::3]:50051"}, {Addr: "[::4]:50051"}}},
				{Addresses: []resolver.Address{{Addr: "[::5]:50051"}, {Addr: "[::6]:50051"}}},
			},
		},
		{
			name: "resolver resolves address without port",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"1.2.3.4"},
					{"1.2.3.5"},
					{"1.2.3.6"},
				}}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})

				return newFakeClientConn(3, 0)
			},
			address: "grpc.test",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.4:1234"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.5:1234"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:1234"}}},
			},
		},
		{
			name: "resolver retries with exponential Backoff when client connection fails to update",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				conn := newFakeClientConn(2, 0)
				connErr := 2
				conn.customUpdateState = func(state resolver.State) error {
					if connErr > 0 {
						connErr--
						return fmt.Errorf("something goes wrong")
					}
					return conn.doUpdateState(state)
				}

				ips := ipList{ips: [][]string{
					{"1.2.3.4"},
					{"1.2.3.5"},
					{"1.2.3.6"},
					{"1.2.3.7"},
				}}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})

				return conn
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.7:50051"}}},
			},
		},
		{
			name: "DNS nameserver returns empty addresses",
			setup: func(server *testhelper.FakeDNSServer, _ *Builder) *fakeClientConn {
				server.WithHandler(dns.TypeA, func(_ string) []string {
					return nil
				})
				return newFakeClientConn(3, 0)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{}},
				{Addresses: []resolver.Address{}},
				{Addresses: []resolver.Address{}},
			},
		},
		{
			name: "DNS nameserver raises timeout error",
			setup: func(server *testhelper.FakeDNSServer, builder *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"1.2.3.4"},
					{},
					{"1.2.3.5"},
					{"1.2.3.6"},
				}}

				builder.opts.authorityFinder = func(authority string) (dnsLookuper, error) {
					f := newFakeLookup(t, authority)
					f.stubLookup = func(ctx context.Context, host string) ([]string, error) {
						nextIPs := ips.peek()
						if nextIPs != nil && len(nextIPs) == 0 {
							ips.next()
							return nil, &net.DNSError{
								Err:       "timeout",
								Name:      host,
								IsTimeout: true,
							}
						}
						return f.realLookup.LookupHost(ctx, host)
					}
					return f, nil
				}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})
				return newFakeClientConn(3, 1)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.4:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.5:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:50051"}}},
			},
			expectedErrors: []error{
				structerr.New("dns: record resolve error: %w", &net.DNSError{
					Err:       "timeout",
					Name:      "grpc.test",
					IsTimeout: true,
				}),
			},
		},
		{
			name: "DNS nameserver raises a temporary error",
			setup: func(server *testhelper.FakeDNSServer, builder *Builder) *fakeClientConn {
				ips := ipList{ips: [][]string{
					{"1.2.3.4"},
					{},
					{},
					{},
					{"1.2.3.5"},
					{"1.2.3.6"},
					{},
					{"1.2.3.6"},
				}}

				builder.opts.authorityFinder = func(authority string) (dnsLookuper, error) {
					f := newFakeLookup(t, authority)
					f.stubLookup = func(ctx context.Context, host string) ([]string, error) {
						nextIPs := ips.peek()
						if nextIPs != nil && len(nextIPs) == 0 {
							ips.next()
							return nil, &net.DNSError{
								Err:         "temporary",
								Name:        host,
								IsTemporary: true,
							}
						}
						return f.realLookup.LookupHost(ctx, host)
					}
					return f, nil
				}
				server.WithHandler(dns.TypeA, func(host string) []string {
					if host != "grpc.test." {
						return nil
					}
					return ips.next()
				})
				return newFakeClientConn(3, 4)
			},
			address: "grpc.test:50051",
			expectedStates: []resolver.State{
				{Addresses: []resolver.Address{{Addr: "1.2.3.4:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.5:50051"}}},
				{Addresses: []resolver.Address{{Addr: "1.2.3.6:50051"}}}, // Cached
			},
			expectedErrors: []error{
				structerr.New("dns: record resolve error: %w", &net.DNSError{
					Err:         "temporary",
					Name:        "grpc.test",
					IsTemporary: true,
				}),
				structerr.New("dns: record resolve error: %w", &net.DNSError{
					Err:         "temporary",
					Name:        "grpc.test",
					IsTemporary: true,
				}),
				structerr.New("dns: record resolve error: %w", &net.DNSError{
					Err:         "temporary",
					Name:        "grpc.test",
					IsTemporary: true,
				}),
				structerr.New("dns: record resolve error: %w", &net.DNSError{
					Err:         "temporary",
					Name:        "grpc.test",
					IsTemporary: true,
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder(&BuilderConfig{
				RefreshRate:     0, // No delay
				LookupTimeout:   15 * time.Second,
				Logger:          testhelper.SharedLogger(t),
				DefaultGrpcPort: "1234",
				Backoff:         &fakeBackoff{},
			})

			fakeServer := testhelper.NewFakeDNSServer(t)
			conn := tc.setup(fakeServer, builder)
			fakeServer.Start()

			r, err := builder.Build(buildResolverTarget(fakeServer, tc.address), conn, resolver.BuildOptions{})
			require.NoError(t, err)

			conn.Wait()
			r.Close()

			require.Equal(t, tc.expectedStates, conn.states)
			require.Equal(t, tc.expectedErrors, conn.errors)
		})
	}
}

// This test spawns a real gRPC server and a fake DNS nameserver that maps grpc.test to spawned
// gRPC server. Too bad, it's very likely the testing machine has only one localhost loopback.
// We could not bind to another IP address without modifying ipconfig/ifconfig. Service discovery
// with SRV record overcomes this limitation. However, we don't support that at the moment.
func TestDnsResolver_grpcCallWithOurDNSResolver(t *testing.T) {
	t.Parallel()

	listener := spawnTestGRPCServer(t)
	serverHost, serverPort, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	fakeServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(host string) []string {
		if host == "grpc.test." {
			return []string{serverHost}
		}
		return nil
	}).Start()

	// This scheme uses our DNS resolver
	target := buildResolverTarget(fakeServer, fmt.Sprintf("grpc.test:%s", serverPort))
	// Ideally we'd use our own client.New() constructor, but that results in an import cycle between the
	// client and dnsresolver packages.
	conn, err := grpc.NewClient(
		target.URL.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(NewBuilder(&BuilderConfig{
			RefreshRate:     0, // No delay
			LookupTimeout:   500 * time.Millisecond,
			Logger:          testhelper.SharedLogger(t),
			DefaultGrpcPort: "1234",
			Backoff:         backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano()))),
		})),
	)
	require.NoError(t, err)

	client := grpc_testing.NewTestServiceClient(conn)
	for i := 0; i < 10; i++ {
		_, err = client.UnaryCall(testhelper.Context(t), &grpc_testing.SimpleRequest{})
		require.NoError(t, err)
	}

	testhelper.MustClose(t, conn)
	// In the grpc-go versions before v1.56, the resolver is closed when the bound connection is closed. After
	// v1.56, the resolver runs more independently. The connection doesn't wait for the resolver to finish anymore.
	// After the test suite finishes, there might be some in-flight DNS queries managed by the DNS resolver. When
	// the connection is closed, those queries don't make sense anymore. They can stay until they finish.
	// Unfortunately, the goroutine detector in the test suite complains about leaked goroutines. The resolver of
	// a connection is not accessible from the outside. Hence, this test sets a low lookup timeout and sleep for
	// a while until the pending DNS queries go away.
	time.Sleep(1 * time.Second)
}

func spawnTestGRPCServer(t *testing.T) net.Listener {
	ctx := testhelper.Context(t)
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(grpccorrelation.UnaryServerCorrelationInterceptor()))
	svc := &testSvc{}
	grpc_testing.RegisterTestServiceServer(grpcServer, svc)
	go testhelper.MustServe(t, grpcServer, listener)
	t.Cleanup(func() { grpcServer.Stop() })

	return listener
}

type testSvc struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (ts *testSvc) UnaryCall(_ context.Context, _ *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{}, nil
}
