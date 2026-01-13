package dnsresolver

import (
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/resolver"
)

func TestBuildDNSBuilder_withBuiltInResolver(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc   string
		target string
	}{
		{desc: "with triple slashes", target: "dns:///localhost:50051"},
		{desc: "without triple slashes", target: "dns:localhost:50051"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Actually resolve with built-in resolver. Localhost may return different results
			// between machines.
			expectedIPs, err := net.DefaultResolver.LookupHost(testhelper.Context(t), "localhost")
			require.NoError(t, err)

			builder := newTestDNSBuilder(t)
			conn := newFakeClientConn(1, 0)

			targetURL, err := url.Parse(tc.target)
			require.NoError(t, err)

			r, err := builder.Build(resolver.Target{URL: *targetURL}, conn, resolver.BuildOptions{})
			require.NoError(t, err)
			defer r.Close()

			conn.Wait()

			// Compare the recorded state with real DNS resolution
			require.Equal(t, 1, len(conn.states))
			state := conn.states[0]

			var actualIPs []string
			for _, addr := range state.Addresses {
				host, port, err := net.SplitHostPort(addr.Addr)
				require.NoError(t, err)
				require.Equal(t, port, "50051")
				actualIPs = append(actualIPs, host)
			}
			require.ElementsMatch(t, expectedIPs, actualIPs)
		})
	}
}

func TestBuildDNSBuilder_customAuthorityResolver(t *testing.T) {
	t.Parallel()

	fakeServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(host string) []string {
		if host == "grpc.test." {
			return []string{"1.2.3.4"}
		}
		return nil
	}).Start()

	builder := NewBuilder(&BuilderConfig{
		RefreshRate:   0,
		LookupTimeout: 15 * time.Second,
		Logger:        testhelper.SharedLogger(t),
		Backoff:       &fakeBackoff{},
	})

	conn := newFakeClientConn(1, 0)
	r, err := builder.Build(buildResolverTarget(fakeServer, "grpc.test:50051"), conn, resolver.BuildOptions{})
	require.NoError(t, err)
	defer r.Close()

	conn.Wait()
	require.Equal(t, []resolver.State{{Addresses: []resolver.Address{{
		Addr: "1.2.3.4:50051",
	}}}}, conn.states)
}

func TestBuildDNSBuilder_staticIPAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		addr string
	}{
		{
			desc: "IPv4",
			addr: "4.3.2.1:50051",
		},
		{
			desc: "Full IPv6",
			addr: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:50051",
		},
		{
			desc: "Shortened IPv6",
			addr: "[::3]:50051",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			fakeServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(_ string) []string {
				require.FailNow(t, "resolving IP address should not result in a real DNS resolution")
				return nil
			}).Start()

			builder := NewBuilder(&BuilderConfig{
				RefreshRate:   0,
				LookupTimeout: 15 * time.Second,
				Logger:        testhelper.SharedLogger(t),
				Backoff:       &fakeBackoff{},
			})

			conn := newFakeClientConn(1, 0)
			r, err := builder.Build(buildResolverTarget(fakeServer, tc.addr), conn, resolver.BuildOptions{})
			require.NoError(t, err)
			defer r.Close()

			conn.Wait()
			require.Equal(t, []resolver.State{{Addresses: []resolver.Address{{
				Addr: tc.addr,
			}}}}, conn.states)

			require.IsType(t, &noopResolver{}, r, "building a resolver for IP address should return a no-op resolver")
		})
	}
}

func TestSchemeDNSBuilder(t *testing.T) {
	t.Parallel()

	d := &Builder{}
	require.Equal(t, d.Scheme(), "dns")
}

func TestSchemeTLSBuilder(t *testing.T) {
	t.Parallel()

	dnsBuilder := &Builder{}
	tlsBuilder := NewTLSPlusDNSBuilder(dnsBuilder)
	require.Equal(t, "dns+tls", tlsBuilder.Scheme())

	// Verify that the TLS builder wraps the DNS builder
	require.Equal(t, dnsBuilder, tlsBuilder.Builder)
}
