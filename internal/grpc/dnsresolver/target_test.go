package dnsresolver

import (
	"fmt"
	"net"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestFindDNSLookup_default(t *testing.T) {
	t.Parallel()

	resolver, err := findDNSLookup("")
	require.NoError(t, err)
	require.Equal(t, net.DefaultResolver, resolver)
}

func TestFindDNSLookup_invalidAuthority(t *testing.T) {
	t.Parallel()

	resolver, err := findDNSLookup("this:is:not:good")
	require.ErrorIs(t, structerr.New("dns resolver: %w", &net.AddrError{
		Err:  "too many colons in address",
		Addr: "this:is:not:good:53",
	}), err)
	require.Nil(t, resolver)
}

func TestFindDNSLookup_validAuthority(t *testing.T) {
	t.Parallel()

	fakeServer := testhelper.NewFakeDNSServer(t).WithHandler(dns.TypeA, func(host string) []string {
		if host == "grpc.test." {
			return []string{"1.2.3.4"}
		}
		return nil
	}).Start()

	resolver, err := findDNSLookup(fakeServer.Addr())
	require.NoError(t, err)

	addrs, err := resolver.LookupHost(testhelper.Context(t), "grpc.test")
	require.NoError(t, err)

	require.Equal(t, []string{"1.2.3.4"}, addrs)
}

func TestValidateTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target      string
		expectedErr error
	}{
		{target: "dns:google.com"},
		{target: "dns:///google.com"},
		{target: "dns://1.1.1.1/google.com"},
		{target: "dns:google.com:50051"},
		{target: "dns:///google.com:50051"},
		{target: "dns://1.1.1.1:53/google.com:50051"},
		{target: "dns:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]"},
		{target: "dns:///[2001:0db8:85a3:0000:0000:8a2e:0370:7334]"},
		{target: "dns://1.1.1.1:53/[2001:0db8:85a3:0000:0000:8a2e:0370:7334]"},
		{target: "dns:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:50051"},
		{target: "dns:///[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:50051"},
		{target: "dns://1.1.1.1:53/[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:50051"},
		{target: "dns:[fe80::1ff:fe23:4567:890a]:50051"},
		{target: "dns:///[fe80::1ff:fe23:4567:890a]:50051"},
		{target: "dns://1.1.1.1:53/[fe80::1ff:fe23:4567:890a]:50051"},
		{
			target:      "dns:[fe80::1ff:fe23:4567:890a]:",
			expectedErr: structerr.New("dns resolver: missing port after port-separator colon"),
		},
		{
			target:      "tcp://[fe80::1ff:fe23:4567:890a]",
			expectedErr: structerr.New("unexpected scheme: tcp"),
		},
		{
			target:      "",
			expectedErr: structerr.New("empty address"),
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("target: %s", tc.target), func(t *testing.T) {
			require.Equal(t, tc.expectedErr, ValidateURL(tc.target))
		})
	}
}

func TestParseTarget(t *testing.T) {
	t.Parallel()

	defaultPort := "443"
	tests := []struct {
		target       string
		expectedHost string
		expectedPort string
		expectedErr  error
	}{
		{
			target:       "www.google.com",
			expectedHost: "www.google.com",
			expectedPort: "443",
		},
		{
			target:       "google.com:50051",
			expectedHost: "google.com",
			expectedPort: "50051",
		},
		{
			target:       "1.2.3.4",
			expectedHost: "1.2.3.4",
			expectedPort: "443",
		},
		{
			target:       "1.2.3.4:50051",
			expectedHost: "1.2.3.4",
			expectedPort: "50051",
		},
		{
			target:       "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
			expectedHost: "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
			expectedPort: "443",
		},
		{
			target:       "[fe80::1ff:fe23:4567:890a]:50051",
			expectedHost: "fe80::1ff:fe23:4567:890a",
			expectedPort: "50051",
		},
		{
			target:      "[fe80::1ff:fe23:4567:890a]:",
			expectedErr: structerr.New("dns resolver: missing port after port-separator colon"),
		},
		{
			target:       ":50051",
			expectedHost: "localhost",
			expectedPort: "50051",
		},
		{
			target:      "",
			expectedErr: structerr.New("dns resolver: missing address"),
		},
		{
			target: "this:is:invalid:address",
			expectedErr: structerr.New("dns resolver: %w", &net.AddrError{
				Err:  "too many colons in address",
				Addr: "this:is:invalid:address:443",
			}),
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("target: %s", tc.target), func(t *testing.T) {
			host, port, err := ParseTarget(tc.target, defaultPort)

			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedHost, host)
			require.Equal(t, tc.expectedPort, port)
		})
	}
}

func TestTryParseIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host         string
		port         string
		expectedAddr string
		expectedOk   bool
	}{
		{host: "google.com", port: "50051", expectedOk: false},
		{host: "1.2.3", port: "50051", expectedOk: false},
		{host: "1.2.3.4", port: "50051", expectedAddr: "1.2.3.4:50051", expectedOk: true},
		{host: "64:ff9b::", port: "50051", expectedAddr: "[64:ff9b::]:50051", expectedOk: true},
		{host: "2001:0db8:85a3:0000:0000:8a2e:0370:7334", port: "50051", expectedAddr: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:50051", expectedOk: true},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("host: %s, port: %s", tc.host, tc.port), func(t *testing.T) {
			addr, ok := tryParseIP(tc.host, tc.port)

			require.Equal(t, tc.expectedOk, ok)
			require.Equal(t, tc.expectedAddr, addr)
		})
	}
}
