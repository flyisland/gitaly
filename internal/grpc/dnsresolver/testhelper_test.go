package dnsresolver

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// fakeClientConn stubs resolver.ClientConn. It captures all states and errors received from the
// resolver. As the resolver runs asynchronously, we cannot control when and how many states are
// pushed to the connection. For testing purpose, the fake connection doesn't need to capture all
// states and errors. Instead, it captures some first states and errors (configured via waitState
// and waitError). The caller is expected to call Wait(). This method returns when enough data
// arrives. Afterward, further data are rejected.
type fakeClientConn struct {
	customUpdateState func(resolver.State) error
	states            []resolver.State
	errors            []error

	waitState     int
	waitStateChan chan struct{}

	waitError     int
	waitErrorChan chan struct{}
}

func newFakeClientConn(waitState int, waitError int) *fakeClientConn {
	return &fakeClientConn{
		waitState:     waitState,
		waitStateChan: make(chan struct{}, waitState),
		waitError:     waitError,
		waitErrorChan: make(chan struct{}, waitError),
	}
}

func (c *fakeClientConn) UpdateState(state resolver.State) error {
	if c.customUpdateState != nil {
		return c.customUpdateState(state)
	}
	return c.doUpdateState(state)
}

func (c *fakeClientConn) doUpdateState(state resolver.State) error {
	// Do nothing if received enough states
	if len(c.states) >= c.waitState {
		return nil
	}
	c.states = append(c.states, state)
	c.waitStateChan <- struct{}{}

	return nil
}

func (c *fakeClientConn) Wait() {
	for i := 0; i < c.waitState; i++ {
		<-c.waitStateChan
	}
	for i := 0; i < c.waitError; i++ {
		<-c.waitErrorChan
	}
}

func (c *fakeClientConn) ReportError(err error) {
	// Do nothing if received enough errors
	if len(c.errors) >= c.waitError {
		return
	}
	c.errors = append(c.errors, err)
	c.waitErrorChan <- struct{}{}
}

func (c *fakeClientConn) NewAddress(_ []resolver.Address) { panic("deprecated") }

func (c *fakeClientConn) NewServiceConfig(_ string) { panic("deprecated") }

func (c *fakeClientConn) ParseServiceConfig(_ string) *serviceconfig.ParseResult { panic("deprecated") }

// fakeBackoff stubs the exponential Backoff strategy. It always returns 0 regardless of the retry
// attempts.
type fakeBackoff struct{}

func (c *fakeBackoff) Backoff(uint) time.Duration {
	return 0
}

// fakeLookup stubs the DNS lookup. It wraps around a real DNS lookup. The caller can return an
// alternative addresses, errors, or fallback to use the real DNS lookup if needed.
type fakeLookup struct {
	realLookup dnsLookuper
	stubLookup func(context.Context, string) ([]string, error)
}

func (f *fakeLookup) LookupHost(ctx context.Context, s string) ([]string, error) {
	return f.stubLookup(ctx, s)
}

func newFakeLookup(t *testing.T, authority string) *fakeLookup {
	lookup, err := findDNSLookup(authority)
	require.NoError(t, err)

	return &fakeLookup{realLookup: lookup}
}

func buildResolverTarget(s *testhelper.FakeDNSServer, addr string) resolver.Target {
	return resolver.Target{URL: url.URL{
		Scheme: "dns",
		Host:   s.Addr(),
		Path:   fmt.Sprintf("/%s", addr),
	}}
}

func newTestDNSBuilder(t *testing.T) *Builder {
	return NewBuilder(&BuilderConfig{
		RefreshRate:   0,
		LookupTimeout: 10 * time.Second,
		Logger:        testhelper.SharedLogger(t),
		Backoff:       &fakeBackoff{},
	})
}

// ipList simulates the list of IPs returned by the DNS server in order
type ipList struct {
	ips   [][]string
	mutex sync.Mutex
	index int
}

func (list *ipList) next() []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.index < len(list.ips) {
		list.index++
		return list.ips[list.index-1]
	}
	return nil
}

func (list *ipList) peek() []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.index < len(list.ips) {
		return list.ips[list.index]
	}
	return nil
}
