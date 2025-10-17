package streamcache

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func testNullCache() *TestLoggingCache { return &TestLoggingCache{Cache: NullCache{}} }

func TestMinOccurrences_suppress(t *testing.T) {
	ctx := testhelper.Context(t)

	tc := testNullCache()
	c := &minOccurrences{
		N:      1,
		MinAge: 5 * time.Minute,
		Cache:  tc,
	}
	defer c.Stop()

	for i := 0; i < 10; i++ {
		output := &bytes.Buffer{}
		_, _, err := c.Fetch(ctx, "key", output, writeString("hello"))
		require.NoError(t, err)
		require.Equal(t, "hello", output.String())

		if i < c.N {
			require.Empty(t, tc.Entries(), "expect first call not to have reached underlying cache")
		} else {
			require.Len(t, tc.Entries(), 1+i-c.N, "call should reach underlying cache")
		}
	}
}

func TestMinOccurrences_manyDifferentCalls(t *testing.T) {
	ctx := testhelper.Context(t)

	tc := testNullCache()
	c := &minOccurrences{
		N:      1,
		MinAge: 5 * time.Minute,
		Cache:  tc,
	}
	defer c.Stop()

	const calls = 1000
	fetchErrors := make(chan error, calls)
	start := make(chan struct{})
	for i := 0; i < calls; i++ {
		go func(i int) {
			<-start
			_, _, err := c.Fetch(ctx, fmt.Sprintf("key.%d", i), io.Discard, writeString(fmt.Sprintf("hello.%d", i)))
			fetchErrors <- err
		}(i)
	}

	close(start)
	for i := 0; i < calls; i++ {
		require.NoError(t, <-fetchErrors)
	}

	require.Empty(t, tc.Entries(), "because every key was unique, no calls should have reached underlying cache")
}

type stopCache struct {
	NullCache
	stopped int
}

func (sc *stopCache) Stop() { sc.stopped++ }

func TestMinOccurrences_propagateStop(t *testing.T) {
	sc := &stopCache{}
	c := minOccurrences{Cache: sc}
	require.Equal(t, 0, sc.stopped)

	c.Stop()
	require.Equal(t, 1, sc.stopped)
}

func (mo *minOccurrences) size() int {
	mo.m.Lock()
	defer mo.m.Unlock()
	return len(mo.oldCount) + len(mo.newCount)
}

func TestMinOccurrences_forgetOldKeys(t *testing.T) {
	ctx := testhelper.Context(t)

	tc := testNullCache()
	c := &minOccurrences{
		N:      1,
		MinAge: time.Hour,
		Cache:  tc,
	}
	defer c.Stop()

	require.Equal(t, 0, c.size())

	const calls = 1000
	for i := 0; i < calls; i++ {
		_, _, err := c.Fetch(ctx, fmt.Sprintf("old key.%d", i), io.Discard, writeString("hello"))
		require.NoError(t, err)
	}
	require.Equal(t, calls, c.size(), "old keys")

	c.rotatedAt = time.Now().Add(-2 * c.MinAge)
	_, _, _ = c.Fetch(ctx, "new key", io.Discard, writeString("hello"))
	require.Equal(t, calls+1, c.size(), "old keys and new key")

	c.rotatedAt = time.Now().Add(-2 * c.MinAge)
	_, _, _ = c.Fetch(ctx, "new key", io.Discard, writeString("hello"))
	require.Equal(t, 1, c.size(), "only new key")
}

func TestMinOccurrences_keySurvivesRotation(t *testing.T) {
	tc := testNullCache()
	c := &minOccurrences{
		N:      1,
		MinAge: time.Hour,
		Cache:  tc,
	}
	defer c.Stop()

	for i := 0; i < 1000; i++ {
		c.rotatedAt = time.Now().Add(-2 * c.MinAge)
		require.Equal(t, i+1, c.incr("frequent key"), "frequently occurring key is remembered and incremented")

		// Use i%3 because it takes 2 rotations for a key to be forgotten
		if i%3 == 0 {
			require.Equal(t, 1, c.incr("infrequent key"), "infrequent key is forgotten and reset")
		}
	}
}
