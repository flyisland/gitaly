package gitpipe

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// chanObjectIterator is an object iterator that can be driven via a set of channels for
// deterministically exercising specific conditions in tests.
type chanObjectIterator struct {
	ObjectIterator

	oid      git.ObjectID
	oidChan  <-chan git.ObjectID
	nextChan chan<- interface{}
}

// newChanObjectIterator returns a new object iterator as well as two channels: one object ID
// channel that can be used to inject the next value returned by `Next()`. And then a second value
// that is written to when `Next()` is called.
func newChanObjectIterator() (ObjectIterator, chan<- git.ObjectID, <-chan interface{}) {
	oidChan := make(chan git.ObjectID)
	nextChan := make(chan interface{})
	return &chanObjectIterator{
		oidChan:  oidChan,
		nextChan: nextChan,
	}, oidChan, nextChan
}

func (ch *chanObjectIterator) Next() bool {
	// Notify the caller that the next object was requested.
	ch.nextChan <- struct{}{}

	var ok bool
	ch.oid, ok = <-ch.oidChan
	return ok
}

func (ch *chanObjectIterator) ObjectID() git.ObjectID {
	return ch.oid
}

func (ch *chanObjectIterator) ObjectName() []byte {
	return []byte("idontcare")
}

func hashDependentObjectSize(tb testing.TB, sha1Size, sha256Size int64) int64 {
	return gittest.ObjectHashDependent(tb, map[string]int64{
		"sha1":   sha1Size,
		"sha256": sha256Size,
	})
}
