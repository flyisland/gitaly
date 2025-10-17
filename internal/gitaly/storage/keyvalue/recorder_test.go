package keyvalue

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestRecordingReadWriter(t *testing.T) {
	db, err := NewBadgerStore(testhelper.SharedLogger(t), t.TempDir())
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	preTX := db.NewTransaction(true)

	// Create some existing keys in the database. We'll read one of them later.
	require.NoError(t, preTX.Set([]byte("prefix-1/key-1"), []byte("prefix-1/value-1")))
	require.NoError(t, preTX.Commit())

	tx := db.NewTransaction(true)
	defer tx.Discard()

	rw := NewRecordingReadWriter(tx)

	// Set and delete some keys. These should become part of the write set.
	require.NoError(t, rw.Set([]byte("key-1"), []byte("value-1")))
	require.NoError(t, rw.Set([]byte("key-2"), []byte("value-2")))

	require.NoError(t, rw.Set([]byte("prefix-2/key-1"), []byte("prefix-2/value-1")))
	require.NoError(t, rw.Set([]byte("prefix-2/key-2"), []byte("prefix-2/value-2")))

	// The keys read directly should become part of the read set.
	_, err = rw.Get([]byte("prefix-1/key-1"))
	require.NoError(t, err)

	_, err = rw.Get([]byte("key-not-found"))
	require.Equal(t, badger.ErrKeyNotFound, err)

	// Iterated keys should become part of the read set and the prefix should also
	// be recorded.
	unprefixedIterator := rw.NewIterator(IteratorOptions{})
	defer unprefixedIterator.Close()

	RequireIterator(t, unprefixedIterator, KeyValueState{
		"key-1":          "value-1",
		"key-2":          "value-2",
		"prefix-1/key-1": "prefix-1/value-1",
		"prefix-2/key-1": "prefix-2/value-1",
		"prefix-2/key-2": "prefix-2/value-2",
	})

	unprefixedIterator.Seek([]byte("key-2"))
	require.True(t, unprefixedIterator.Valid())
	require.Equal(t, unprefixedIterator.Item().Key(), []byte("key-2"))
	require.NoError(t, unprefixedIterator.Item().Value(func(value []byte) error {
		require.Equal(t, []byte("value-2"), value)
		return nil
	}))

	// Non-existent keys that are seeked are not added to the read set. The iterator's
	// prefix is already recorded which covers the conflicts on reading this key and it
	// being created concurrently.
	// This will result in invalid iterator since there are no other keys greater than seeked.
	unprefixedIterator.Seek([]byte("seek-non-existent-key"))
	require.False(t, unprefixedIterator.Valid())
	// This will result in valid iterator since seek will go the the next key greater than seeked.
	unprefixedIterator.Seek([]byte("non-existent-key"))
	require.True(t, unprefixedIterator.Valid())
	require.Equal(t, unprefixedIterator.Item().Key(), []byte("prefix-1/key-1"))

	// Iterated keys should become part of the read set and the prefix should also
	// be recorded.
	prefixedIterator := rw.NewIterator(IteratorOptions{Prefix: []byte("prefix-2/")})
	defer prefixedIterator.Close()

	prefixedIterator.Seek([]byte("prefix-2/key-2"))
	require.True(t, prefixedIterator.Valid())
	require.Equal(t, prefixedIterator.Item().Key(), []byte("prefix-2/key-2"))
	require.NoError(t, prefixedIterator.Item().Value(func(value []byte) error {
		require.Equal(t, []byte("prefix-2/value-2"), value)
		return nil
	}))

	// Using wrong key during seek will seek to next key greater than seeked.
	prefixedIterator.Seek([]byte("prefix-1/key-2"))
	require.True(t, prefixedIterator.Valid())
	require.Equal(t, prefixedIterator.Item().Key(), []byte("prefix-2/key-1"))

	RequireIterator(t, prefixedIterator, KeyValueState{
		"prefix-2/key-1": "prefix-2/value-1",
		"prefix-2/key-2": "prefix-2/value-2",
	})

	require.NoError(t, rw.Delete([]byte("deleted-key")))
	require.NoError(t, rw.Delete([]byte("unread-key")))

	require.Equal(t, rw.ReadSet(), KeySet{
		"key-1":          {},
		"key-2":          {},
		"key-not-found":  {},
		"prefix-1/key-1": {},
		"prefix-2/key-1": {},
		"prefix-2/key-2": {},
	})

	require.Equal(t, rw.WriteSet(), KeySet{
		"key-1":          {},
		"key-2":          {},
		"unread-key":     {},
		"deleted-key":    {},
		"prefix-2/key-1": {},
		"prefix-2/key-2": {},
	})

	require.Equal(t, rw.PrefixesRead(), KeySet{
		"":          {},
		"prefix-2/": {},
	})
}
