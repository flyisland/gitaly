package raftmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestConfChangeContext_EncodeDecode(t *testing.T) {
	t.Parallel()

	metadata := &gitalypb.ReplicaID_Metadata{
		Address: "unix:///tmp/test.socket",
	}

	t.Run("encode and decode with event ID and metadata", func(t *testing.T) {
		changes := NewReplicaConfChanges(1, 2, 3, 12345, metadata)

		// Encode
		contextBytes, err := changes.encodeContext()
		require.NoError(t, err)
		require.NotEmpty(t, contextBytes)

		// Decode
		eventID, decodedMetadata, err := parseContext(contextBytes)
		require.NoError(t, err)
		require.Equal(t, EventID(12345), eventID)
		require.NotNil(t, decodedMetadata)
		require.Equal(t, "unix:///tmp/test.socket", decodedMetadata.GetAddress())
	})

	t.Run("encode and decode with only event ID", func(t *testing.T) {
		changes := NewReplicaConfChanges(1, 2, 3, 67890, nil)

		// Encode
		contextBytes, err := changes.encodeContext()
		require.NoError(t, err)
		require.NotEmpty(t, contextBytes)

		// Decode
		eventID, decodedMetadata, err := parseContext(contextBytes)
		require.NoError(t, err)
		require.Equal(t, EventID(67890), eventID)
		require.Nil(t, decodedMetadata)
	})

	t.Run("encode and decode with only metadata", func(t *testing.T) {
		changes := NewReplicaConfChanges(1, 2, 3, 0, metadata)

		contextBytes, err := changes.encodeContext()
		require.NoError(t, err)
		require.NotEmpty(t, contextBytes)

		eventID, decodedMetadata, err := parseContext(contextBytes)
		require.NoError(t, err)
		require.Equal(t, EventID(0), eventID)
		require.NotNil(t, decodedMetadata)
		require.Equal(t, "unix:///tmp/test.socket", decodedMetadata.GetAddress())
	})

	t.Run("empty context", func(t *testing.T) {
		eventID, metadata, err := parseContext(nil)
		require.NoError(t, err)
		require.Equal(t, EventID(0), eventID)
		require.Nil(t, metadata)

		eventID, metadata, err = parseContext([]byte{})
		require.NoError(t, err)
		require.Equal(t, EventID(0), eventID)
		require.Nil(t, metadata)
	})
}
