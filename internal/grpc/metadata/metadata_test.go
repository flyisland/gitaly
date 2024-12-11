package metadata

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestOutgoingToIncoming(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx, err := storage.InjectGitalyServers(ctx, "a", "b", "c")
	require.NoError(t, err)

	_, err = storage.ExtractGitalyServer(ctx, "a")
	require.Equal(t, errors.New("empty gitaly-servers metadata"), err,
		"server should not be found in the incoming context")

	ctx = OutgoingToIncoming(ctx)

	info, err := storage.ExtractGitalyServer(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, storage.ServerInfo{Address: "b", Token: "c"}, info)
}

func TestAppendToIncomingContext(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = AppendToIncomingContext(ctx, "test-key", "test-value")
	newCtx := AppendToIncomingContext(ctx, "test-new-key", "test-new-value")

	require.Equal(t, "test-value", GetValue(ctx, "test-key"))
	require.Equal(t, "", GetValue(ctx, "test-new-key")) // new key should not be present in the old context

	require.Equal(t, "test-value", GetValue(newCtx, "test-key"))
	require.Equal(t, "test-new-value", GetValue(newCtx, "test-new-key"))
}
