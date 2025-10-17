package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestContextWithTransactionID(t *testing.T) {
	t.Run("no transaction id in context", func(t *testing.T) {
		require.Equal(t,
			TransactionID(0),
			ExtractTransactionID(testhelper.Context(t)),
		)
	})

	t.Run("transaction id in context", func(t *testing.T) {
		require.Equal(t,
			TransactionID(1),
			ExtractTransactionID(
				ContextWithTransactionID(testhelper.Context(t), 1),
			),
		)
	})
}

type nilTransaction struct{ Transaction }

func TestContextWithTransaction(t *testing.T) {
	t.Run("no transaction in context", func(t *testing.T) {
		require.Nil(t, ExtractTransaction(context.Background()))
	})

	t.Run("transaction in context", func(t *testing.T) {
		expectedTX := &nilTransaction{}
		require.Same(t, expectedTX, ExtractTransaction(ContextWithTransaction(context.Background(), expectedTX)))
	})
}

func TestPartitioningHint(t *testing.T) {
	t.Run("no hint provided", func(t *testing.T) {
		ctx := context.Background()

		relativePath, err := ExtractPartitioningHint(ctx)
		require.NoError(t, err)
		require.Empty(t, relativePath)
	})

	t.Run("hint provided", func(t *testing.T) {
		ctx := ContextWithPartitioningHint(context.Background(), "relative-path")

		relativePath, err := ExtractPartitioningHint(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")
	})

	t.Run("doesn't modify original metadata", func(t *testing.T) {
		originalMetadata := metadata.New(nil)
		originalCtx := metadata.NewIncomingContext(context.Background(), originalMetadata)

		ctx := ContextWithPartitioningHint(originalCtx, "relative-path")

		relativePath, err := ExtractPartitioningHint(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")

		relativePath, err = ExtractPartitioningHint(originalCtx)
		require.NoError(t, err)
		require.Empty(t, relativePath)
	})

	t.Run("fails if multiple hints set", func(t *testing.T) {
		md := metadata.New(nil)
		md.Set(keyPartitioningHint, "relative-path-1", "relative-path-2")

		relativePath, err := ExtractPartitioningHint(
			metadata.NewIncomingContext(context.Background(), md),
		)
		require.Equal(t, errors.New("multiple partitioning hints"), err)
		require.Empty(t, relativePath)
	})

	t.Run("removes the hint", func(t *testing.T) {
		ctx := ContextWithPartitioningHint(context.Background(), "relative-path")

		relativePath, err := ExtractPartitioningHint(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")

		ctx = ContextWithoutPartitioningHint(ctx)
		relativePath, err = ExtractPartitioningHint(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "")
	})
}
