package conflict

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestManager(t *testing.T) {
	ctx := testhelper.Context(t)

	requireState := func(t *testing.T, mgr *Manager,
		expectedRepositoryDeletions map[string]storage.LSN,
		expectedRepositoryDeletionsByLSN map[storage.LSN]string,
	) {
		require.Equal(t, expectedRepositoryDeletions, mgr.repositoryDeletions)
		require.Equal(t, expectedRepositoryDeletionsByLSN, mgr.repositoryDeletionsByLSN)
	}

	t.Run("empty state produces no conflicts", func(t *testing.T) {
		mgr := NewManager()

		_, err := mgr.Prepare(ctx, &Transaction{
			ReadLSN:            1,
			TargetRelativePath: "relative-path-1",
		})
		require.NoError(t, err)

		require.Equal(t, NewManager(), mgr)
	})

	t.Run("discarding transaction makes no changes", func(T *testing.T) {
		mgr := NewManager()

		tx, err := mgr.Prepare(ctx, &Transaction{
			ReadLSN:            1,
			DeleteRepository:   true,
			TargetRelativePath: "relative-path-1",
		})
		require.NoError(t, err)
		require.NotNil(t, tx)

		require.Equal(t, NewManager(), mgr)
	})

	t.Run("deletions before read LSN do not conflict", func(t *testing.T) {
		mgr := NewManager()

		tx, err := mgr.Prepare(ctx, &Transaction{
			TargetRelativePath: "relative-path-1",
			DeleteRepository:   true,
		})
		require.NoError(t, err)
		tx.Commit(ctx, 1)

		_, err = mgr.Prepare(ctx, &Transaction{
			ReadLSN:            2,
			TargetRelativePath: "relative-path-1",
		})
		require.NoError(t, err)

		requireState(t, mgr,
			map[string]storage.LSN{
				"relative-path-1": 1,
			},
			map[storage.LSN]string{
				1: "relative-path-1",
			},
		)
	})

	t.Run("deletions after read LSN conflict", func(t *testing.T) {
		mgr := NewManager()

		tx, err := mgr.Prepare(ctx, &Transaction{
			ReadLSN:            1,
			TargetRelativePath: "relative-path-1",
			DeleteRepository:   true,
		})
		require.NoError(t, err)
		tx.Commit(ctx, 2)

		tx, err = mgr.Prepare(ctx, &Transaction{
			ReadLSN:            1,
			TargetRelativePath: "relative-path-1",
		})
		require.Equal(t, ErrRepositoryConcurrentlyDeleted, err)
		require.Nil(t, tx)

		requireState(t, mgr,
			map[string]storage.LSN{
				"relative-path-1": 2,
			},
			map[storage.LSN]string{
				2: "relative-path-1",
			},
		)
	})

	t.Run("evict LSN drops the relevant state", func(t *testing.T) {
		mgr := NewManager()

		tx, err := mgr.Prepare(ctx, &Transaction{
			TargetRelativePath: "relative-path-1",
			DeleteRepository:   true,
		})
		require.NoError(t, err)
		tx.Commit(ctx, 1)

		tx, err = mgr.Prepare(ctx, &Transaction{
			TargetRelativePath: "relative-path-2",
			DeleteRepository:   true,
		})
		require.NoError(t, err)
		tx.Commit(ctx, 2)

		// Evicting a non-existent LSN does nothing.
		mgr.EvictLSN(ctx, 0)

		requireState(t, mgr,
			map[string]storage.LSN{
				"relative-path-1": 1,
				"relative-path-2": 2,
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-2",
			},
		)

		mgr.EvictLSN(ctx, 1)

		requireState(t, mgr,
			map[string]storage.LSN{
				"relative-path-2": 2,
			},
			map[storage.LSN]string{
				2: "relative-path-2",
			},
		)

		mgr.EvictLSN(ctx, 2)

		requireState(t, mgr, map[string]storage.LSN{}, map[storage.LSN]string{})
	})
}
