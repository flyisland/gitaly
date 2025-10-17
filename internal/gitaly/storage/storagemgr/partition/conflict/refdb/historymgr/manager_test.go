package historymgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

func TestManager(t *testing.T) {
	zeroOID := gittest.DefaultObjectHash.ZeroOID

	requireState := func(
		t *testing.T,
		mgr *Manager,
		expectedHistoryByRelativePath map[string]struct{},
		expectedRelativePathsByLSN map[storage.LSN]string,
		expectedLSNsByRelativePath map[string]map[storage.LSN]struct{},
	) {
		t.Helper()

		actualHistoryByRelativePath := map[string]struct{}{}
		for key := range mgr.historyByRelativePath {
			actualHistoryByRelativePath[key] = struct{}{}
		}

		require.Equal(t, expectedHistoryByRelativePath, actualHistoryByRelativePath)
		require.Equal(t, expectedRelativePathsByLSN, mgr.relativePathsByLSN)
		require.Equal(t, expectedLSNsByRelativePath, mgr.lsnByRelativePath)
	}

	t.Run("no changes before transaction commits", func(t *testing.T) {
		mgr := New()

		tx := mgr.Begin("relative-path-1", zeroOID)

		// If the transaction is not committed, the state should not change.
		require.Equal(t, New(), mgr)

		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		// Committed transaction records the change.
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}},
			},
		)
	})

	t.Run("reuses existing histories for the same repository", func(t *testing.T) {
		mgr := New()

		tx := mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		// If we start a second transaction against the same relative path, it should
		// reuse the existing history.
		existingHistory := mgr.historyByRelativePath["relative-path-1"]

		tx = mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {OldOID: "oid-1", NewOID: "oid-2"}}))
		tx.Commit(2)

		require.Same(t, existingHistory, mgr.historyByRelativePath["relative-path-1"])
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}, 2: {}},
			},
		)
	})

	t.Run("uses different histories for different repositories", func(t *testing.T) {
		mgr := New()

		// Starting a transactions against different relative paths should target different
		// histories.
		tx := mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		tx = mgr.Begin("relative-path-2", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-2"}}))
		tx.Commit(2)

		require.NotSame(t, mgr.historyByRelativePath["relative-path-1"], mgr.historyByRelativePath["relative-path-2"])
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
				"relative-path-2": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-2",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}},
				"relative-path-2": {2: {}},
			},
		)
	})

	t.Run("evicting LSN without a history does nothing", func(t *testing.T) {
		mgr := New()

		tx := mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		// Evicting non-existent LSN does nothing.
		mgr.EvictLSN(0)
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}},
			},
		)
	})

	t.Run("evict retains non-empty histories", func(t *testing.T) {
		mgr := New()

		tx := mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		tx = mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {OldOID: "oid-1", NewOID: "oid-2"}}))
		tx.Commit(2)

		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}, 2: {}},
			},
		)

		// Evicting an LSN keeps the history still around if
		// there are further writes into it.
		mgr.EvictLSN(1)
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				2: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {2: {}},
			},
		)
	})

	t.Run("evict drops empty histories", func(t *testing.T) {
		mgr := New()

		tx := mgr.Begin("relative-path-1", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(1)

		tx = mgr.Begin("relative-path-2", zeroOID)
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{"refs/heads/main": {NewOID: "oid-1"}}))
		tx.Commit(2)

		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
				"relative-path-2": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-2",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}},
				"relative-path-2": {2: {}},
			},
		)

		// Evicting an LSN keeps drops the history if there are no
		// further writes to it.
		mgr.EvictLSN(1)
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-2": {},
			},
			map[storage.LSN]string{
				2: "relative-path-2",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-2": {2: {}},
			},
		)

		mgr.EvictLSN(2)
		requireState(t, mgr,
			map[string]struct{}{},
			map[storage.LSN]string{},
			map[string]map[storage.LSN]struct{}{},
		)
	})

	t.Run("empty transactions are handled the same as others", func(t *testing.T) {
		mgr := New()

		mgr.Begin("relative-path-1", zeroOID).Commit(1)
		mgr.Begin("relative-path-1", zeroOID).Commit(2)

		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				1: "relative-path-1",
				2: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {1: {}, 2: {}},
			},
		)

		mgr.EvictLSN(1)
		requireState(t, mgr,
			map[string]struct{}{
				"relative-path-1": {},
			},
			map[storage.LSN]string{
				2: "relative-path-1",
			},
			map[string]map[storage.LSN]struct{}{
				"relative-path-1": {2: {}},
			},
		)

		mgr.EvictLSN(2)
		requireState(t, mgr,
			map[string]struct{}{},
			map[storage.LSN]string{},
			map[string]map[storage.LSN]struct{}{},
		)
	})
}
