package datastore

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
)

func TestRepositoryReferenceWriteLock(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)
	dbConfig := testdb.GetConfig(t, db.Name)
	logger := testhelper.NewLogger(t)
	ctx := testhelper.Context(t)

	// waitForListener is needed when a test relies on the listener actually
	// receiving a NOTIFY. Without waiting, the test is flaky since we can't
	// guarantee the notification channel is ready to listen before a NOTIFY fires.
	//
	// Call this after creating a new RepoReferenceWriteLockManager.
	// Not all tests need it, but it is provided to avoid unexpected flakiness.
	waitForListener := func(ready <-chan struct{}) {
		t.Helper()
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("listener never connected")
		}
	}

	// t.Run subtests use different relative paths so they can run in parallel
	// without conflicting on the same lock_id.
	t.Run("txn acquires lock successfully", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		unlock, renew, err := mgr.Lock(ctx, "default", "repo/acquire.git", 1)
		require.NoError(t, err)
		require.NotNil(t, unlock)
		require.NotNil(t, renew)

		require.NoError(t, unlock())
	})

	t.Run("callers with same txn id acquires lock successfully", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		wg := &sync.WaitGroup{}
		callerNum := 3
		unlockCalls := make([]func() error, callerNum)
		wg.Add(callerNum)
		for i := 0; i < callerNum; i++ {
			go func(j int) {
				defer wg.Done()
				unlock, renew, err := mgr.Lock(ctx, "default", "acquire-reentrant.git", 1)
				require.NoError(t, err)
				require.NotNil(t, unlock)
				require.NotNil(t, renew)
				unlockCalls[j] = unlock
			}(i)
		}
		wg.Wait()
		for _, unlock := range unlockCalls {
			require.NoError(t, unlock())
		}
	})

	t.Run("first txn release lock and second txn acquires it lock afterward", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		storageName := "default"
		relativePath := "repo/repo/release_and_acquire.git"
		unlock1, renew1, err1 := mgr.Lock(ctx, storageName, relativePath, 1)
		require.NoError(t, err1)
		require.NotNil(t, unlock1)
		require.NotNil(t, renew1)

		go func() {
			// The first caller releases the lock after 1s
			<-time.After(1 * time.Second)
			require.NoError(t, unlock1())
		}()

		// The second should block here and eventually have the lock after it is released
		unlock2, renew2, err2 := mgr.Lock(ctx, storageName, relativePath, 2)
		require.NoError(t, err2)
		require.NotNil(t, unlock2)
		require.NotNil(t, renew2)
		require.NoError(t, unlock2())
	})

	t.Run("second txn fails to acquire held lock and receives notification channel", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		res1 := mgr.tryLock(ctx, "default", "repo/contend.git", 1)
		require.NoError(t, res1.Err)
		require.True(t, res1.Acquired)
		require.NotNil(t, res1.Unlock)
		require.NotNil(t, res1.Renew)
		require.Nil(t, res1.NotificationCh)
		require.Nil(t, res1.Deregister)

		res2 := mgr.tryLock(ctx, "default", "repo/contend.git", 2)
		require.NoError(t, res2.Err)
		require.False(t, res2.Acquired)
		require.Nil(t, res2.Unlock)
		require.Nil(t, res2.Renew)
		require.NotNil(t, res2.NotificationCh)
		require.NotNil(t, res2.Deregister)
		defer res2.Deregister()

		require.NoError(t, res1.Unlock())
	})

	t.Run("txn2 notification channel is closed when txn1 releases the lock", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		// This test requires waitForListener to guarantee the listener is ready.
		waitForListener(mgr.handler.ready)
		res1 := mgr.tryLock(ctx, "default", "repo/notify.git", 1)
		require.NoError(t, res1.Err)
		require.True(t, res1.Acquired)

		res2 := mgr.tryLock(ctx, "default", "repo/notify.git", 2)
		require.NoError(t, res2.Err)
		require.False(t, res2.Acquired)
		require.NotNil(t, res2.NotificationCh)
		require.NotNil(t, res2.Deregister)
		defer res2.Deregister()

		require.NoError(t, res1.Unlock())

		// The listener goroutine needs to connect and receive the NOTIFY from the
		// DELETE trigger. Use Eventually to handle the async delivery.
		require.Eventually(t, func() bool {
			select {
			case <-res2.NotificationCh:
				return true
			default:
				return false
			}
		}, 5*time.Second, 50*time.Millisecond, "timed out waiting for lock release notification")
	})

	t.Run("txn2 can steal an expired lock", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		_, _, err1 := mgr.Lock(ctx, "default", "repo/expire.git", 1)
		require.NoError(t, err1)

		// Simulate expiry by back-dating the lock directly in the DB.
		_, err := db.ExecContext(ctx, `
			UPDATE repository_reference_write_locks
			SET expired_at = NOW() - INTERVAL '1 second'
			WHERE lock_id = 'default|repo/expire.git'`)
		require.NoError(t, err)

		unlock2, _, err2 := mgr.Lock(ctx, "default", "repo/expire.git", 2)
		require.NoError(t, err2)
		require.NoError(t, unlock2())
	})

	t.Run("renew extends lock and prevents acquisition by another txn", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		res1 := mgr.tryLock(ctx, "default", "repo/renew.git", 1)
		require.NoError(t, res1.Err)
		require.NoError(t, res1.Renew())
		defer func() {
			require.NoError(t, res1.Unlock())
		}()

		// After renewal the lock is still held; txn2 must not acquire it.
		res2 := mgr.tryLock(ctx, "default", "repo/renew.git", 2)
		require.NoError(t, res2.Err)
		require.False(t, res2.Acquired)
		require.NotNil(t, res2.NotificationCh)
		require.NotNil(t, res2.Deregister)
		defer res2.Deregister()
	})

	t.Run("deregister removes the waiter from the dispatcher", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		res1 := mgr.tryLock(ctx, "default", "repo/deregister.git", 1)
		require.NoError(t, res1.Err)
		require.True(t, res1.Acquired)
		defer func() {
			require.NoError(t, res1.Unlock())
		}()

		res2 := mgr.tryLock(ctx, "default", "repo/deregister.git", 2)
		require.NoError(t, res2.Err)
		require.NotNil(t, res2.Deregister)
		res2.Deregister()

		mgr.handler.mu.Lock()
		waiters := mgr.handler.waiters["default|repo/deregister.git"]
		mgr.handler.mu.Unlock()
		require.Empty(t, waiters)
	})

	t.Run("locks on different repos are independent", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		unlock1, _, err1 := mgr.Lock(ctx, "default", "repo/independent-a.git", 1)
		require.NoError(t, err1)
		defer func() {
			require.NoError(t, unlock1())
		}()

		// A different repo must be lockable even while the first is held.
		unlock2, _, err2 := mgr.Lock(ctx, "default", "repo/independent-b.git", 2)
		require.NoError(t, err2)
		defer func() {
			require.NoError(t, unlock2())
		}()
	})

	t.Run("cleanup removes expired locks from the database", func(t *testing.T) {
		t.Parallel()

		_, err := db.ExecContext(ctx, `
			INSERT INTO repository_reference_write_locks (lock_id, holder_txn_id, expired_at)
			VALUES ('default|repo/cleanup-expired.git', 99, NOW() - INTERVAL '1 hour')`)
		require.NoError(t, err)

		cleanUpExpiredRepoRefWriteLocks(ctx, db, logger)

		var count int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM repository_reference_write_locks
			WHERE lock_id = 'default|repo/cleanup-expired.git'`).Scan(&count))
		require.Equal(t, 0, count)
	})

	t.Run("cleanup does not remove locks that have not expired", func(t *testing.T) {
		t.Parallel()
		mgr := NewRepoReferenceWriteLockManager(ctx, db, dbConfig, logger)
		waitForListener(mgr.handler.ready)

		res := mgr.tryLock(ctx, "default", "repo/cleanup-active.git", 1)
		require.NoError(t, res.Err)
		defer func() {
			require.NoError(t, res.Unlock())
		}()

		cleanUpExpiredRepoRefWriteLocks(ctx, db, logger)

		var count int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM repository_reference_write_locks
			WHERE lock_id = 'default|repo/cleanup-active.git'`).Scan(&count))
		require.Equal(t, 1, count)
	})
}
