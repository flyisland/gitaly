package datastore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/glsql"
)

// RepositoryReferenceWriteLockReleasesChannel is the PostgreSQL channel on
// which notify_on_write_lock_release() sends NOTIFY events.
const RepositoryReferenceWriteLockReleasesChannel = "repository_reference_write_lock_releases"

// LockID identifies a repository reference write lock, formatted as "virtualStorage|relativePath".
type LockID string

// RepoReferenceWriteLockManager manages per-repository reference write locks backed by
// PostgreSQL. It serializes write requests to the same repository by ensuring only one
// transaction holds the lock at a time. Callers use Lock to acquire the lock.
//
// PostgreSQL-backed locks are used instead of in-process mutexes to coordinate across
// multiple Praefect instances sharing the same database.
type RepoReferenceWriteLockManager struct {
	qc            glsql.Querier
	handler       *lockReleaseDispatcher
	renewInterval time.Duration
	logger        log.Logger

	// lockReleasingListener listens for PostgreSQL NOTIFY events when locks are released.
	// Held here to expose its reconnect metrics via Collect.
	lockReleasingListener *ResilientListener
	// lockAcquiredAt tracks when each lock was successfully acquired, keyed by lockID
	// (virtualStorage|relativePath). Used to compute hold duration at Unlock time.
	lockAcquiredAt sync.Map
	// lockAcquiredTotal counts tryLock attempts by result: "new_acquisition", "contended", or "error".
	lockAcquiredTotal *prometheus.CounterVec
	// locksHeld is the current number of locks held, per virtual storage.
	locksHeld *prometheus.GaugeVec
	// lockHoldDuration observes how long each lock was held (tryLock success → Unlock).
	lockHoldDuration *prometheus.HistogramVec
	// operationDuration observes database round-trip time per operation (trylock/unlock/renew).
	operationDuration *prometheus.HistogramVec
}

// lockReleaseDispatcher implements glsql.ListenHandler and fans out PostgreSQL lock release
// notifications to goroutines waiting on a specific lock. Waiters register via
// RegisterForLockRelease and are signalled by closing their channel when the corresponding
// lock_id is deleted from the database.
type lockReleaseDispatcher struct {
	mu      sync.Mutex
	waiters map[LockID][]chan struct{} // lockID → list of waiters

	// ready is a signal channel that indicates the listener is connected.
	// It is used in tests to provide proper synchronization.
	ready     chan struct{}
	readyOnce sync.Once
}

// Notification signals all goroutines waiting on the released lock_id by closing their channels,
// then removes them from the waiters map.
func (d *lockReleaseDispatcher) Notification(n glsql.Notification) {
	lockID := LockID(n.Payload)
	d.mu.Lock()
	chs := d.waiters[lockID]
	delete(d.waiters, lockID)
	d.mu.Unlock()
	for _, ch := range chs {
		close(ch) // wake all waiters
	}
}

// RegisterForLockRelease registers the caller as a waiter for the lock identified by lockID.
// It returns a channel that will be closed when the lock is released, and a deregister
// function that the caller must invoke if it stops waiting before the channel is closed
// (e.g. on context cancellation), to remove itself from the waiters map.
func (d *lockReleaseDispatcher) RegisterForLockRelease(lock LockID) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	d.mu.Lock()
	d.waiters[lock] = append(d.waiters[lock], ch)
	d.mu.Unlock()
	return ch, func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		chs := d.waiters[lock]
		for i, w := range chs {
			if w == ch {
				d.waiters[lock] = append(chs[:i], chs[i+1:]...)
				if len(d.waiters[lock]) == 0 {
					delete(d.waiters, lock)
				}
				break
			}
		}
	}
}

// Disconnect is a no-op; waiters are woken on reconnect in Connected.
func (d *lockReleaseDispatcher) Disconnect(error) {
	// Disconnect is a no-op. Waiters are woken up in Connected() once the
	// listener reconnects, at which point they retry and re-register if needed.
}

// Connected would be triggered once a connection to remote service is established.
func (d *lockReleaseDispatcher) Connected() {
	d.readyOnce.Do(func() { close(d.ready) }) // synchronization on tests, not used in production

	// Wake all current waiters so they retry lock and discover whether the lock
	// is free. This handles the case where a lock was released and its NOTIFY was
	// fired while the listener was disconnected and missed. Since the listener is
	// now re-subscribed, any subsequent release will be captured, so waiters that
	// re-register after this point are safe.
	d.mu.Lock()
	waiters := d.waiters
	d.waiters = make(map[LockID][]chan struct{})
	d.mu.Unlock()
	for _, chs := range waiters {
		for _, ch := range chs {
			close(ch)
		}
	}
}

// NewRepoReferenceWriteLockManager creates a new RepoReferenceWriteLockManager. It starts
// a background listener for lock release notifications and a background job to
// clean up expired locks. Both run until ctx is cancelled.
func NewRepoReferenceWriteLockManager(ctx context.Context, qc glsql.Querier, conf config.DB, logger log.Logger) *RepoReferenceWriteLockManager {
	resilientListenerTicker := helper.NewTimerTicker(5 * time.Second)

	lockReleasingListener := NewResilientListener(conf, resilientListenerTicker, logger)
	handler := &lockReleaseDispatcher{
		waiters: make(map[LockID][]chan struct{}),
		ready:   make(chan struct{}),
	}

	// In production, we start the listener asynchronously to speed up startup.
	// Conceptually, we could block in the constructor until the connection is established,
	// since the manager cannot function correctly without the listener. However, this has
	// a downside: if the database is temporarily slow during startup, the constructor may hang.
	go func() {
		err := lockReleasingListener.Listen(ctx, handler, RepositoryReferenceWriteLockReleasesChannel)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.WithError(err).Error("notifications listener terminated")
		}
	}()

	// Start a scheduled cleanup background job to remove expired locks.
	lockRenewInterval := 20 * time.Second
	cleanUpJobTicker := helper.NewTimerTicker(2 * lockRenewInterval)
	go func() {
		defer cleanUpJobTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-cleanUpJobTicker.C():
				cleanUpExpiredRepoRefWriteLocks(ctx, qc, logger)
			}
		}
	}()

	return &RepoReferenceWriteLockManager{
		qc:            qc,
		handler:       handler,
		renewInterval: lockRenewInterval,
		logger:        logger,

		lockReleasingListener: lockReleasingListener,
		lockAcquiredTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_praefect_repo_reference_write_lock_acquired_total",
				Help: "Total number of repository reference write locks acquired for the first time, partitioned by virtual storage.",
			},
			[]string{"virtual_storage", "result"},
		),
		locksHeld: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_praefect_repo_reference_write_locks_held",
				Help: "Current number of repository reference write locks held, per virtual storage.",
			},
			[]string{"virtual_storage"},
		),
		lockHoldDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_praefect_repo_reference_write_lock_hold_duration_seconds",
				Help:    "Duration in seconds that repository reference write locks are held (from successful acquisition to unlock).",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"virtual_storage"},
		),
		operationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_praefect_repo_reference_write_lock_operation_duration_seconds",
				Help:    "Duration in seconds of repository reference write lock database operations, by operation (trylock, unlock, renew).",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"virtual_storage", "operation"},
		),
	}
}

// Describe implements prometheus.Collector.
func (r *RepoReferenceWriteLockManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, descs)
}

// Collect implements prometheus.Collector.
func (r *RepoReferenceWriteLockManager) Collect(ch chan<- prometheus.Metric) {
	r.lockAcquiredTotal.Collect(ch)
	r.locksHeld.Collect(ch)
	r.lockHoldDuration.Collect(ch)
	r.operationDuration.Collect(ch)
	r.lockReleasingListener.Collect(ch)
}

// Lock acquires the write lock for a given virtualStorage, relativePath and txnID,
// blocking until the lock is acquired or ctx is cancelled.
func (r *RepoReferenceWriteLockManager) Lock(ctx context.Context, virtualStorage, relativePath string, txnID uint64,
) (unlock func() error, renew func() error, retErr error) {
	for {
		res := r.tryLock(ctx, virtualStorage, relativePath, txnID)
		if res.Err != nil {
			return nil, nil, res.Err
		}
		if res.Acquired {
			return res.Unlock, res.Renew, nil
		}

		select {
		case <-ctx.Done():
			res.Deregister()
			return nil, nil, fmt.Errorf("wait for lock: %w", ctx.Err())
		case <-res.NotificationCh:
			// wait for the lock to release
		}
	}
}

type tryLockResult struct {
	Acquired       bool
	Unlock         func() error
	Renew          func() error
	NotificationCh <-chan struct{}
	Deregister     func()
	Err            error
}

// tryLock attempts a single non-blocking acquisition of the write lock for
// a given virtualStorage, relativePath and txnID.
// If the same txnID already holds the lock, the call is a no-op and returns
// as acquired.
//
// If tryLockResult.Err is non-nil, the request has failed and the caller should retry
// after a brief delay.
//
// If tryLockResult.Acquired is true, the lock has been successfully acquired. The caller
// should invoke either the tryLockResult.Unlock() or tryLockResult.Renew() closure.
//
// If tryLockResult.Acquired is false, the lock is held by a different txnID. The caller
// should listen on tryLockResult.NotificationCh to be notified when the lock is released.
// The caller must call tryLockResult.Deregister() if aborting the current operation: for
// example, if the request context is cancelled.
func (r *RepoReferenceWriteLockManager) tryLock(ctx context.Context, virtualStorage, relativePath string, txnID uint64,
) tryLockResult {
	lockID := repoLockID(virtualStorage, relativePath)
	// Register for a lock release notification before attempting the INSERT, so
	// that no release event can be missed between a failed attempt and the caller
	// beginning to wait, thus eliminating the race window between contention detection
	// and notification.
	notificationCh, deregister := r.handler.RegisterForLockRelease(lockID)

	query := `
INSERT INTO repository_reference_write_locks as locks (lock_id, holder_txn_id, expired_at)
VALUES ($1, $2, NOW() + $3::interval)
ON CONFLICT (lock_id) DO UPDATE
  SET holder_txn_id = EXCLUDED.holder_txn_id,
      expired_at    = EXCLUDED.expired_at
WHERE locks.expired_at < NOW() OR locks.holder_txn_id = $2
RETURNING lock_id, holder_txn_id, expired_at;`

	start := time.Now()
	rows, err := r.qc.QueryContext(ctx, query, lockID, txnID, r.renewInterval)
	r.operationDuration.WithLabelValues(virtualStorage, "trylock").Observe(time.Since(start).Seconds())
	if err != nil {
		r.lockAcquiredTotal.WithLabelValues(virtualStorage, "error").Inc()
		deregister()
		return tryLockResult{
			Acquired: false,
			Err:      fmt.Errorf("acquire repo reference write lock: %s, %w", lockID, err),
		}
	}
	defer func() {
		if err := rows.Close(); err != nil {
			r.logger.WithError(err).Error("close rows")
		}
	}()
	if rows.Next() {
		_, alreadyHeld := r.lockAcquiredAt.LoadOrStore(lockID, time.Now())
		if !alreadyHeld {
			r.locksHeld.WithLabelValues(virtualStorage).Inc()
			r.lockAcquiredTotal.WithLabelValues(virtualStorage, "new_acquisition").Inc()
		}
		deregister()
		unlockFn := func() error {
			return r.unlock(virtualStorage, relativePath, txnID)
		}
		renewFn := func() error {
			return r.renew(ctx, virtualStorage, relativePath, txnID)
		}
		return tryLockResult{
			Acquired: true,
			Unlock:   unlockFn,
			Renew:    renewFn,
		}
	}
	if err := rows.Err(); err != nil {
		deregister()
		return tryLockResult{
			Acquired: false,
			Err:      fmt.Errorf("acquire repo reference write lock: %s, %w", lockID, err),
		}
	}

	r.lockAcquiredTotal.WithLabelValues(virtualStorage, "contended").Inc()
	return tryLockResult{
		Acquired:       false,
		NotificationCh: notificationCh,
		Deregister:     deregister,
	}
}

// unlock releases the write lock held by txnID. It is a no-op if the lock is not currently held by txnID
// (e.g. it expired and was taken over by another transaction).
func (r *RepoReferenceWriteLockManager) unlock(virtualStorage string, relativePath string, txnID uint64) error {
	// unlock has its own context, because the lock must be released regardless of
	// the request's lifecycle. Don't use the caller's ctx because if the caller's request
	// context is cancelled (e.g. client disconnected, deadline exceeded), the request is
	// done but the lock row still exists in the database.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	query := `
DELETE FROM repository_reference_write_locks
WHERE lock_id = $1 AND holder_txn_id = $2;`
	lockID := repoLockID(virtualStorage, relativePath)
	start := time.Now()
	_, err := r.qc.ExecContext(ctx, query, lockID, txnID)
	r.operationDuration.WithLabelValues(virtualStorage, "unlock").Observe(time.Since(start).Seconds())

	if err != nil {
		return fmt.Errorf("release repo reference write lock: %s, %w", lockID, err)
	}

	if acquiredAt, ok := r.lockAcquiredAt.LoadAndDelete(lockID); ok {
		r.locksHeld.WithLabelValues(virtualStorage).Dec()
		r.lockHoldDuration.WithLabelValues(virtualStorage).Observe(time.Since(acquiredAt.(time.Time)).Seconds())
	}
	return nil
}

// renew extends the expiry of the write lock held by txnID by another renewInterval.
// It returns an error if the lock is not currently held by txnID.
func (r *RepoReferenceWriteLockManager) renew(ctx context.Context, virtualStorage string, relativePath string, txnID uint64) error {
	query := `
UPDATE repository_reference_write_locks as locks
SET  expired_at = (NOW() + $3::interval)
WHERE locks.lock_id = $1 AND holder_txn_id = $2
RETURNING expired_at;
`
	lockID := repoLockID(virtualStorage, relativePath)
	start := time.Now()
	rows, err := r.qc.QueryContext(ctx, query, lockID, txnID, r.renewInterval)
	r.operationDuration.WithLabelValues(virtualStorage, "renew").Observe(time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("renew repo reference write lock (executing query): %s, %w", lockID, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			r.logger.WithError(err).Error("close rows")
		}
	}()
	if rows.Next() {
		return nil
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("renew repo reference write lock (iterating rows): %s, %w", lockID, err)
	}
	return fmt.Errorf("renew repo reference write lock (no rows): %s", lockID)
}

func cleanUpExpiredRepoRefWriteLocks(ctx context.Context, qc glsql.Querier, logger log.Logger) {
	query := `
DELETE FROM repository_reference_write_locks
WHERE expired_at < NOW();`
	result, err := qc.ExecContext(ctx, query)
	if err != nil {
		logger.WithError(err).Error("cleanup expired lock")
		return
	}
	n, err := result.RowsAffected()
	if err != nil {
		logger.WithError(err).Error("cleanup expired lock")
		return
	}
	logger.WithField("count", n).Info("clean up expired repository reference write locks")
}

// repoLockID constructs the lock ID used in the write lock table.
func repoLockID(virtualStorage, relativePath string) LockID {
	return LockID(virtualStorage + "|" + relativePath)
}
