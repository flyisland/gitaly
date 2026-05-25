package transactions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
var ErrNotFound = errors.New("transaction not found")

// Manager handles reference transactions for Praefect. It is required in order
// for Praefect to handle transactions directly instead of having to reach out
// to reference transaction RPCs.
type Manager struct {
	idSequence            uint64
	lock                  sync.Mutex
	logger                log.Logger
	transactions          map[uint64]*transaction
	repoLocks             sync.Map
	counterMetric         *prometheus.CounterVec
	delayMetric           *prometheus.HistogramVec
	subtransactionsMetric prometheus.Histogram
	repoWriteLockMgr      datastore.WriteLockManager
}

// NewManager creates a new transactions Manager.
func NewManager(cfg config.Config, logger log.Logger, repoWriteLockMgr datastore.WriteLockManager) *Manager {
	return &Manager{
		logger:           logger.WithField("component", "transactions.Manager"),
		transactions:     make(map[uint64]*transaction),
		repoWriteLockMgr: repoWriteLockMgr,
		counterMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gitaly",
				Subsystem: "praefect",
				Name:      "transactions_total",
				Help:      "Total number of transaction actions",
			},
			[]string{"action"},
		),
		delayMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "gitaly",
				Subsystem: "praefect",
				Name:      "transactions_delay_seconds",
				Help:      "Delay between casting a vote and reaching quorum",
				Buckets:   cfg.Prometheus.GRPCLatencyBuckets,
			},
			[]string{"action"},
		),
		subtransactionsMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_praefect_subtransactions_per_transaction_total",
				Help:    "The number of subtransactions created for a single registered transaction",
				Buckets: []float64{0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0},
			},
		),
	}
}

//nolint:revive // This is unintentionally missing documentation.
func (mgr *Manager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(mgr, descs)
}

//nolint:revive // This is unintentionally missing documentation.
func (mgr *Manager) Collect(metrics chan<- prometheus.Metric) {
	mgr.counterMetric.Collect(metrics)
	mgr.delayMetric.Collect(metrics)
	mgr.subtransactionsMetric.Collect(metrics)
}

// CancelFunc is the transaction cancellation function returned by
// `RegisterTransaction`. Calling it will cause the transaction to be removed
// from the transaction manager.
type CancelFunc func() error

// RegisterTransaction registers a new reference transaction for a set of nodes
// taking part in the transaction. `threshold` is the threshold at which an
// election will succeed. It needs to be in the range `weight(voters)/2 <
// threshold <= weight(voters) to avoid indecidable votes.
func (mgr *Manager) RegisterTransaction(ctx context.Context, voters []Voter, threshold uint) (Transaction, CancelFunc, error) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	transactionID := atomic.AddUint64(&mgr.idSequence, 1)

	transaction, err := newTransaction(transactionID, voters, threshold)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := mgr.transactions[transactionID]; ok {
		return nil, nil, errors.New("transaction exists already")
	}
	mgr.transactions[transactionID] = transaction

	mgr.logger.WithFields(log.Fields{
		"transaction.id":     transactionID,
		"transaction.voters": voters,
	}).DebugContext(ctx, "RegisterTransaction")

	mgr.counterMetric.WithLabelValues("registered").Add(float64(len(voters)))

	return transaction, func() error {
		return mgr.cancelTransaction(ctx, transaction)
	}, nil
}

func (mgr *Manager) cancelTransaction(ctx context.Context, transaction *transaction) error {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	delete(mgr.transactions, transaction.ID())
	// Release while holding mgr.lock so a concurrent voteTransaction can't
	// re-acquire the repo lock between delete and cancel.
	mgr.releaseRepoLock(transaction.ID())
	transaction.cancel()
	mgr.subtransactionsMetric.Observe(float64(transaction.CountSubtransactions()))

	var committed uint64
	state, err := transaction.State()
	if err != nil {
		return err
	}

	for _, result := range state {
		if result == VoteCommitted {
			committed++
		}
	}

	mgr.logger.WithFields(log.Fields{
		"transaction.id":              transaction.ID(),
		"transaction.committed":       fmt.Sprintf("%d/%d", committed, len(state)),
		"transaction.subtransactions": transaction.CountSubtransactions(),
	}).InfoContext(ctx, "transaction completed")

	return nil
}

func (mgr *Manager) voteTransaction(ctx context.Context, transactionID uint64, storageName, repoRelativePath, node string,
	phase gitalypb.VoteTransactionRequest_Phase, vote voting.Vote,
) (returnedErr error) {
	mgr.lock.Lock()
	transaction, ok := mgr.transactions[transactionID]
	mgr.lock.Unlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrNotFound, transactionID)
	}

	err := mgr.lockRepoForTransaction(ctx, transactionID, storageName, repoRelativePath, phase)
	if err != nil {
		return fmt.Errorf("lock transaction %d: %w", transactionID, err)
	}
	defer func() {
		mgr.unlockRepoForTransaction(ctx, transactionID, returnedErr, phase)
	}()
	if err := transaction.vote(ctx, node, vote); err != nil {
		return err
	}

	return nil
}

// VoteTransaction is called by a client who's casting a vote on a reference
// transaction. It waits until quorum was reached on the given transaction.
func (mgr *Manager) VoteTransaction(ctx context.Context, transactionID uint64, storageName, repoRelativePath, node string, phase gitalypb.VoteTransactionRequest_Phase, vote voting.Vote) error {
	start := time.Now()
	defer func() {
		delay := time.Since(start)
		mgr.delayMetric.WithLabelValues("vote").Observe(delay.Seconds())
	}()

	logger := mgr.logger.WithFields(log.Fields{
		"transaction.id":    transactionID,
		"transaction.voter": node,
		"transaction.hash":  vote.String(),
		"transaction.phase": phase,
	})

	mgr.counterMetric.WithLabelValues("started").Inc()
	logger.DebugContext(ctx, "VoteTransaction")

	if err := mgr.voteTransaction(ctx, transactionID, storageName, repoRelativePath, node, phase, vote); err != nil {
		var counterLabel string

		if errors.Is(err, ErrTransactionStopped) {
			counterLabel = "stopped"
			// Stopped transactions indicate a graceful
			// termination, so we should not log an error here.
		} else if errors.Is(err, ErrTransactionFailed) {
			counterLabel = "failed"
			logger.WithError(err).ErrorContext(ctx, "VoteTransaction: did not reach quorum")
		} else if errors.Is(err, ErrTransactionCanceled) {
			counterLabel = "canceled"
			logger.WithError(err).ErrorContext(ctx, "VoteTransaction: transaction was canceled")
		} else {
			counterLabel = "invalid"
			logger.WithError(err).ErrorContext(ctx, "VoteTransaction: failure")
		}

		mgr.counterMetric.WithLabelValues(counterLabel).Inc()

		return err
	}

	logger.InfoContext(ctx, "VoteTransaction: transaction committed")
	mgr.counterMetric.WithLabelValues("committed").Inc()

	return nil
}

// StopTransaction will gracefully stop a transaction.
func (mgr *Manager) StopTransaction(ctx context.Context, transactionID uint64) error {
	mgr.lock.Lock()
	transaction, ok := mgr.transactions[transactionID]
	if ok {
		// Release while holding mgr.lock so a concurrent voteTransaction
		// can't acquire a fresh repo lock between this release and stop.
		mgr.releaseRepoLock(transactionID)
	}
	mgr.lock.Unlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrNotFound, transactionID)
	}
	if err := transaction.stop(); err != nil {
		return err
	}

	mgr.logger.WithFields(log.Fields{
		"transaction.id": transactionID,
	}).DebugContext(ctx, "VoteTransaction: transaction stopped")
	mgr.counterMetric.WithLabelValues("stopped").Inc()

	return nil
}

// CancelTransactionNodeVoter cancels the voter associated with the specified transaction
// and node. Voters are canceled when the node RPC fails and its votes can no longer count
// towards quorum.
func (mgr *Manager) CancelTransactionNodeVoter(transactionID uint64, node string) error {
	mgr.lock.Lock()
	transaction, ok := mgr.transactions[transactionID]
	mgr.lock.Unlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrNotFound, transactionID)
	}

	if err := transaction.cancelNodeVoter(node); err != nil {
		return fmt.Errorf("canceling transaction node voter: %w", err)
	}

	return nil
}

func (mgr *Manager) lockRepoForTransaction(ctx context.Context, transactionID uint64, storageName, repoRelativePath string,
	phase gitalypb.VoteTransactionRequest_Phase,
) error {
	if featureflag.PraefectSerializedWrite.IsDisabled(ctx) {
		return nil
	}

	switch phase {
	case gitalypb.VoteTransactionRequest_PREPARING_PHASE:
		lock, err := mgr.repoWriteLockMgr.Lock(ctx, storageName, repoRelativePath, transactionID)
		if err != nil {
			return fmt.Errorf("try lock: %w", err)
		}
		mgr.repoLocks.Store(transactionID, lock)

	case gitalypb.VoteTransactionRequest_PREPARED_PHASE, gitalypb.VoteTransactionRequest_COMMITTED_PHASE:
		v, ok := mgr.repoLocks.Load(transactionID)
		if !ok {
			// No Preparing was cast for this transaction. This happens when Git itself drives
			// the reference-transaction hook on a Git build that doesn't emit "preparing"
			// (i.e. pre-2.54). Skip lock work; serialization is dormant for this path
			// until GIT_VERSION_PREV is bumped past 60d8c1e9.
			mgr.logger.WithFields(log.Fields{
				"transaction.id": transactionID,
				"phase":          phase.String(),
			}).DebugContext(ctx, "skipping lock check: no preparing phase recorded")
			return nil
		}
		lock := v.(datastore.RepoLock)
		if err := lock.Renew(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *Manager) unlockRepoForTransaction(ctx context.Context, transactionID uint64, returnedErr error, phase gitalypb.VoteTransactionRequest_Phase) {
	if featureflag.PraefectSerializedWrite.IsDisabled(ctx) {
		return
	}

	// Decide whether this call releases the write lock. The lock spans all the
	// phases of one transaction and is only released when either the vote
	// failed (so callers can't advance to the next phase) or the committed
	// phase completes. Phases outside the reference transaction hook
	// lifecycle (e.g. SYNCHRONIZED_PHASE) never acquired a lock.
	var shouldUnlock bool
	switch phase {
	case gitalypb.VoteTransactionRequest_PREPARING_PHASE,
		gitalypb.VoteTransactionRequest_PREPARED_PHASE:
		shouldUnlock = returnedErr != nil
	case gitalypb.VoteTransactionRequest_COMMITTED_PHASE:
		shouldUnlock = true
	default:
		return
	}

	if !shouldUnlock {
		return
	}

	mgr.releaseRepoLockOnPhase(transactionID, phase.String())
}

func (mgr *Manager) releaseRepoLockOnPhase(transactionID uint64, phase string) {
	v, ok := mgr.repoLocks.LoadAndDelete(transactionID)
	if !ok {
		return
	}
	logFields := log.Fields{
		"transaction.id": transactionID,
	}
	if phase != "" {
		logFields["phase"] = phase
	}
	lock := v.(datastore.RepoLock)
	if err := lock.Unlock(); err != nil {
		mgr.logger.WithError(err).WithFields(logFields).Error("release repo lock")
	}
}

func (mgr *Manager) releaseRepoLock(transactionID uint64) {
	mgr.releaseRepoLockOnPhase(transactionID, "")
}
