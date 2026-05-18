package praefect

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func runPraefectServerAndTxMgrAndLockMgr(tb testing.TB, ctx context.Context, lockMgr datastore.WriteLockManager) (*grpc.ClientConn, *transactions.Manager, testhelper.Cleanup) {
	conf := testConfig(1)
	txMgr := transactions.NewManager(conf, testhelper.SharedLogger(tb), lockMgr)
	cc, _, cleanup := RunPraefectServer(tb, ctx, conf, BuildOptions{
		WithTxMgr:   txMgr,
		WithNodeMgr: nullNodeMgr{}, // to suppress node address issues
	})
	return cc, txMgr, cleanup
}

// mockWriteWithRefTxnHook mocks the process of how a write request would invoke each phase of the reference transaction hook
func mockWriteWithRefTxnHook(ctx context.Context, client gitalypb.RefTransactionClient,
	node string, voteHash []byte, txnID uint64, storageName string, relativePath string, probingFn func() error,
) error {
	phases := []gitalypb.VoteTransactionRequest_Phase{
		gitalypb.VoteTransactionRequest_PREPARING_PHASE,
		gitalypb.VoteTransactionRequest_PREPARED_PHASE,
		gitalypb.VoteTransactionRequest_COMMITTED_PHASE,
	}

	for _, phase := range phases {
		_, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId: txnID,
			Repository: &gitalypb.Repository{
				StorageName:  storageName,
				RelativePath: relativePath,
			},
			Node:                 node,
			ReferenceUpdatesHash: voteHash,
			Phase:                phase,
		})
		if err != nil {
			return err
		}
		if phase != gitalypb.VoteTransactionRequest_COMMITTED_PHASE {
			if err := probingFn(); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestPraefectWriteSerialization verifies that the write lock manager serializes
// concurrent write requests to the same repository.
//
// A real write RPC (e.g. UserCreateBranch) cannot be used here because mock
// Gitaly nodes do not invoke the reference transaction hook, so the lock manager
// is never called. Instead, each "write" is simulated by calling VoteTransaction
// directly for the three phases (PREPARING → PREPARED → COMMITTED). The lock is
// acquired inside the transaction manager at PREPARING and released after COMMITTED,
// matching the real hook lifecycle.
//
// Serialization is verified with a probing function that runs between phases:
// it increments a per-request counter (inFlightVoters[i]) while the request is
// inside the critical section, then checks that no other request has a non-zero
// counter. A sleep inside the probing window (varying by request to avoid
// lock-step wakeups) keeps the counter elevated long enough for concurrent
// requests to observe it.
//
// The noop-lock test case confirms the probe is effective: without serialization,
// overlapping requests are reliably detected. The real-lock cases then assert
// that no overlap occurs.
func TestPraefectWriteSerialization(t *testing.T) {
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	writeRequestTotal := 10
	requestOverlapErr := fmt.Errorf("request overlap")
	storageName := "mock"
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	for _, tc := range []struct {
		desc             string
		repoRelativePath string // Different for each test case to avoid lock table row conflicting
		voters           []transactions.Voter
		threshold        uint
		lockMgr          datastore.WriteLockManager
		expectedErr      error
	}{
		{
			desc:             "praefect with 3 nodes noop lock manager sees request overlapping",
			repoRelativePath: "noop/lock/mgr/i_am_noop.git",
			voters: []transactions.Voter{
				{Name: "primary", Votes: 1},
				{Name: "secondary-1", Votes: 1},
				{Name: "secondary-2", Votes: 1},
			},
			threshold:   3,
			lockMgr:     &datastore.NoopWriteLockManager{},
			expectedErr: requestOverlapErr,
		},
		{
			desc:             "praefect with 3 nodes write lock manager serialize requests",
			repoRelativePath: "3nodes/real/lock/mgr/i_am_real.git",
			voters: []transactions.Voter{
				{Name: "primary", Votes: 1},
				{Name: "secondary-1", Votes: 1},
				{Name: "secondary-2", Votes: 1},
			},
			threshold: 3,
			lockMgr:   repoWriteLockMgr,
		},
		{
			desc:             "praefect with 5 nodes write lock manager serialize requests",
			repoRelativePath: "5nodes/real/lock/mgr/i_am_real.git",
			voters: []transactions.Voter{
				{Name: "primary", Votes: 1},
				{Name: "secondary-1", Votes: 1},
				{Name: "secondary-2", Votes: 1},
				{Name: "secondary-3", Votes: 1},
				{Name: "secondary-4", Votes: 1},
			},
			threshold: 5,
			lockMgr:   repoWriteLockMgr,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// inFlightVoters[x] > 0 while request x's voters are in the probing window.
			// The probing window is defined by inprobingFn inside the mock write call.
			// Multiple voters for the same request run concurrently, so the count can exceed 1.
			inFlightVoters := make([]int32, writeRequestTotal)

			transactionIDs := make([]uint64, writeRequestTotal)
			hash := sha1.Sum([]byte(tc.desc))
			cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, tc.lockMgr)
			defer cleanup()
			client := gitalypb.NewRefTransactionClient(cc)

			// Register a transaction for each request
			for requestID := range writeRequestTotal {
				transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, tc.voters, tc.threshold)
				require.NoError(t, err)
				require.NotNil(t, transaction)
				require.NotZero(t, transaction.ID())
				transactionIDs[requestID] = transaction.ID()
				defer func() {
					require.NoError(t, cancelTransaction())
				}()
			}

			eg, ctx := errgroup.WithContext(ctx)
			for requestID := range writeRequestTotal {
				for _, v := range tc.voters {
					eg.Go(func() error {
						err := mockWriteWithRefTxnHook(ctx, client, v.Name, hash[:],
							transactionIDs[requestID], storageName, tc.repoRelativePath,
							func() error {
								atomic.AddInt32(&inFlightVoters[requestID], 1)
								defer atomic.AddInt32(&inFlightVoters[requestID], -1)

								for other := 0; other < writeRequestTotal; other++ {
									if other == requestID {
										// Sleep with two different durations so that even- and odd-numbered
										// requests are unlikely to wake up in sync. If all requests slept
										// the same amount they could all enter and leave the probing window
										// together, making the overlap check a no-op.
										// Prime values (37ms, 79ms) avoid harmonic alignment across the
										// 10 concurrent requests, maximising the chance that at least two
										// requests are in the probing window at the same time.
										// The noop-lock test case relies on this overlap being detected.
										if requestID%2 == 0 {
											time.Sleep(79 * time.Millisecond)
										} else {
											time.Sleep(37 * time.Millisecond)
										}
										continue
									}

									// Assert all if all the other request have zero in-flight
									if count := atomic.LoadInt32(&inFlightVoters[other]); count > 0 {
										return fmt.Errorf("requestID %d is executing while request %d is also active: %w",
											requestID, other, requestOverlapErr)
									}
								}
								return nil
							})
						return err
					})
				}
			}
			err := eg.Wait()
			if tc.expectedErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestPraefectWriteSerialization_HangAfterPrepared(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	storageName := "mock"
	relativePath := "hang/after/prepared.git"
	voters := []transactions.Voter{{Name: "primary", Votes: 1}}
	threshold := uint(1)

	cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, repoWriteLockMgr)
	defer cleanup()
	client := gitalypb.NewRefTransactionClient(cc)

	// Txn1: PREPARING + PREPARED, then "hang" (no COMMITTED)
	txn1, _, err := txMgr.RegisterTransaction(ctx, voters, threshold)
	require.NoError(t, err)
	hash1 := sha1.Sum([]byte("txn1"))

	for _, phase := range []gitalypb.VoteTransactionRequest_Phase{
		gitalypb.VoteTransactionRequest_PREPARING_PHASE,
		gitalypb.VoteTransactionRequest_PREPARED_PHASE,
	} {
		_, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        txn1.ID(),
			Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
			Node:                 voters[0].Name,
			ReferenceUpdatesHash: hash1[:],
			Phase:                phase,
		})
		require.NoError(t, err)
	}
	// Intentionally skip COMMITTED phase to simulate a stuck client.

	// Simulate lock expiry by back-dating the row
	_, err = db.ExecContext(ctx, `
          UPDATE repository_reference_write_locks
          SET expired_at = NOW() - INTERVAL '1 second'
          WHERE lock_id = $1`, storageName+"|"+relativePath)
	require.NoError(t, err)

	// Txn2: PREPARING should succeed (steals the expired lock)
	txn2, cancel2, err := txMgr.RegisterTransaction(ctx, voters, threshold)
	require.NoError(t, err)
	defer func() { require.NoError(t, cancel2()) }()
	hash2 := sha1.Sum([]byte("txn2"))

	_, err = client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn2.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters[0].Name,
		ReferenceUpdatesHash: hash2[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err)
}

func TestPraefectWriteSerialization_ContextCancellationReleasesLock(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	storageName := "mock"
	relativePath := "ctx/cancel/releases.git"
	lockID := storageName + "|" + relativePath

	cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, repoWriteLockMgr)
	defer cleanup()
	client := gitalypb.NewRefTransactionClient(cc)

	// Txn1: 3 voters, threshold 3. One voter casts at PREPARING; the call blocks
	// waiting for quorum while holding the lock.
	voters1 := []transactions.Voter{
		{Name: "primary", Votes: 1},
		{Name: "secondary-1", Votes: 1},
		{Name: "secondary-2", Votes: 1},
	}
	txn1, cancelTxn1Reg, err := txMgr.RegisterTransaction(ctx, voters1, 3)
	require.NoError(t, err)
	defer func() { _ = cancelTxn1Reg() }()

	hash1 := sha1.Sum([]byte("txn1"))
	txn1Ctx, cancelTxn1Ctx := context.WithCancel(ctx)
	defer cancelTxn1Ctx()

	txn1ErrCh := make(chan error, 1)
	go func() {
		// Register with threshold=3 but cast only one vote at PREPARING. transaction.vote()
		// blocks in collectVotes() waiting for quorum, keeping the lock held; the gRPC ctx
		// cancel below should propagate through and trigger the deferred unlock.
		_, err := client.VoteTransaction(txn1Ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        txn1.ID(),
			Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
			Node:                 voters1[0].Name,
			ReferenceUpdatesHash: hash1[:],
			Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
		})
		txn1ErrCh <- err
	}()

	// Wait for the lock to be held (row visible in the DB).
	require.Eventually(t, func() bool {
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
			lockID,
		).Scan(&count))
		return count > 0
	}, 30*time.Second, 50*time.Millisecond, "txn1 did not acquire the lock")

	// Cancel the gRPC call's context. The server-side handler should return with a
	// ctx error, the deferred unlockRepoForTransaction should fire, and the lock
	// row should be removed.
	cancelTxn1Ctx()

	select {
	case err := <-txn1ErrCh:
		require.Error(t, err, "txn1 should return an error due to context cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("txn1 did not return after ctx cancellation")
	}

	// The lock row is gone.
	require.Eventually(t, func() bool {
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
			lockID,
		).Scan(&count))
		return count == 0
	}, 5*time.Second, 50*time.Millisecond, "lock was not released after ctx cancellation")

	// A fresh single-voter transaction acquires immediately.
	voters2 := []transactions.Voter{{Name: "primary", Votes: 1}}
	txn2, cancelTxn2Reg, err := txMgr.RegisterTransaction(ctx, voters2, 1)
	require.NoError(t, err)
	defer func() { _ = cancelTxn2Reg() }()

	hash2 := sha1.Sum([]byte("txn2"))
	txn2Ctx, cancelTxn2Ctx := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTxn2Ctx()

	_, err = client.VoteTransaction(txn2Ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn2.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters2[0].Name,
		ReferenceUpdatesHash: hash2[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err, "txn2 should acquire the lock immediately because txn1's lock was released")
}

func TestPraefectWriteSerialization_VoteFailureReleasesLock(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	storageName := "mock"
	relativePath := "vote/failure/releases.git"
	lockID := storageName + "|" + relativePath

	cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, repoWriteLockMgr)
	defer cleanup()
	client := gitalypb.NewRefTransactionClient(cc)

	// Txn1: 3 voters, threshold 3, each casts a DIFFERENT hash so no quorum is reachable.
	// Each voter's VoteTransaction returns ErrTransactionFailed; the deferred unlock fires
	// and the lock row is removed.
	voters := []transactions.Voter{
		{Name: "primary", Votes: 1},
		{Name: "secondary-1", Votes: 1},
		{Name: "secondary-2", Votes: 1},
	}
	txn1, cancelTxn1Reg, err := txMgr.RegisterTransaction(ctx, voters, 3)
	require.NoError(t, err)
	defer func() { _ = cancelTxn1Reg() }()

	// The gRPC handler converts ErrTransactionFailed into a successful response
	// with State=ABORT rather than a gRPC error (see
	// internal/praefect/service/transaction/server.go), so we check response.State
	// instead of err.
	var wg sync.WaitGroup
	voteResps := make([]*gitalypb.VoteTransactionResponse, len(voters))
	voteErrs := make([]error, len(voters))
	for i, v := range voters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hash := sha1.Sum([]byte(fmt.Sprintf("voter-%d-disagrees", i)))
			resp, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
				TransactionId:        txn1.ID(),
				Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
				Node:                 v.Name,
				ReferenceUpdatesHash: hash[:],
				Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
			})
			voteResps[i] = resp
			voteErrs[i] = err
		}()
	}
	wg.Wait()

	for i, err := range voteErrs {
		require.NoErrorf(t, err, "voter %d gRPC call should not error", i)
		require.Equalf(t, gitalypb.VoteTransactionResponse_ABORT, voteResps[i].GetState(),
			"voter %d should receive ABORT state because no value reaches threshold", i)
	}

	// The deferred unlock should have removed the row.
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
		lockID,
	).Scan(&count))
	require.Zero(t, count, "lock should have been released after vote failure")

	// Sanity check: a fresh single-voter transaction acquires immediately
	// (no waiting for expiry).
	voters2 := []transactions.Voter{{Name: "primary", Votes: 1}}
	txn2, cancelTxn2Reg, err := txMgr.RegisterTransaction(ctx, voters2, 1)
	require.NoError(t, err)
	defer func() { _ = cancelTxn2Reg() }()

	txn2Ctx, cancelTxn2Ctx := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTxn2Ctx()

	hash2 := sha1.Sum([]byte("txn2"))
	_, err = client.VoteTransaction(txn2Ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn2.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters2[0].Name,
		ReferenceUpdatesHash: hash2[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err, "txn2 should acquire immediately because txn1's vote failure released the lock")
}

func TestPraefectWriteSerialization_CancelTransactionReleasesLock(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	storageName := "mock"
	relativePath := "cancel/transaction/releases.git"
	lockID := storageName + "|" + relativePath

	cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, repoWriteLockMgr)
	defer cleanup()
	client := gitalypb.NewRefTransactionClient(cc)

	voters := []transactions.Voter{{Name: "primary", Votes: 1}}

	// Txn1: PREPARING succeeds; the lock is held in mgr.repoLocks.
	txn1, cancelTxn1, err := txMgr.RegisterTransaction(ctx, voters, 1)
	require.NoError(t, err)

	hash1 := sha1.Sum([]byte("txn1"))
	_, err = client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn1.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters[0].Name,
		ReferenceUpdatesHash: hash1[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err)

	// Confirm the lock is currently held.
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
		lockID,
	).Scan(&count))
	require.Equal(t, 1, count, "lock should be held after PREPARING")

	// Invoke the cancel closure: this MR makes it release the lock.
	require.NoError(t, cancelTxn1())

	// The lock row should now be gone.
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
		lockID,
	).Scan(&count))
	require.Zero(t, count, "cancelTransaction should have released the lock")

	// Sanity: txn2 acquires immediately, no expiry wait.
	txn2, cancelTxn2, err := txMgr.RegisterTransaction(ctx, voters, 1)
	require.NoError(t, err)
	defer func() { _ = cancelTxn2() }()

	txn2Ctx, cancelTxn2Ctx := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTxn2Ctx()

	hash2 := sha1.Sum([]byte("txn2"))
	_, err = client.VoteTransaction(txn2Ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn2.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters[0].Name,
		ReferenceUpdatesHash: hash2[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err, "txn2 should acquire immediately because cancelTransaction released txn1's lock")
}

func TestPraefectWriteSerialization_StopTransactionReleasesLock(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx := testhelper.Context(t)
	logger := testhelper.SharedLogger(t)
	repoWriteLockMgr := datastore.NewRepoReferenceWriteLockManager(ctx, db, testdb.GetConfig(t, db.Name), logger)

	storageName := "mock"
	relativePath := "stop/transaction/releases.git"
	lockID := storageName + "|" + relativePath

	cc, txMgr, cleanup := runPraefectServerAndTxMgrAndLockMgr(t, ctx, repoWriteLockMgr)
	defer cleanup()
	client := gitalypb.NewRefTransactionClient(cc)

	voters := []transactions.Voter{{Name: "primary", Votes: 1}}

	// Txn1: PREPARING succeeds; the lock is held in mgr.repoLocks.
	txn1, cancelTxn1Reg, err := txMgr.RegisterTransaction(ctx, voters, 1)
	require.NoError(t, err)
	// StopTransaction doesn't delete the txn from the in-memory map, so we still
	// need to call the cancel closure at the end to clean it up. Ignore its error
	// because transaction.cancel() runs against an already-stopped transaction.
	defer func() { _ = cancelTxn1Reg() }()

	hash1 := sha1.Sum([]byte("txn1"))
	_, err = client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn1.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters[0].Name,
		ReferenceUpdatesHash: hash1[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
		lockID,
	).Scan(&count))
	require.Equal(t, 1, count, "lock should be held after PREPARING")

	// Invoke StopTransaction: this MR makes it release the lock. ---
	require.NoError(t, txMgr.StopTransaction(ctx, txn1.ID()))

	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM repository_reference_write_locks WHERE lock_id = $1",
		lockID,
	).Scan(&count))
	require.Zero(t, count, "StopTransaction should have released the lock")

	// Sanity: txn2 acquires immediately. ---
	txn2, cancelTxn2Reg, err := txMgr.RegisterTransaction(ctx, voters, 1)
	require.NoError(t, err)
	defer func() { _ = cancelTxn2Reg() }()

	txn2Ctx, cancelTxn2Ctx := context.WithTimeout(ctx, 2*time.Second)
	defer cancelTxn2Ctx()

	hash2 := sha1.Sum([]byte("txn2"))
	_, err = client.VoteTransaction(txn2Ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        txn2.ID(),
		Repository:           &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath},
		Node:                 voters[0].Name,
		ReferenceUpdatesHash: hash2[:],
		Phase:                gitalypb.VoteTransactionRequest_PREPARING_PHASE,
	})
	require.NoError(t, err, "txn2 should acquire immediately because StopTransaction released txn1's lock")
}
