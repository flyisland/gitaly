# Praefect Serialized Writes

Praefect coordinates writes to a repository across multiple Gitaly nodes using a
voting protocol driven by the Git `reference-transaction` hook. This document
describes why writes must be serialized at the repository level, how the
serialization is implemented, what upstream Git change it depends on, and how to
turn the feature on.

## Why Serialize Writes

The voting protocol relies on Git's `reference-transaction` hook to vote on a
ref update at each phase of the transaction. The phases Git historically
exposed are:

- `prepared` — references have been locked on disk
- `committed` — updates have been persisted
- `aborted` — the transaction was rolled back

Praefect collects votes from all Gitaly nodes participating in the transaction
and only allows the write to proceed when quorum is reached.

The problem is that at the `prepared` phase, **on-disk ref locks have already
been taken**. With two concurrent transactions T1 and T2 updating overlapping
refs A and B:

1. Node 1: T1 locks A, T2 locks B
1. Node 2: T2 locks A, T1 locks B

Each transaction is now waiting for the other to release a lock, and neither
can release until quorum is reached for its own lock. The cluster is
deadlocked.

The fix is to serialize transactions **before** any node has taken an on-disk
ref lock. If only one transaction per repository can advance past this point at
a time, lock acquisition order is globally consistent and the deadlock above
cannot occur.

## Requirements

Serialized writes require two upstream changes: one in Git, one in Gitaly's
protocol. That together expose a phase running before any locks are taken.

### Git: the "preparing" phase

The serialization point is a new `reference-transaction` hook phase called
`preparing`, introduced upstream in:

> `60d8c1e97d62c27ef60db0bc3d5deadd6dfdb98d`
> *refs: add 'preparing' phase to the reference-transaction hook*
>
> First released in **Git v2.54.0**.

`preparing` fires **before** Git stages any ref update. This is the only phase
at which Praefect can take a global lock without already racing with the
on-disk locking machinery.

Gitaly's bundled Git pin is set in `Makefile`:

```makefile
GIT_VERSION_PREV   ?= ...   # default bundled Git
GIT_VERSION_MASTER ?= ...   # Git's master branch tip
```

For serialized writes to work on every code path, `GIT_VERSION_PREV` must point
to a revision that contains `60d8c1e9…`.

### Praefect: `PREPARING_PHASE`

The matching `VoteTransactionRequest_Phase` enum value `PREPARING_PHASE = 4`
was added to `proto/transaction.proto`. The Gitaly-side reference-transaction
hook handler maps the Git `preparing` state to `voting.Preparing`, and the
matching write paths in Gitaly (`update_with_hooks.go`, `repoutil/create.go`,
`repoutil/custom_hooks.go`, `repoutil/remove.go`, `objectpool/disconnect.go`,
`objectpool/fetch.go`, `transaction/voting.go::CommitLockedFile`,
`service/operations/squash.go`, `service/ref/delete_refs.go`) cast a
`voting.Preparing` vote before any ref or file lock is acquired.

## How It Works

`internal/praefect/transactions/manager.go` owns the lock lifecycle. A
repository-scoped write lock backed by PostgreSQL is acquired, renewed, and
released according to the phase of each `VoteTransaction` call:

| Phase            | Action                                                  |
|------------------|---------------------------------------------------------|
| `PREPARING_PHASE`| `WriteLockManager.Lock` — block until exclusive access  |
| `PREPARED_PHASE` | `lock.Renew` — keep the lock alive across phases        |
| `COMMITTED_PHASE`| `lock.Unlock` — release once the write is durable       |

If any phase fails before commit, the deferred
`unlockRepoForTransaction` releases the lock so the next transaction can
proceed. Transactions that are explicitly stopped or canceled
(`StopTransaction`, `cancelTransaction`) also release the lock to avoid
stranding it in PostgreSQL.

The `WriteLockManager` interface lives in
`internal/praefect/datastore/lock_manager.go`. The production implementation is
PostgreSQL-backed; a `NoopWriteLockManager` is provided for callers that don't
need serialization.

### Why PostgreSQL and not an in-process mutex

A `sync.Mutex` (or any per-process construct) only serializes within a single
Praefect. A production cluster typically runs **multiple Praefect instances**
behind a load balancer for availability and horizontal scaling, and votes for
the same transaction can arrive at different instances. With an in-process
lock, two Praefect instances would each happily believe they hold the lock for
the same repository at the same time, which would defeat serialization
entirely.

The lock therefore has to live in a store all Praefect instances share. The
existing PostgreSQL database that Praefect already uses for its metadata is a
natural fit: it gives us cluster-wide visibility, transactional semantics, and
a notification channel for lock release without introducing a new dependency.

### Flow for one write

1. Client calls a write RPC against Praefect.
1. Praefect forwards the RPC to each Gitaly node in the replica set.
1. Each Gitaly node, in `UpdateReference` (or the equivalent write path),
   casts a `Preparing` vote **before** locking refs locally. The hook RPC
   reaches Praefect, which calls `lockRepoForTransaction(PREPARING_PHASE)`:
   `WriteLockManager.Lock` blocks until the per-repository lock is free.
1. Both nodes return from `Preparing` together (quorum reached).
1. Each Gitaly node locks refs on disk and votes `Prepared`. Praefect renews
   the lock.
1. Each Gitaly node writes the update and votes `Committed`. Praefect releases
   the lock.

Because step 3 is exclusive per repository, no two transactions can be in
steps 4-6 at the same time for the same repo, so on-disk locks are always
taken in a consistent order.

## Enabling It

Serialized writes are gated by the
`praefect_serialized_write` feature flag. It defaults to **disabled** so
existing deployments are not affected.

Turn it on the same way as any other Gitaly feature flag:

- **Via gRPC metadata**: pass the header
  `gitaly-feature-praefect-serialized-write: true` on outgoing RPCs.
- **In Gitaly's own tests**: the test helper at
  `internal/testhelper/testhelper.go` enables the flag on the test context with
  `featureflag.ContextWithFeatureFlag(ctx, featureflag.PraefectSerializedWrite, true)`,
  so every test that derives its context from the helper runs with the feature
  on.
- **In GitLab Rails tests**: GitLab Rails spawns Gitaly and Praefect with
  `GITALY_TESTING_ENABLE_ALL_FEATURE_FLAGS=true` (see
  `spec/support/helpers/gitaly_setup.rb`), which forces every feature flag,
  including this one, to evaluate as enabled regardless of context.
- **In GitLab Rails for production rollout**: register the flag in
  `config/feature_flags/` once it is ready to ship.

## Limitations

Some Gitaly write paths run `git update-ref` or `git fetch` directly. For
example `FetchSourceBranch`, `Replicate`, `ResolveConflicts`,
`UserCommitFiles`, `UserRevert`, `RebaseToRef`, `MergeToRef`, and anything
using `localrepo.UpdateRef` / `localrepo.FetchInternal` /
`setDefaultBranchWithUpdateRef`. On these paths, Git itself drives the
`reference-transaction` hook rather than Gitaly's manual driver in
`update_with_hooks.go`.

On a bundled Git that contains `60d8c1e9…`, Git emits the `preparing` phase
for these paths too and the serialization is complete.

On a bundled Git **without** that commit, Git only emits `prepared` and
`committed`. Praefect tolerates this by skipping the lock check when no
`PREPARING_PHASE` was recorded for the transaction. The transaction completes
normally, but **serialization is dormant for that write**. The deadlock
described above remains possible on those paths until `GIT_VERSION_PREV` is
bumped past `60d8c1e9…`. Once it is, the tolerance branch becomes unreachable
and can be removed alongside a startup-time minimum Git version check.

## References

- Issue: <https://gitlab.com/gitlab-org/gitaly/-/issues/7059>
- Upstream Git patch series:
  <https://lore.kernel.org/git/20260317023624.43070-1-eric.peijian@gmail.com/>
- Feature flag definition:
  `internal/featureflag/ff_praefect_serialized_write.go`
- Lock manager: `internal/praefect/datastore/lock_manager.go`
- Phase handling: `internal/praefect/transactions/manager.go`
