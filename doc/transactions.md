# Transactions

Gitaly contains work-in-progress support for transactions that can be enabled by through configuration:

```toml
[transactions]
enabled = true
```

Gitaly logs the following line confirming transactions are enabled:

```json
{"level":"warning","msg":"Transactions enabled. Transactions are an experimental feature. The feature is not production ready yet and might lead to various issues including data loss.","pid":70356,"time":"2025-07-16T07:44:28.884Z"}
```

While transactions are functional and being tested in production on GitLab.com, they're not considered generally available yet. The original blueprint describing transactions in Gitaly can be found [here](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/gitaly_transaction_management), and it's worthwhile reading it before this document. This document focuses on the current state of the implementation.

Transactions provide [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) with [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control). Transactions read a stable snapshot of the latest committed state of a repository at the time the transaction began and remain isolated from concurrent changes of other transactions. Possible conflicts are detected at commit time, and lead to transactions being aborted.

While the logical properties are similar to other databases providing snapshot isolation, the implementation is non-conventional. Gitaly is tightly coupled to Git, and the transaction implementation works around shortcomings of Git. For example, Git only understands repositories stored on a file system in the `.git` directory, so snapshot isolation is implemented through indirection on the file system level. Some Git operations produce conflicting file modifications which the transaction implementation has to resolve at commit time. There's also a lot of existing code tightly coupled to Git in Gitaly, and the transaction implementation is designed to plug into the existing code transparently.

## Implementation

The transaction implementation is described below roughly in a top-down order as an RPC would flow through the stack.

### Integration

Transactions integrate largely transparently with the rest of Gitaly. The integration is done through plugging in a gRPC interceptor that wraps each RPC call into a transaction. The interceptor implementation can be found [here](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/middleware.go#L97). On a high level, the interceptor:

- Extracts the repositories the RPC is operating on from the request based on the annotations in the protobuf schemas.
- Begins a transaction against the target repository.
  - This creates a snapshot containing of the target repository, a possible additional repository in the request, and possible alternates these repositories are connected to.
- Rewrites the relative paths in the request to point the repositories in the snapshot.
  - This indirection of rewriting the request to point the snapshot of a repository is what isolates the RPC handlers from each other. The RPC handlers always operate on a snapshot of a repository, never the canonical repository.
- Calls the RPC handler with the rewritten request.
  - If the RPC returns successfully, the transaction is committed.
  - If the RPC returns an error, the transaction is aborted.

Read-only and write RPCs generally follow the same flow. There are some optimizations specific to read-only RPCs, such as snapshot sharing, which we'll cover later.

This general flow applies to majority of transactions but it's good to keep in mind there are exceptions. Non-exhaustive list of exceptions include:

- The background maintenance jobs performing periodic housekeeping are not initiated by RPCs. Transactions there are generally handled the same but they are not managed by the interceptor.
- If an RPC invokes the `post-receive` hook, the transaction initiated by the interceptor is [committed in the hook handler](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/hook/postreceive.go#L128), and a new one is began to read the new state.
  - Semantically `post-receive` hook is invoked after a write has been performed. If the hook was called before the transaction is committed, the transaction could still be aborted. In this case, we would have called the hook on changes that ultimtately were not performed.
- `FetchIntoObjectPool` runs housekeeping after the fetching into the object pool. Transcations don't support other updated in the same transaction has housekeeping, so the RPC handler [commits the transaction](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/git/objectpool/fetch.go#L126) and runs another one for the housekeeping.

### Partitioning

Gitaly divides the data it stores into partitions. Each partition is essentially its own mini-database. They have their own write-ahead log, and store one or more repositories and their associated key-value entries. Transactions always operate against a single partition, never across them. Transactional guarantees only apply within a partition, and transactions are not ordered between different partitions.

Before a transaction can be started against a repository, Gitaly needs to determine which partition the repository belongs to. This is by looking up the repository's partition assignment from the embedded key-value store mapping. The mapping stores the assigned partition ID of each relative path in the form of `(relative_path, partition_id)`. If a mapping already exists, a transaction is started against the assigned partition. If there's no existing mapping yet, the relative path will automatically be assigned into a partition.

Gitaly automatically assigns repositories to partitions when they are first accessed:

- Object pools and all repositories connected to the object pool are placed in the same partition. Repositories that are about to be connected to an object pool, such as newly created forks, are also placed in the same partition with the object pool they are about to be connected.
  - Assigning pools and their connected repositories into the same partition ensures transactions can guarantee consistency between them. If pools were in different partitions, transaction ordering could cause issues, for example updating a reference in a fork before the objects are written into the pool.
- Repositories that are not connected (nor about to be connected) to an object pool are placed in their own partitions.

Whether repository is, or is about to be connected to an object pool, is determined from multiple sources:

- If it has an `info/alternates` file, the repository is currently connected to a pool.
- If it is an RPC that contains an additional repository in the request, the repository is about to be connected.
  - For example, `CreateFork` contains the origin repository as an additional repository. We place the fork in the same partition as an objet pool might be created, and the fork and the origin connected to it.
  - `CreateObjectPool` will similarly place the object pool into the same partition as the source repository since it will generally be connected to it.

Partitioning is handled in the `StorageManager` component. This component handles routing transactions to the correct partitions, and starting and stopping partitions as needed. The partition assignments are retrieved or created [here](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition_manager.go#L331) when a transaction is began against a repository for the first time. The assignment handling is fairly crude at the moment. The assignments are created before the repository is even created, and assignments are not deleted even if the repository is deleted.

### Transaction Manager

`TransactionManager` is the component responsible for managing transactions and write-ahead log application of single partition.

A partition is started by `StorageManager` by [launching a goroutine](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition_manager.go#L555) running [`TransactionManager.Run()`](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1670).
This goroutine is the partition's single writer, and is the only one operating on the actual state of the partition. This goroutine runs a loop to [apply the write-ahead log](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1705) and [process transactions](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1711)
that are ready to commit. It also synchronizes access to the partition's state between applying the log, and creating transaction snapshots.

The component is fairly large, and difficult to describe in detail. We'll focus on the higher level pieces of the implementation below.

#### Write-ahead log

The write-ahead log of a partition is stored under `+gitaly/partitions/<xx>/<yy>/<partition_id>/wal`. The WAL directory contains the WAL entries in subdirectories named by the log sequence number the transactions were committed under, so `+gitaly/partitions/<xx>/<yy>/<partition_id>/wal/<LSN>`. The WAL entries record the file system and key-value operations that are to be performed to successfully apply a given transaction. An example WAL entry's directory structure could look like:

- ...
  - wal
    - `<LSN>`
      - MANIFEST
      - 1
      - 2
      - 3
      - 4

The MANIFEST file contains the list of operations to apply. The supported operations are listed in the file's [schema](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/proto/log.proto#L109). The numbered files after the MANIFEST are new files committed by the transaction, for example reference and object files written out by Git. The MANIFEST file will contain instructions to link these files at their destination in the storage.

If the above example logged creation of a new commit, and updating the master branch to point to it, the operations in the MANIFEST file could look like:

```plaintext
create_hard_link: {
  source_path: "1"
  destination_path: "@hashed/28/fd/28fd2da2453c290510b6feb1d6da1d88472e8924f481b79bfd011ac8ac9f1776.git/objects/pack/pack-4274682fcb6a4dbb1a59ba7dd8577402e61ccbd2.pack"
}
create_hard_link: {
  source_path: "2"
  destination_path: "@hashed/28/fd/28fd2da2453c290510b6feb1d6da1d88472e8924f481b79bfd011ac8ac9f1776.git/objects/pack/pack-4274682fcb6a4dbb1a59ba7dd8577402e61ccbd2.idx"
}
create_hard_link: {
  source_path: "3"
  destination_path: "@hashed/28/fd/28fd2da2453c290510b6feb1d6da1d88472e8924f481b79bfd011ac8ac9f1776.git/objects/pack/pack-4274682fcb6a4dbb1a59ba7dd8577402e61ccbd2.rev"
}
remove_directory_entry: {
  path: "@hashed/28/fd/28fd2da2453c290510b6feb1d6da1d88472e8924f481b79bfd011ac8ac9f1776.git/objects/refs/heads/master"
}
create_hard_link: {
  source_path: "4"
  destination_path: "@hashed/28/fd/28fd2da2453c290510b6feb1d6da1d88472e8924f481b79bfd011ac8ac9f1776.git/refs/heads/master"
}
```

All transactions are committed as a list of new files, and the operations to apply in the MANIFEST file. The operations are recorded during the transcation through [the FS interface](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storage.go#L57) on the transaction.

The log entry schema still contains some historic fields, for example the logical reference updates and housekeeping operations, that are not actually used to apply the transactions. These fields are still used by conflict checks specific to housekeeping operations.

#### Snapshots

Each transaction operates on a snapshot of the data it is accessing. The snapshot captures committed state of the partition as it was when the transaction begins. This keeps transactions isolated from concurrent changes done by other transactions. The snapshot captures both the partition's key-value state and the repositories being accessed.

Transactions are created by calling [`Begin()`](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L213). Begin sets up temporary state needed by the transaction, including the transaction's snapshot, and returns the transaction.

The LSN of the transaction's read snapshot [determined](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L257) before the read snapshot is taken. Transactions always read the latest committed state, so the read LSN is the LSN of the latest committed log entry. As the latest transaction committed to the log may not be applied yet to the partition, Begin [waits for the WAL to be applied](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L379) up to the read LSN of the transaction.
This ensures the state is up to date and consistent when creating the transaction's snapshot. Log application [waits while read snapshots](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L2354) are taken. This ensures all transactions starting at a given LSN can get a consistent read snapshot before subsequent transactions are applied.

The snapshots are created by:

- [Opening a transaction](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L380) against the key-value store.
- [Snapshotting the repositories](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L394) by copying the directory hierarchy of the accessed repositories into a temporary directory, and hard linking the files in place.

Creating repository snapshots is an expensive operations that scales by the number of files and directories in the repositories. There are some optimizations in place for this:

- The snapshot only includes the repositories that the transaction is explicitly started against rather than the all of the repositories in a partition.
- Each write transaction gets an exclusive snapshot to operate on as they need to write into the snapshots. Read-only transactions however can share snapshots if they access the same repositories at the same LSN.

Snapshots get [cleaned up](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L314) when the transaction commits or aborts. Shared snapshots are [cleaned up only once they have no active users](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/snapshot/manager.go#L237).

#### Conflict checks

As transactions operate on a snapshot, they're not aware of changes of other transactions. TransactionManager must thus at commit time verify the transactions aren't making conflicting changes. All conflict checks are done at commit time in [`processTransaction`](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1755). Conflicts can occur both at the logical level, and at a file level:

- Logical conflicts could occur for example if two transactions attempt to modify the same reference.
- File level conflicts could occur if two transactions delete different references from the `packed-refs` file. Deleting differente references is logically fine but the concurrent modifications to the same `packed-refs` file conflict.

The [file level conflict checks](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1876) prevent concurrent transactions from writing to the same files. This done by [recording the file system changes](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/conflict/fshistory/history.go#L101) each committed transaction in memory. When a transaction is being committed, its changes are compared against the recorded changes kept in memory. If another transaction changed the same path concurrently, the transaction is aborted as it could end up overriding changes of some earlier transactions.

The logical conflict checks are not as generalized as the file level conflict checks due to lacking information of some of the logical operations, and existing code in Gitaly not upholding consistency everywhere. These checks are more complex than the file checks as they're not handled in as general manner. Some of the logical checks include:

- There's [a high level check](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1763) that rejects a transaction if the target repository was concurrently created or deleted.
- TransactionManager keeps reference changes performed by transactions in memory, and [verifies the reference changes](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1783) being performed by transactions don't result in conflicts, be it concurrent modificatons of references or introduction of file-directory conflicts.
- TransactionManager checks that objects the transactions depend on either directly through references or through the dependencies of objects are still present in the repository.

Failing any of the conflict checks leads to the transaction being aborted. If no issues are detected, the transaction is [committed to the log](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L1857).

The conflict checking logic is not general enough to detect all conflicts. For example, we do not have a way to generally record the read set of a transaction, and can't prevent write skew.

## Tooling

- There are some basic transaction specific metrics dashboarded [here](https://dashboards.gitlab.net/d/cdqjq90oyrzswb/transactions).
- There's [a dashboard in Kibana](https://log.gprd.gitlab.net/app/lens#/edit/d2e51e80-33f5-4c2e-a401-fccf8858cad4) to identify repositories that are slow to snapshot.
- Go's execution traces can be useful for identifying delays in transaction processing and interactions between different goroutines.
  - [Gotraceui](https://gotraceui.dev) is a useful for digging into the traces.
  - We've annotated most notable methods of `TransactionManager` with user regions. These show up in Gotraceui and make it easier to identify the code being executed at a given location.
  - We also [log some information within the traces](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/transaction_manager.go#L218), including the correlation ID. This can be useful for identifying a particular RPC from the trace.
- The errors returned may include [helpful metadata](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/storagemgr/partition/conflict/fshistory/errors.go#L22) that is included in the logs in Kibana. When running into a transaction specific error, it is worth checking if the error includes metadata that helps make sense of it.
  - When investigating conflict related errors, the error includes the read LSN of the transaction, and the LSN of the conflicting write in the metadata. The LSN a write is committed under is [logged when a transaction commits](https://gitlab.com/gitlab-org/gitaly/-/blob/7c0f925b3df33c77de8c124b5f89447a13da3059/internal/gitaly/storage/log.go#L11). This is useful for identifying a write that produced the concurrent modification that led to aborting a transaction.
