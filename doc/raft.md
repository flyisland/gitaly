# Gitaly's Multi-Raft Architecture

## Overview

The unit of Git data storage is the **repository**. Repositories in a Gitaly cluster are hosted within **storages**, each
with a globally unique name. A Gitaly server can host multiple storages simultaneously. Downstream services,
particularly GitLab Rails, maintain a mapping between repositories, storages, and the addresses of the servers hosting
them.

Within each storage, repositories are grouped into **partitions**. A partition may consist of a single repository or a
collection of related repositories, such as those belonging to the same object pool in the case of a fork network.
Write-Ahead Logging (WAL) operates at the partition level, with all repositories in a partition sharing the same
monotonic log sequence number (LSN). Partition log entries are applied sequentially.

The `raftmgr` package manages each partition in a separate Raft consensus group (Raft group), which operates
independently. Partitions are replicated across multiple storages. Storage is where partitions are placed. If a
storage creates a partition, it will mint its ID, but there's no special relationship between the partition and
storage. A partition can be moved freely to another storage. At any given time, a single Gitaly server can host thousands
or even hundreds of thousands of Raft groups.

Partitions are replicated across multiple storages. A storage can host both its own partitions and replicas of
partitions from other storages.

The data size and workload of partitions vary significantly. For example, a partition containing a monorepository and
its forks can dominate a server's resource usage and traffic. As a result, replication factors and routing tables are
designed at the partition level. Each partition can define its own replication constraints, such as locality and
optional storage capacity (e.g., HDD or SSD).

## Partition Identity and Membership Management

Each partition in a cluster is identified by a globally unique **Partition ID**. The Partition ID of a partition is generated
once when it is created. Under the hood, the Partition ID is a tuple of `(Authority Name, Local Partition ID)`, in which:

- Authority Name: Represents the storage that created the partition.
- Local Partition ID: A local, monotonic identifier for the partition within the authority's storage.

This identity system allows distributed partition creation without the need for a global ID registry. Although the
authority storage name is a part of the Partition ID, it does not imply ownership of the partition. A partition can move
freely to other storages after creation.

Each partition is a Raft group with one or more members. Raft groups are also identified by the Partition ID due to
the one-to-one relationship.

The `raftmgr.Manager` oversees all Raft activities for a Raft group member. Internally, etcd/raft assigns an
integer **Node ID** to a Raft group member. The Node ID does not change throughout the lifecycle of a group member.

When a partition is bootstrapped for the first time, the Raft manager initializes the `etcd/raft` state machine,
elects itself as the initial leader, and persists all Raft metadata to persistent storage. Its internal Node ID
is always 1 at this stage, making it a fully functional single-node Raft instance.

When a new member joins a Raft group, the leader issues a Config Change entry. This entry contains the metadata
of the storage, such as storage name, address, and authentication/authorization info. The new member's Node ID
is assigned the LSN of this log entry, ensuring unambiguous identification of members. As the LSN is monotonic,
Node IDs are never reused even if the storage re-joins later. This Node ID system is not exposed outside the
scope of `etcd/raft` integration.

Since Gitaly follows a multi-Raft architecture, the Node ID alone is insufficient to precisely locate a
partition or Raft group replica. Therefore, each replica (including the leader) is identified using a
**Replica ID**, which consists of `(Partition ID, Node ID, Replica Storage Name)`.

To ensure fault tolerance in a quorum-based system, a Raft cluster requires a minimum replication factor of 3.
It can tolerate up to `(n-1)/2` failures. For example, a 3-replica cluster can tolerate 1 failure.

The cluster maintains a global, eventually consistent **routing table** for lookup and routing purposes. Each entry
in the table includes:

```plaintext
Partition ID: [
    RelativePath: "@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git"
    Replicas: [
      <Replica ID 1, Replica 1 Metadata>
      <Replica ID 2, Replica 2 Metadata>
      <Replica ID 3, Replica 3 Metadata>
    ]
    Term: 99
    Index: 80
}
```

The current leader of the Raft group is responsible for updating the respective entry in the routing table. The routing
table is updated only if the replica set changes. Leadership changes are not broadcasted due to the high number of Raft
groups and the high update frequency. The routing table and advertised storage addresses are propagated via the
gossip network.

The `RelativePath` field in the routing table aims to maintain backward compatibility with GitLab-specific clients. It is
removed after clients fully transition to the new Partition ID system. The `Term` and `Index` fields ensure the order of
updates. Entries of the same Partition ID with either a lower term or index are replaced by newer ones.

The following chart illustrates a simplified semantic organization of data (not actual data placement) on a Gitaly
server.

```plaintext
gitaly-server-1344
|_ storage-a
   |_ <Replica ID>
      |_ repository @hashed/aa/aabbcc...git
      |_ repository @hashed/bb/bbccdd...git (fork of @hashed/aa/aabbcc...git)
      |_ repository @hashed/cc/ccddee...git (fork of @hashed/aa/aabbcc...git)
   |_ <Replica ID> (current leader)
      |_ repository @hashed/dd/ddeeff...git
   |_ <Replica ID>
      |_ repository @hashed/ee/eeffgg...git
   |_ <Replica ID> (current leader)
      |_ repository @hashed/ff/ffgghh...git
   |_ <Replica ID>
      |_ repository @hashed/gg/gghhii...git
   |_ ...
```

## Communication Between Members of a Raft Group

The `etcd/raft` package does not include network communication implementation. Gitaly uses gRPC to transfer messages.
Messages are sent through a single RPC, `RaftService.SendMessage`, which enhances Raft messages with Gitaly's partition
identity metadata. Membership management is facilitated by another RPC (TBD).

Given that a Gitaly server may host a large number of Raft groups, the "chatty" nature of the Raft protocol can
cause potential issues. Additionally, the protocol is sensitive to health-check failures. To mitigate these challenges,
Gitaly applies techniques such as batching node health checks and quiescing inactive Raft groups.

## Communication Between Client and Gitaly Cluster (via Proxying)

TBD

## Interaction with Transactions and WAL

The Raft manager implements the `storage.LogManager` interface. By default, the Transaction Manager uses `log.Manager`.
All log entries are appended to the filesystem WAL. Once an entry is persisted, it is ready to be applied by the
Transaction Manager. When Raft is enabled, `log.Manager` is replaced by `raftmgr.Manager` which manages the entire
commit flow, including network communications and quorum acknowledgment.

The responsibilities of three critical components are:

- `log.Manager`: Handles local log management.
- `raftmgr.Manager`: Manages distributed log management.
- `partition.TransactionManager`: Manages transactions, concurrency, snapshots, conflicts, and related tasks.

The flow is illustrated in the following chart:

```plaintext
                                              ┌──────────┐
                                              │Local Disk│
                                              └────▲─────┘
                                                   │
                                               log.Manager
                                                   ▲
TransactionManager    New Transaction              │ Without Raft
         │                   ▼                     │
         └─►Initialize─►txn.Commit()─►Verify─► AppendLogEntry()─────►Apply
                │                                  │
                │                                  │ With Raft
                ▼                                  ▼
         ┌─►Initialize─────...──────────────────Propose
         │                                         │
raftmgr.Manager                          etcd/raft state machine

                                           ┌───────┴────────┐
                                           ▼                ▼
                                    raftmgr.Storage raftmgr.Transport
                                           │                │
                                           ▼                │
                                       log.Manager       Network
                                           │                │
                                     ┌─────▼────┐     ┌─────▼────┐
                                     │Local Disk│     │Raft Group│
                                     └──────────┘     └──────────┘
```

## References

- [Raft-based decentralized architecture for Gitaly Cluster](https://gitlab.com/groups/gitlab-org/-/epics/8903)
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation)
- [etcd/raft package documentation](https://pkg.go.dev/go.etcd.io/etcd/raft/v3)
