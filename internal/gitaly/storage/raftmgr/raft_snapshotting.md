## Raft Snapshots
#### Streaming Snapshots 
When a new follower with no state joins the cluster, the leader may send a streaming snapshot requested on demand to let it catch up to the latest state. Another use case is when a follower is too far behind and is in need of entries that have already been compacted on the leader, the leader will have to send a snapshot over the network. 

When the leader starts a transaction to invoke InstallSnapshot RPC, it will reuse the snapshot created in that transaction by materialising it as an archive of the snapshot. Considerable space will be needed for this packaging step if performed on disk. Hence, streaming is used to avoid buffering the entire state machine in memory and having to store yet another compressed copy of the archive. 

Compression and checksum will be used for reducing the size of a snapshot and maintaining data integrity respectively. Hence our approach is to implement streaming compression so the sending of chunks and compression happens simultaneously.

Snapshots will be initially created as a .tmp file in the configured raft SnapshotDir. Once the entire operation is done, it will be renamed with a .snap extension and flushed to disk ensuring no node will receive a half written snapshot.

```mermaid
sequenceDiagram
    participant Leader
    participant Compressor
    participant LeaderStorage
    participant Transport
    participant Follower
    participant Decompressor
    participant FollowerStorage
    participant Txn as Transactions
    participant LM as LogManager

    Leader->>LeaderStorage: Request snapshot
    LeaderStorage->>Txn: Get Snapshot
    Txn-->>LeaderStorage: Return snapshot with hardlinks from transactions
    LeaderStorage->>LeaderStorage: Store snapshot and metadata
    LeaderStorage-->>Leader: Successfully saved snapshot to disk
    Leader->>Leader: Package snapshot
    Leader->>Compressor: Start compression stream
    Compressor->>Transport: Start grpc streaming
    
    loop Until InstallSnapshot is sent with done = true
        Leader->>Follower: InstallSnapshot RPC (term, leaderId, lastConfig, lastIncludedIndex, lastIncludedTerm, data[], done)

    alt term < currentTerm
        Follower-->>Leader: Reject (currentTerm)
    else term >= currentTerm
        Follower->>Follower: Update currentTerm if necessary
        Follower->>Decompressor: Initialize decompression stream
        Follower->>FollowerStorage: Create temporary snapshot file
        loop Until last chunk is transferred or when done is true
            Leader->>LeaderStorage: Read chunk of snapshot data
            LeaderStorage-->>Leader: Return data chunk
            Leader->>Compressor: Compress data chunk
            Compressor-->>Leader: Compressed chunk
            Leader->>Transport: Send compressed chunk
            Transport->>Follower: Receive compressed chunk
            Follower->>Follower: Reset election timer
            Follower->>Decompressor: Decompress chunk
            Decompressor-->>Follower: Decompressed chunk
            Follower->>FollowerStorage: Write decompressed chunk
        end
        Leader->>Follower: Send 'done' signal
        Follower->>FollowerStorage: Finalize snapshot
        FollowerStorage-->>Follower: Renamed snapshot
        Follower->>FollowerStorage: Restore snapshot
        FollowerStorage->>FollowerStorage: Applies snapshot to disk
        Follower->>LM: Compact log and update state
        LM-->>Follower: Discarded logs up to lastIncludedIndex, updated follower's snapshotIndex
        Follower-->>Leader: Success (currentTerm)
    end
    end
    Leader->>Compressor: Close compression stream
    Follower->>Decompressor: Close decompression stream
    LeaderStorage-->>Leader: Streamed compressed snapshot successfully
```