package raftmgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func routingKey(partitionKey *gitalypb.PartitionKey) []byte {
	return []byte(fmt.Sprintf("/raft/%s/%d", partitionKey.GetAuthorityName(), partitionKey.GetPartitionId()))
}

// RoutingTableEntry represents a Raft cluster's routing state for a partition.
// It includes the current leader, all replicas, and Raft consensus metadata.
type RoutingTableEntry struct {
	RelativePath string // For backward compatibility
	Replicas     []*gitalypb.ReplicaID
	LeaderID     uint64
	Term         uint64
	Index        uint64
}

// ReplicaMetadata contains additional information about a replica
// that is needed for routing messages.
type ReplicaMetadata struct {
	Address string
}

// RoutingTable handles translation between node IDs and addresses
type RoutingTable interface {
	Translate(partitionKey *gitalypb.PartitionKey, nodeID uint64) (*gitalypb.ReplicaID, error)
	GetEntry(partitionKey *gitalypb.PartitionKey) (*RoutingTableEntry, error)
	UpsertEntry(entry RoutingTableEntry) error
}

// PersistentRoutingTable implements the RoutingTable interface with KV storage
type kvRoutingTable struct {
	kvStore keyvalue.Transactioner
	mutex   sync.RWMutex
}

// NewKVRoutingTable creates a new key-value based routing table implementation
// that persists routing information using badgerDB.
func NewKVRoutingTable(kvStore keyvalue.Store) *kvRoutingTable {
	prefix := []byte(fmt.Sprintf("p/%d", storagemgr.MetadataPartitionID))
	prefixedStore := keyvalue.NewPrefixedTransactioner(kvStore, prefix)
	return &kvRoutingTable{
		kvStore: prefixedStore,
	}
}

// UpsertEntry updates or creates a routing table entry
func (r *kvRoutingTable) UpsertEntry(entry RoutingTableEntry) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.kvStore.Update(func(txn keyvalue.ReadWriter) error {
		partitionKey := entry.Replicas[0].GetPartitionKey()
		key := routingKey(partitionKey)

		item, err := txn.Get(key)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("get existing entry: %w", err)
		}

		var existing *RoutingTableEntry
		if item != nil {
			existing = &RoutingTableEntry{}
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, existing)
			}); err != nil {
				return fmt.Errorf("unmarshal existing entry: %w", err)
			}
		}

		// Only update if new entry has higher term or index
		if existing != nil {
			if entry.Term < existing.Term ||
				(entry.Term == existing.Term && entry.Index <= existing.Index) {
				return fmt.Errorf("stale entry: current term=%d,index=%d, new term=%d,index=%d",
					existing.Term, existing.Index, entry.Term, entry.Index)
			}
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return fmt.Errorf("set entry: %w", err)
		}

		return nil
	})
}

// GetEntry retrieves a routing table entry
func (r *kvRoutingTable) GetEntry(partitionKey *gitalypb.PartitionKey) (*RoutingTableEntry, error) {
	key := routingKey(partitionKey)

	var entry RoutingTableEntry
	if err := r.kvStore.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(value []byte) error {
			return json.Unmarshal(value, &entry)
		})
	}); err != nil {
		return nil, fmt.Errorf("view: %w", err)
	}

	return &entry, nil
}

// Translate returns the storage name and address for a given partition key and node ID
func (r *kvRoutingTable) Translate(partitionKey *gitalypb.PartitionKey, nodeID uint64) (*gitalypb.ReplicaID, error) {
	entry, err := r.GetEntry(partitionKey)
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}

	// Look for the node in replicas
	for _, replica := range entry.Replicas {
		if replica.GetNodeId() == nodeID {
			return replica, nil
		}
	}

	return nil, fmt.Errorf("no address found for nodeID %d", nodeID)
}
