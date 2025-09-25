package raftmgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func routingKey(partitionKey *gitalypb.RaftPartitionKey) []byte {
	return []byte(fmt.Sprintf("raft/%s", partitionKey.GetValue()))
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

// RoutingTable handles translation between member IDs and addresses
type RoutingTable interface {
	Translate(partitionKey *gitalypb.RaftPartitionKey, memberID uint64) (*gitalypb.ReplicaID, error)
	GetEntry(partitionKey *gitalypb.RaftPartitionKey) (*RoutingTableEntry, error)
	UpsertEntry(entry RoutingTableEntry) error
	ApplyReplicaConfChange(storageName string, partitionKey *gitalypb.RaftPartitionKey, changes *ReplicaConfChanges) error
	ListEntries() (map[string]*RoutingTableEntry, error)
}

// PersistentRoutingTable implements the RoutingTable interface with KV storage
type kvRoutingTable struct {
	kvStore keyvalue.Transactioner
	mutex   sync.RWMutex
}

// NewKVRoutingTable creates a new key-value based routing table implementation
// that persists routing information using badgerDB.
func NewKVRoutingTable(kvStore keyvalue.Store) *kvRoutingTable {
	prefix := storagemgr.KeyPrefixPartition(storagemgr.MetadataPartitionID)
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
		if len(entry.Replicas) == 0 {
			return fmt.Errorf("no replicas to upsert")
		}

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
func (r *kvRoutingTable) GetEntry(partitionKey *gitalypb.RaftPartitionKey) (*RoutingTableEntry, error) {
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

// Translate returns the storage name and address for a given partition key and member ID
func (r *kvRoutingTable) Translate(partitionKey *gitalypb.RaftPartitionKey, memberID uint64) (*gitalypb.ReplicaID, error) {
	entry, err := r.GetEntry(partitionKey)
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}

	for _, replica := range entry.Replicas {
		if replica.GetMemberId() == memberID {
			return replica, nil
		}
	}

	return nil, fmt.Errorf("no address found for memberID %d", memberID)
}

func (r *kvRoutingTable) ApplyReplicaConfChange(storageName string, partitionKey *gitalypb.RaftPartitionKey, changes *ReplicaConfChanges) error {
	routingTableEntry, err := r.GetEntry(partitionKey)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("getting routing table entry: %w", err)
	}

	if routingTableEntry == nil {
		routingTableEntry = &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{},
		}
	}

	routingTableEntry.LeaderID = changes.LeaderID()
	routingTableEntry.Term = changes.Term()
	routingTableEntry.Index = changes.Index()

	metadata := changes.Metadata()

	for _, confChange := range changes.Changes() {
		switch confChange.changeType {
		case ConfChangeAddNode:
			if confChange.memberID == 0 {
				return fmt.Errorf("member ID should be non-zero")
			}

			if slices.ContainsFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMemberId() == confChange.memberID
			}) {
				return fmt.Errorf("member ID %d already exists", confChange.memberID)
			}

			replica := &gitalypb.ReplicaID{
				PartitionKey: partitionKey,
				MemberId:     confChange.memberID,
				StorageName:  storageName,
				Metadata:     metadata,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_VOTER,
			}
			routingTableEntry.Replicas = append(routingTableEntry.Replicas, replica)

		case ConfChangeAddLearnerNode:
			if confChange.memberID == 0 {
				return fmt.Errorf("member ID should be non-zero")
			}

			if slices.ContainsFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMemberId() == confChange.memberID
			}) {
				return fmt.Errorf("member ID %d already exists as a replica", confChange.memberID)
			}

			learner := &gitalypb.ReplicaID{
				PartitionKey: partitionKey,
				MemberId:     confChange.memberID,
				StorageName:  storageName,
				Metadata:     metadata,
				Type:         gitalypb.ReplicaID_REPLICA_TYPE_LEARNER,
			}
			routingTableEntry.Replicas = append(routingTableEntry.Replicas, learner)

		case ConfChangeRemoveNode:
			if len(routingTableEntry.Replicas) == 0 {
				return fmt.Errorf("no replicas to remove")
			}

			routingTableEntry.Replicas = slices.DeleteFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMemberId() == confChange.memberID
			})

		case ConfChangeUpdateNode:
			index := slices.IndexFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMemberId() == confChange.memberID
			})
			if index == -1 {
				return fmt.Errorf("member ID %d not found for update", confChange.memberID)
			}
			routingTableEntry.Replicas[index].Metadata = metadata

		default:
			return fmt.Errorf("unknown conf change type: %d", confChange.changeType)
		}
	}

	// Update routing table with new entry
	if err := r.UpsertEntry(*routingTableEntry); err != nil {
		return fmt.Errorf("updating routing table: %w", err)
	}

	return nil
}

// ListEntries returns routing table entries.
func (r *kvRoutingTable) ListEntries() (map[string]*RoutingTableEntry, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	entries := make(map[string]*RoutingTableEntry)

	// With opaque partition keys, we return all entries.
	prefix := "raft/"

	if err := r.kvStore.View(func(txn keyvalue.ReadWriter) error {
		iter := txn.NewIterator(keyvalue.IteratorOptions{
			Prefix: []byte(prefix),
		})
		defer iter.Close()

		for iter.Seek([]byte(prefix)); iter.Valid(); iter.Next() {
			var entry RoutingTableEntry
			if err := iter.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &entry)
			}); err != nil {
				return fmt.Errorf("unmarshal entry: %w", err)
			}

			// Use the key as the map key for easy lookup
			keyStr := string(iter.Item().Key())
			entries[keyStr] = &entry
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("view: %w", err)
	}

	return entries, nil
}
