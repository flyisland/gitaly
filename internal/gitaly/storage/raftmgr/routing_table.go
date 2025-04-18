package raftmgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
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

// RoutingTable handles translation between member IDs and addresses
type RoutingTable interface {
	Translate(partitionKey *gitalypb.PartitionKey, memberID uint64) (*gitalypb.ReplicaID, error)
	GetEntry(partitionKey *gitalypb.PartitionKey) (*RoutingTableEntry, error)
	UpsertEntry(entry RoutingTableEntry) error
	ApplyConfChange(term uint64, index uint64, leaderID uint64, partitionKey *gitalypb.PartitionKey, cc raftpb.ConfChangeI) error
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

// Translate returns the storage name and address for a given partition key and member ID
func (r *kvRoutingTable) Translate(partitionKey *gitalypb.PartitionKey, memberID uint64) (*gitalypb.ReplicaID, error) {
	entry, err := r.GetEntry(partitionKey)
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}

	// Look for the node in replicas
	for _, replica := range entry.Replicas {
		if replica.GetMemberId() == memberID {
			return replica, nil
		}
	}

	return nil, fmt.Errorf("no address found for memberID %d", memberID)
}

func (r *kvRoutingTable) ApplyConfChange(term uint64, index uint64, leaderID uint64, partitionKey *gitalypb.PartitionKey, cc raftpb.ConfChangeI) error {
	routingTableEntry, err := r.GetEntry(partitionKey)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("getting routing table entry: %w", err)
	}

	if routingTableEntry == nil {
		routingTableEntry = &RoutingTableEntry{
			Replicas: []*gitalypb.ReplicaID{},
		}
	}
	routingTableEntry.LeaderID = leaderID
	routingTableEntry.Term = term
	routingTableEntry.Index = index

	authorityName := partitionKey.GetAuthorityName()

	switch cc := cc.(type) {
	case raftpb.ConfChange:
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			var metadata gitalypb.ReplicaID_Metadata
			if err := proto.Unmarshal(cc.Context, &metadata); err != nil {
				return fmt.Errorf("unmarshal node address: %w", err)
			}

			if cc.NodeID == 0 {
				return fmt.Errorf("nodeID should be non-zero")
			}

			replica := &gitalypb.ReplicaID{
				PartitionKey: partitionKey,
				MemberId:     cc.NodeID,
				StorageName:  authorityName,
				Metadata:     &metadata,
			}
			routingTableEntry.Replicas = append(routingTableEntry.Replicas, replica)

		case raftpb.ConfChangeRemoveNode:
			if len(routingTableEntry.Replicas) == 0 {
				return fmt.Errorf("no replicas to remove")
			}

			routingTableEntry.Replicas = slices.DeleteFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
				return r.GetMemberId() == cc.NodeID
			})

		case raftpb.ConfChangeUpdateNode:
			var metadata gitalypb.ReplicaID_Metadata
			if err := proto.Unmarshal(cc.Context, &metadata); err != nil {
				return fmt.Errorf("unmarshal node address: %w", err)
			}
			for i, r := range routingTableEntry.Replicas {
				if r.GetMemberId() == cc.NodeID {
					routingTableEntry.Replicas[i].Metadata = &metadata
					break
				}
			}

		default:
			return fmt.Errorf("unknown conf change type: %d", cc.Type)
		}
	case raftpb.ConfChangeV2:
		// Unmarshal the address from the context - it will be the same for all changes
		var metadata gitalypb.ReplicaID_Metadata
		if err := proto.Unmarshal(cc.Context, &metadata); err != nil {
			return fmt.Errorf("unmarshal node address: %w", err)
		}

		for _, change := range cc.Changes {
			switch change.Type {
			case raftpb.ConfChangeAddNode:
				if change.NodeID == 0 {
					return fmt.Errorf("nodeID should be non-zero")
				}
				replica := &gitalypb.ReplicaID{
					PartitionKey: partitionKey,
					MemberId:     change.NodeID,
					StorageName:  authorityName,
					Metadata:     &metadata,
				}
				routingTableEntry.Replicas = append(routingTableEntry.Replicas, replica)
			case raftpb.ConfChangeRemoveNode:
				if len(routingTableEntry.Replicas) == 0 {
					return fmt.Errorf("no replicas to remove")
				}
				routingTableEntry.Replicas = slices.DeleteFunc(routingTableEntry.Replicas, func(r *gitalypb.ReplicaID) bool {
					return r.GetMemberId() == change.NodeID
				})

			case raftpb.ConfChangeUpdateNode:
				for i, r := range routingTableEntry.Replicas {
					if r.GetMemberId() == change.NodeID {
						routingTableEntry.Replicas[i].Metadata = &metadata
						break
					}
				}
			default:
				return fmt.Errorf("unknown conf change type: %d", change.Type)
			}
		}
	}

	// Update routing table with new entry
	if err := r.UpsertEntry(*routingTableEntry); err != nil {
		return fmt.Errorf("updating routing table: %w", err)
	}

	return nil
}
