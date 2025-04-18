package raftmgr

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ConfChangeType represents the type of configuration change.
type ConfChangeType int

// Constants representing different configuration change types.
const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
	ConfChangeUpdateNode
)

// ReplicaConfChange represents a single configuration change.
type ReplicaConfChange struct {
	memberID   uint64
	changeType ConfChangeType
}

// ReplicaConfChanges is a wrapper around raftpb.ConfChangeI that provides
// a consistent interface for configuration changes regardless of the underlying
// implementation (ConfChange or ConfChangeV2).
type ReplicaConfChanges struct {
	changes  []ReplicaConfChange
	metadata *gitalypb.ReplicaID_Metadata
	term     uint64
	index    uint64
	leaderID uint64
}

// NewReplicaConfChanges creates a new ReplicaConfChanges instance.
func NewReplicaConfChanges(
	term uint64,
	index uint64,
	leaderID uint64,
	metadata *gitalypb.ReplicaID_Metadata,
) *ReplicaConfChanges {
	return &ReplicaConfChanges{
		changes:  make([]ReplicaConfChange, 0),
		metadata: metadata,
		term:     term,
		index:    index,
		leaderID: leaderID,
	}
}

// AddChange adds a configuration change to the changes list.
func (r *ReplicaConfChanges) AddChange(memberID uint64, nodeType ConfChangeType) {
	r.changes = append(r.changes, ReplicaConfChange{
		memberID:   memberID,
		changeType: nodeType,
	})
}

// Changes returns the list of changes.
func (r *ReplicaConfChanges) Changes() []ReplicaConfChange {
	return r.changes
}

// Metadata returns the metadata associated with the configuration changes.
func (r *ReplicaConfChanges) Metadata() *gitalypb.ReplicaID_Metadata {
	return r.metadata
}

// Term returns the term of the configuration changes.
func (r *ReplicaConfChanges) Term() uint64 {
	return r.term
}

// Index returns the index of the configuration changes.
func (r *ReplicaConfChanges) Index() uint64 {
	return r.index
}

// LeaderID returns the leader ID associated with the configuration changes.
func (r *ReplicaConfChanges) LeaderID() uint64 {
	return r.leaderID
}

// ToConfChangeV2 converts ReplicaConfChanges to a raftpb.ConfChangeV2.
func (r *ReplicaConfChanges) ToConfChangeV2() (raftpb.ConfChangeV2, error) {
	if len(r.changes) == 0 {
		return raftpb.ConfChangeV2{}, fmt.Errorf("no changes available to convert to ConfChangeV2")
	}

	changes := make([]raftpb.ConfChangeSingle, 0, len(r.changes))
	for _, change := range r.changes {
		var confType raftpb.ConfChangeType
		switch change.changeType {
		case ConfChangeAddNode:
			confType = raftpb.ConfChangeAddNode
		case ConfChangeRemoveNode:
			confType = raftpb.ConfChangeRemoveNode
		case ConfChangeUpdateNode:
			confType = raftpb.ConfChangeUpdateNode
		default:
			return raftpb.ConfChangeV2{}, fmt.Errorf("unknown conf change type: %d", change.changeType)
		}

		changes = append(changes, raftpb.ConfChangeSingle{
			Type:   confType,
			NodeID: change.memberID,
		})
	}

	var context []byte
	var err error
	if r.metadata != nil {
		context, err = proto.Marshal(r.metadata)
		if err != nil {
			return raftpb.ConfChangeV2{}, fmt.Errorf("marshal metadata: %w", err)
		}
	}

	return raftpb.ConfChangeV2{
		Context: context,
		Changes: changes,
	}, nil
}

// parseChangeType converts a raftpb.ConfChangeType to a ConfChangeType
func parseChangeType(ccType raftpb.ConfChangeType) (ConfChangeType, error) {
	switch ccType {
	case raftpb.ConfChangeAddNode:
		return ConfChangeAddNode, nil
	case raftpb.ConfChangeRemoveNode:
		return ConfChangeRemoveNode, nil
	case raftpb.ConfChangeUpdateNode:
		return ConfChangeUpdateNode, nil
	default:
		return ConfChangeType(0), fmt.Errorf("unknown conf change type: %d", ccType)
	}
}

// parseMetadata extracts metadata from the context byte slice
func parseMetadata(context []byte) (*gitalypb.ReplicaID_Metadata, error) {
	if len(context) == 0 {
		return nil, nil
	}

	metadata := &gitalypb.ReplicaID_Metadata{}
	if err := proto.Unmarshal(context, metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return metadata, nil
}

// The Convert function has been merged into ParseConfChange to reduce steps

// ParseConfChange parses a raftpb.Entry containing a configuration change directly into a ReplicaConfChanges.
// This handles unmarshalling for both EntryConfChange and EntryConfChangeV2 types and converts them
// directly to our unified format.
func ParseConfChange(entry raftpb.Entry, leaderID uint64) (*ReplicaConfChanges, error) {
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return nil, fmt.Errorf("unmarshalling EntryConfChange: %w", err)
		}

		metadata, err := parseMetadata(cc.Context)
		if err != nil {
			return nil, err
		}

		nodeType, err := parseChangeType(cc.Type)
		if err != nil {
			return nil, err
		}

		result := NewReplicaConfChanges(entry.Term, entry.Index, leaderID, metadata)
		result.AddChange(cc.NodeID, nodeType)
		return result, nil
	} else if entry.Type == raftpb.EntryConfChangeV2 {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			return nil, fmt.Errorf("unmarshalling EntryConfChangeV2: %w", err)
		}

		if len(cc.Changes) == 0 {
			return nil, fmt.Errorf("no changes in ConfChangeV2")
		}

		metadata, err := parseMetadata(cc.Context)
		if err != nil {
			return nil, err
		}

		result := NewReplicaConfChanges(entry.Term, entry.Index, leaderID, metadata)

		for _, change := range cc.Changes {
			nodeType, err := parseChangeType(change.Type)
			if err != nil {
				return nil, err
			}

			result.AddChange(change.NodeID, nodeType)
		}

		return result, nil
	}

	return nil, fmt.Errorf("entry is not a configuration change: %s", entry.Type)
}
