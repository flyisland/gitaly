package raft

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// JoinCluster handles leader-initiated node joining. The leader calls this RPC on a new node
// to instruct it to join the cluster. The leader will commit the ConfChange
// after calling this RPC to ensure atomic cluster membership changes.
func (s *Server) JoinCluster(ctx context.Context, req *gitalypb.JoinClusterRequest) (*gitalypb.JoinClusterResponse, error) {
	if err := s.validateJoinClusterRequest(req); err != nil {
		return nil, structerr.NewInvalidArgument("invalid join cluster request: %w", err)
	}

	storageInterface, err := s.node.GetStorage(req.GetStorageName())
	if err != nil {
		return nil, structerr.NewInternal("failed to get storage: %w", err)
	}

	raftEnabledStorage, ok := storageInterface.(*raftmgr.RaftEnabledStorage)
	if !ok {
		return nil, structerr.NewInternal("storage is not Raft-enabled")
	}

	routingTable := raftEnabledStorage.GetRoutingTable()
	if routingTable == nil {
		return nil, structerr.NewInternal("routing table not available")
	}

	if err := s.validateMemberID(req.GetPartitionKey(), req.GetMemberId(), routingTable); err != nil {
		return nil, structerr.NewInvalidArgument("member ID validation failed: %w", err)
	}

	routingEntry := &raftmgr.RoutingTableEntry{
		RelativePath: req.GetRelativePath(),
		Replicas:     req.GetReplicas(),
		LeaderID:     req.GetLeaderId(),
		Term:         req.GetTerm(),
		Index:        req.GetIndex(),
	}

	if err := routingTable.UpsertEntry(*routingEntry); err != nil {
		return nil, structerr.NewInternal("failed to update routing table: %w", err)
	}

	return nil, nil
}

func (s *Server) validateMemberID(partitionKey *gitalypb.RaftPartitionKey, memberID uint64, routingTable raftmgr.RoutingTable) error {
	_, err := routingTable.Translate(partitionKey, memberID)
	if err == nil {
		return fmt.Errorf("member ID %d already exists in the cluster", memberID)
	}
	return nil
}

func (s *Server) validateJoinClusterRequest(req *gitalypb.JoinClusterRequest) error {
	if req.GetPartitionKey() == nil {
		return fmt.Errorf("partition_key is required")
	}

	if req.GetMemberId() == 0 {
		return fmt.Errorf("member_id is required")
	}

	if req.GetStorageName() == "" {
		return fmt.Errorf("storage_name is required")
	}

	if len(req.GetReplicas()) == 0 {
		return fmt.Errorf("replica is required")
	}

	if req.GetLeaderId() == 0 {
		return fmt.Errorf("leader_id is required")
	}

	if req.GetTerm() == 0 {
		return fmt.Errorf("term is required")
	}

	if req.GetIndex() == 0 {
		return fmt.Errorf("index is required")
	}

	return nil
}
