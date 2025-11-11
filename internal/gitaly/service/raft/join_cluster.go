package raft

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
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
	}

	if err := routingTable.UpsertEntry(*routingEntry); err != nil {
		return nil, structerr.NewInternal("failed to update routing table: %w", err)
	}

	ctx = storage.ContextWithPartitionInfo(ctx, req.GetPartitionKey(), req.GetMemberId(), req.GetRelativePath())

	replicaRegistry := raftEnabledStorage.GetReplicaRegistry()
	err = s.createReplicaViaTransaction(ctx, req.GetRelativePath(), raftEnabledStorage, replicaRegistry, req.GetPartitionKey())
	if err != nil {
		// Clean up the routing table if replica creation fails
		if rollbackErr := s.rollbackRoutingTableEntry(req.GetPartitionKey(), routingTable); rollbackErr != nil {
			return nil, structerr.NewInternal("%w: failed to create replica: %w", rollbackErr, err)
		}

		return nil, structerr.NewInternal("failed to create replica: %w", err)
	}

	return &gitalypb.JoinClusterResponse{}, nil
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

	return nil
}

func (s *Server) createReplicaViaTransaction(ctx context.Context, relativePath string, storageManager storage.Storage, replicaRegistry raftmgr.ReplicaRegistry, partitionKey *gitalypb.RaftPartitionKey) (returnedErr error) {
	tx, err := storageManager.Begin(ctx, storage.TransactionOptions{
		RelativePath: relativePath,
		AllowPartitionAssignmentWithoutRepository: true,
	})
	if err != nil {
		return fmt.Errorf("begin bootstrap transaction: %w", err)
	}

	replica, err := replicaRegistry.GetReplica(partitionKey)
	if err != nil {
		return fmt.Errorf("replica not found after partition creation: %w", err)
	}

	started := replica.(*raftmgr.Replica).IsStarted()
	if !started {
		return fmt.Errorf("replica has not started")
	}

	defer func() {
		if returnedErr != nil {
			if err := tx.Rollback(ctx); err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("rollback: %w", err))
			}
		} else {
			commitLSN, err := tx.Commit(ctx)
			if err != nil {
				returnedErr = errors.Join(err, fmt.Errorf("fail to commit transaction: commit LSN: %d: %w", commitLSN, err))
			}
		}
	}()

	return returnedErr
}

// rollbackRoutingTableEntry restores the routing table to its previous state after a failed operation
func (s *Server) rollbackRoutingTableEntry(partitionKey *gitalypb.RaftPartitionKey, routingTable raftmgr.RoutingTable) error {
	if err := routingTable.DeleteEntry(partitionKey); err != nil {
		s.logger.WithField("partition_key", partitionKey).WithError(err).Error(
			"delete routing table entry during rollback",
		)
		return fmt.Errorf("delete routing table entry during rollback: %w", err)
	}
	return nil
}
