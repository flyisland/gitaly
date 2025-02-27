package raft

import (
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// SendMessage is a gRPC method for sending a Raft message across nodes.
func (s *Server) SendMessage(stream gitalypb.RaftService_SendMessageServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return structerr.NewInternal("receive error: %w", err)
		}

		replicaID := req.GetReplicaId()
		partitionKey := replicaID.GetPartitionKey()
		authorityName := partitionKey.GetAuthorityName()
		partitionID := partitionKey.GetPartitionId()

		// The cluster ID protects Gitaly from cross-cluster interactions, which could potentially corrupt the clusters.
		// This is particularly crucial after disaster recovery so that an identical cluster is restored from backup.
		if req.GetClusterId() == "" {
			return structerr.NewInvalidArgument("cluster_id is required")
		}

		// Let's assume we have a single cluster per node for now.
		if req.GetClusterId() != s.cfg.Raft.ClusterID {
			return structerr.NewPermissionDenied("message from wrong cluster: got %q, want %q",
				req.GetClusterId(), s.cfg.Raft.ClusterID)
		}

		if authorityName == "" {
			return structerr.NewInvalidArgument("authority_name is required")
		}
		if partitionID == 0 {
			return structerr.NewInvalidArgument("partition_id is required")
		}

		storageName := replicaID.GetStorageName()
		node, ok := s.node.(*raftmgr.Node)
		if !ok {
			return structerr.NewInternal("node is not Raft-enabled")
		}

		storageManager, err := node.GetStorage(storageName)
		if err != nil {
			return structerr.NewInternal("get storage manager: %w", err)
		}

		raftStorage, ok := storageManager.(*raftmgr.RaftStorageWrapper)
		if !ok {
			return structerr.NewInternal("storage is not Raft-enabled")
		}

		transport := raftStorage.GetTransport()
		if transport == nil {
			return structerr.NewInternal("transport not available")
		}

		if err := transport.Receive(stream.Context(), partitionKey, *req.GetMessage()); err != nil {
			return structerr.NewInternal("receive error: %w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.RaftMessageResponse{})
}
