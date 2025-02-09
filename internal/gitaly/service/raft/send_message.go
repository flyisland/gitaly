package raft

import (
	"errors"
	"io"

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

		if req.GetAuthorityName() == "" {
			return structerr.NewInvalidArgument("authority_name is required")
		}
		if req.GetPartitionId() == 0 {
			return structerr.NewInvalidArgument("partition_id is required")
		}

		raftMsg := req.GetMessage()

		if err := s.transport.Receive(stream.Context(), req.GetPartitionId(), req.GetAuthorityName(), *raftMsg); err != nil {
			return structerr.NewInternal("receive error: %w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.RaftMessageResponse{})
}
