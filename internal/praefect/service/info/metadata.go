package info

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetRepositoryMetadata returns the cluster metadata for a repository.
func (s *Server) GetRepositoryMetadata(ctx context.Context, req *gitalypb.GetRepositoryMetadataRequest) (*gitalypb.GetRepositoryMetadataResponse, error) {
	var getMetadata func() (datastore.RepositoryMetadata, error)
	switch query := req.GetQuery().(type) {
	case *gitalypb.GetRepositoryMetadataRequest_RepositoryId:
		getMetadata = func() (datastore.RepositoryMetadata, error) {
			return s.rs.GetRepositoryMetadata(ctx, query.RepositoryId)
		}
	case *gitalypb.GetRepositoryMetadataRequest_Path_:
		getMetadata = func() (datastore.RepositoryMetadata, error) {
			return s.rs.GetRepositoryMetadataByPath(ctx, query.Path.GetVirtualStorage(), query.Path.GetRelativePath())
		}
	default:
		return nil, structerr.NewInternal("unknown query type: %T", query)
	}

	metadata, err := getMetadata()
	if err != nil {
		if errors.Is(err, datastore.ErrRepositoryNotFound) {
			return nil, structerr.NewNotFound("%w", err)
		}

		return nil, structerr.NewInternal("get metadata: %w", err)
	}

	replicas := make([]*gitalypb.GetRepositoryMetadataResponse_Replica, 0, len(metadata.Replicas))
	for _, replica := range metadata.Replicas {
		var verifiedAt *timestamppb.Timestamp
		if !replica.VerifiedAt.IsZero() {
			verifiedAt = timestamppb.New(replica.VerifiedAt)
		}

		replicas = append(replicas, &gitalypb.GetRepositoryMetadataResponse_Replica{
			Storage:      replica.Storage,
			Assigned:     replica.Assigned,
			Generation:   replica.Generation,
			Healthy:      replica.Healthy,
			ValidPrimary: replica.ValidPrimary,
			VerifiedAt:   verifiedAt,
		})
	}

	return &gitalypb.GetRepositoryMetadataResponse{
		RepositoryId:   metadata.RepositoryID,
		VirtualStorage: metadata.VirtualStorage,
		RelativePath:   metadata.RelativePath,
		ReplicaPath:    metadata.ReplicaPath,
		Primary:        metadata.Primary,
		Generation:     metadata.Generation,
		Replicas:       replicas,
	}, nil
}
