package praefect

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRepositoryService struct {
	gitalypb.UnimplementedRepositoryServiceServer
	RepositoryExistsFunc    func(context.Context, *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error)
	ReplicateRepositoryFunc func(context.Context, *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error)
}

func (m *mockRepositoryService) RepositoryExists(ctx context.Context, r *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	return m.RepositoryExistsFunc(ctx, r)
}

func (m *mockRepositoryService) ReplicateRepository(ctx context.Context, r *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	return m.ReplicateRepositoryFunc(ctx, r)
}
