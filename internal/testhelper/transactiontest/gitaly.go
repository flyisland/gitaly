package transactiontest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// ForceSnapshotInvalidation provides a temporary workaround that allows tests to assert against the state of a repository
// after any pending changes are applied. It invokes the WriteRef RPC that will start a new transaction, which
// will be forced to wait for previous changes to be applied before the RPC returns. It invalidates any cached
// snapshots.
func ForceSnapshotInvalidation(tb testing.TB, ctx context.Context, revision string, conn *grpc.ClientConn, repo *gitalypb.Repository) {
	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err := client.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository: repo,
		Ref:        []byte(fmt.Sprintf("refs/heads/temp-%d", time.Now().UnixNano())), // Use unique ref name,
		Revision:   []byte(revision),
	})
	require.NoError(tb, err)
}

// ForceWALSync provides a temporary workaround that allows tests to assert against the state of a repository
// after any pending changes are applied. It invokes RepositoryExists RPC that will start a new transaction, which
// will be forced to wait for previous changes to be applied before the RPC returns. This workaround will leave behind
// a cached snapshot of the repository.
func ForceWALSync(tb testing.TB, ctx context.Context, conn *grpc.ClientConn, repo *gitalypb.Repository) {
	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: repo,
	})
	require.NoError(tb, err)
}
