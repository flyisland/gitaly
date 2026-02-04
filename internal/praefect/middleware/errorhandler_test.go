package middleware

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type repositoryService struct {
	gitalypb.UnimplementedRepositoryServiceServer
}

func (s *repositoryService) RepositoryExists(ctx context.Context, req *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	if req.GetRepository() == nil {
		return nil, structerr.NewInternal("error")
	}

	return &gitalypb.RepositoryExistsResponse{}, nil
}

func (s *repositoryService) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if req.GetRepository() == nil {
		return nil, structerr.NewInternal("error")
	}

	return &gitalypb.WriteRefResponse{}, nil
}

func TestStreamInterceptor(t *testing.T) {
	ctx := testhelper.Context(t)

	isInErrorWindow := true
	threshold := 5
	errTracker, err := tracker.NewErrors(ctx, func(_, _ time.Time) bool {
		return isInErrorWindow
	}, uint32(threshold), uint32(threshold))
	require.NoError(t, err)
	nodeName := "node-1"

	internalSrv := grpc.NewServer()

	internalServerSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "unix", internalServerSocketPath)
	require.NoError(t, err)

	gitalypb.RegisterRepositoryServiceServer(internalSrv, &repositoryService{})

	go testhelper.MustServe(t, internalSrv, lis)
	defer internalSrv.Stop()

	srvOptions := []grpc.ServerOption{
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context,
			fullMethodName string,
			peeker proxy.StreamPeeker,
		) (*proxy.StreamParameters, error) {
			cc, err := client.New(ctx, "unix://"+internalServerSocketPath,
				client.WithGrpcOptions([]grpc.DialOption{
					grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
					grpc.WithStreamInterceptor(StreamErrorHandler(protoregistry.GitalyProtoPreregistered, errTracker, nodeName)),
				}))
			require.NoError(t, err)
			t.Cleanup(func() { testhelper.MustClose(t, cc) })
			f, err := peeker.Peek()
			require.NoError(t, err)
			return proxy.NewStreamParameters(proxy.Destination{Conn: cc, Ctx: ctx, Msg: f}, nil, func() error { return nil }, nil), nil
		})),
	}

	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	praefectLis, err := lc.Listen(ctx, "unix", praefectSocket)
	require.NoError(t, err)

	praefectSrv := grpc.NewServer(srvOptions...)
	defer praefectSrv.Stop()
	go testhelper.MustServe(t, praefectSrv, praefectLis)

	praefectCC, err := client.New(ctx, "unix://"+praefectSocket)
	defer testhelper.MustClose(t, praefectCC)
	require.NoError(t, err)

	client := gitalypb.NewRepositoryServiceClient(praefectCC)

	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	for i := 0; i < threshold; i++ {
		_, err = client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		_, err = client.WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: repo,
		})
		require.NoError(t, err)
	}

	assert.False(t, errTracker.WriteThresholdReached(nodeName))
	assert.False(t, errTracker.ReadThresholdReached(nodeName))

	for i := 0; i < threshold; i++ {
		_, err = client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: nil,
		})
		require.Error(t, err)
		_, err = client.WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: nil,
		})
		require.Error(t, err)
	}

	assert.True(t, errTracker.WriteThresholdReached(nodeName))
	assert.True(t, errTracker.ReadThresholdReached(nodeName))

	isInErrorWindow = false

	for i := 0; i < threshold; i++ {
		_, err = client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		_, err = client.WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository: repo,
		})
		require.NoError(t, err)
	}

	assert.False(t, errTracker.WriteThresholdReached(nodeName))
	assert.False(t, errTracker.ReadThresholdReached(nodeName))
}
