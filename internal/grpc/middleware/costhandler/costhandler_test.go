package costhandler

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/grpcstats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

func TestStaticCostForMethod(t *testing.T) {
	tests := []struct {
		name       string
		fullMethod string
		expected   int
	}{
		{
			name:       "overridden heavy streaming RPC",
			fullMethod: "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel",
			expected:   10,
		},
		{
			name:       "overridden lightweight RPC",
			fullMethod: "/gitaly.ServerService/ServerInfo",
			expected:   0,
		},
		{
			name:       "accessor falls back to default",
			fullMethod: "/gitaly.CommitService/FindCommit",
			expected:   defaultAccessorCost,
		},
		{
			name:       "mutator falls back to default",
			fullMethod: "/gitaly.OperationService/UserCreateBranch",
			expected:   defaultMutatorCost,
		},
		{
			name:       "unknown method returns default",
			fullMethod: "/unknown.Service/Unknown",
			expected:   defaultUnknownCost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := staticCostForMethod(tt.fullMethod)
			require.Equal(t, tt.expected, cost)
		})
	}
}

func TestComputeCost(t *testing.T) {
	t.Run("no stats in context", func(t *testing.T) {
		ctx := context.Background()
		cost := computeCost(ctx, "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel")
		require.Equal(t, 10, cost)
	})

	t.Run("static only when no bytes", func(t *testing.T) {
		handler := &grpcstats.PayloadBytes{}
		ctx := handler.TagRPC(context.Background(), nil)

		cost := computeCost(ctx, "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel")
		require.Equal(t, 10, cost)
	})

	t.Run("static plus byte cost", func(t *testing.T) {
		handler := &grpcstats.PayloadBytes{}
		ctx := handler.TagRPC(context.Background(), nil)
		stats := grpcstats.PayloadBytesStatsFromContext(ctx)
		stats.OutPayloadBytes = 3 * 1024 * 1024

		cost := computeCost(ctx, "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel")
		require.Equal(t, 13, cost)
	})

	t.Run("small payload rounds up to 1", func(t *testing.T) {
		handler := &grpcstats.PayloadBytes{}
		ctx := handler.TagRPC(context.Background(), nil)
		stats := grpcstats.PayloadBytesStatsFromContext(ctx)
		stats.InPayloadBytes = 512
		stats.OutPayloadBytes = 256

		cost := computeCost(ctx, "/gitaly.CommitService/FindCommit")
		require.Equal(t, 2, cost)
	})
}

type testService struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (ts testService) UnaryCall(_ context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{Payload: req.GetPayload()}, nil
}

func TestCostTrailerIntegration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	srv := grpc.NewServer(
		grpc.StatsHandler(&grpcstats.PayloadBytes{}),
		grpc.ChainUnaryInterceptor(UnaryInterceptor),
	)
	grpc_testing.RegisterTestServiceServer(srv, testService{})

	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "unix", socketPath)
	require.NoError(t, err)
	t.Cleanup(srv.GracefulStop)
	go testhelper.MustServe(t, srv, lis)

	cc, err := client.New(ctx, "unix://"+socketPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cc.Close()) })

	testClient := grpc_testing.NewTestServiceClient(cc)

	t.Run("small payload produces static + 1 dynamic", func(t *testing.T) {
		var trailer metadata.MD

		_, err := testClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: []byte("small")},
		}, grpc.Trailer(&trailer))
		require.NoError(t, err)

		costValues := trailer.Get(CostHeader)
		require.Len(t, costValues, 1)
		require.Equal(t, "2", costValues[0])
	})

	t.Run("large payload adds proportional dynamic cost", func(t *testing.T) {
		var trailer metadata.MD
		largeBody := make([]byte, 2*1024*1024)

		_, err := testClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: largeBody},
		}, grpc.Trailer(&trailer))
		require.NoError(t, err)

		costValues := trailer.Get(CostHeader)
		require.Len(t, costValues, 1)
		require.Equal(t, "4", costValues[0])
	})

	t.Run("trailer is always set even for minimal requests", func(t *testing.T) {
		var trailer metadata.MD

		_, err := testClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{},
			grpc.Trailer(&trailer))
		require.NoError(t, err)

		costValues := trailer.Get(CostHeader)
		require.NotEmpty(t, costValues)
	})
}
