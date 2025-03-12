package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockRepositoryService struct {
	writeRef       func(context.Context, *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error)
	objectFormat   func(context.Context, *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error)
	setCustomHooks func(gitalypb.RepositoryService_SetCustomHooksServer) error
	getCustomHooks func(*gitalypb.GetCustomHooksRequest, gitalypb.RepositoryService_GetCustomHooksServer) error
	gitalypb.RepositoryServiceServer
}

func (m *mockRepositoryService) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	return m.writeRef(ctx, req)
}

func (m *mockRepositoryService) ObjectFormat(ctx context.Context, req *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error) {
	return m.objectFormat(ctx, req)
}

func (m *mockRepositoryService) SetCustomHooks(stream gitalypb.RepositoryService_SetCustomHooksServer) error {
	return m.setCustomHooks(stream)
}

func (m *mockRepositoryService) GetCustomHooks(req *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
	return m.getCustomHooks(req, stream)
}

func TestRetryPolicy(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	retryableError := status.Error(codes.Unavailable, "retryable error")

	failRequest := true
	failFirstRequest := func() error {
		if failRequest {
			failRequest = false
			return retryableError
		}

		return nil
	}

	// Configure a mock service that rejects only the first RPC call with retryable status code `UNAVAILABLE`.
	// If retries are configured correctly, a subsequent retry attempt would succeed.
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, &mockRepositoryService{
			writeRef: func(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
				if err := failFirstRequest(); err != nil {
					return nil, err
				}

				return nil, errors.New("never retried")
			},
			objectFormat: func(ctx context.Context, req *gitalypb.ObjectFormatRequest) (*gitalypb.ObjectFormatResponse, error) {
				if err := failFirstRequest(); err != nil {
					return nil, err
				}

				return &gitalypb.ObjectFormatResponse{}, nil
			},
			setCustomHooks: func(stream gitalypb.RepositoryService_SetCustomHooksServer) error {
				_, err := stream.Recv()
				if err != nil {
					return fmt.Errorf("recv: %w", err)
				}

				if err := failFirstRequest(); err != nil {
					return err
				}

				return errors.New("never retried")
			},
			getCustomHooks: func(req *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
				if err := failFirstRequest(); err != nil {
					return err
				}

				return stream.SendMsg(&gitalypb.GetCustomHooksResponse{})
			},
			RepositoryServiceServer: repository.NewServer(deps),
		})
	})

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipSnapshotInvalidation: true,
	})

	conn, err := client.New(ctx, cfg.SocketPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	client := gitalypb.NewRepositoryServiceClient(conn)

	t.Run("unary", func(t *testing.T) {
		t.Run("non-accessor", func(t *testing.T) {
			failRequest = true

			resp, err := client.WriteRef(ctx, &gitalypb.WriteRefRequest{Repository: repo})
			require.Equal(t, retryableError, err)
			require.Nil(t, resp)
		})

		t.Run("accessor", func(t *testing.T) {
			failRequest = true

			resp, err := client.ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{Repository: repo})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.ObjectFormatResponse{}, resp)
		})
	})

	t.Run("stream", func(t *testing.T) {
		t.Run("non-accessor", func(t *testing.T) {
			failRequest = true

			stream, err := client.SetCustomHooks(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.SetCustomHooksRequest{Repository: repo}))

			resp, err := stream.CloseAndRecv()
			require.Equal(t, retryableError, err)
			require.Nil(t, resp)
		})

		t.Run("accessor", func(t *testing.T) {
			failRequest = true

			stream, err := client.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: repo})
			require.NoError(t, err)

			resp, err := stream.Recv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.GetCustomHooksResponse{}, resp)
		})
	})
}
