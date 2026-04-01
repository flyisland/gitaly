package backup

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerSideAdapter allows calling the server-side backup RPCs `BackupRepository`
// and `RestoreRepository` through `backup.Strategy` such that server-side
// backups can be used with `backup.Pipeline`. It also implements
// `backup.IDHandler` via dedicated RPCs.
type ServerSideAdapter struct {
	pool *client.Pool
}

// NewServerSideAdapter creates and returns initialized *ServerSideAdapter instance.
func NewServerSideAdapter(pool *client.Pool) *ServerSideAdapter {
	return &ServerSideAdapter{
		pool: pool,
	}
}

// Create calls the BackupRepository RPC.
func (ss ServerSideAdapter) Create(ctx context.Context, req *CreateRequest) error {
	if err := setContextServerInfo(ctx, &req.Server, req.Repository.GetStorageName()); err != nil {
		return fmt.Errorf("server-side create: %w", err)
	}

	client, err := ss.newRepoClient(ctx, req.Server)
	if err != nil {
		return fmt.Errorf("server-side create: %w", err)
	}

	_, err = client.BackupRepository(ctx, &gitalypb.BackupRepositoryRequest{
		Repository:       req.Repository,
		VanityRepository: req.VanityRepository,
		BackupId:         req.BackupID,
		LatestBackupId:   req.LatestBackupID,
		Incremental:      req.Incremental,
	})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.NotFound {
			return fmt.Errorf("server-side create: %w: %s", ErrSkipped, err.Error())
		}
		for _, detail := range st.Details() {
			switch detail.(type) {
			case *gitalypb.BackupRepositoryResponse_SkippedError:
				return fmt.Errorf("server-side create: %w: %s", ErrSkipped, err.Error())
			}
		}

		return fmt.Errorf("server-side create: %w", err)
	}

	return nil
}

// Restore calls the RestoreRepository RPC.
func (ss ServerSideAdapter) Restore(ctx context.Context, req *RestoreRequest) error {
	if err := setContextServerInfo(ctx, &req.Server, req.Repository.GetStorageName()); err != nil {
		return fmt.Errorf("server-side restore: %w", err)
	}

	client, err := ss.newRepoClient(ctx, req.Server)
	if err != nil {
		return fmt.Errorf("server-side restore: %w", err)
	}

	_, err = client.RestoreRepository(ctx, &gitalypb.RestoreRepositoryRequest{
		Repository:       req.Repository,
		VanityRepository: req.VanityRepository,
		AlwaysCreate:     req.AlwaysCreate,
		BackupId:         req.BackupID,
		UseLatest:        req.UseLatest,
	})
	if err != nil {
		st := status.Convert(err)
		for _, detail := range st.Details() {
			switch detail.(type) {
			case *gitalypb.RestoreRepositoryResponse_SkippedError:
				return fmt.Errorf("server-side restore: %w: %s", ErrSkipped, err.Error())
			}
		}

		return structerr.New("server-side restore: %w", err)
	}

	return nil
}

// WriteBackupID calls the WriteBackupID RPC. It picks any Gitaly server from
// the context's injected server map — all servers share the same backup sink
// so the choice is arbitrary.
func (ss ServerSideAdapter) WriteBackupID(ctx context.Context, backupID string) error {
	storageName, server, err := anyServerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("server-side write backup id: %w", err)
	}

	client, err := ss.newBackupClient(ctx, server)
	if err != nil {
		return fmt.Errorf("server-side write backup id: %w", err)
	}

	_, err = client.WriteBackupID(ctx, &gitalypb.WriteBackupIDRequest{
		StorageName: storageName,
		BackupId:    backupID,
	})
	if err != nil {
		return structerr.New("server-side write backup id: %w", err)
	}

	return nil
}

// ReadLatestBackupID calls the ReadLatestBackupID RPC. It picks any Gitaly
// server from the context's injected server map.
func (ss ServerSideAdapter) ReadLatestBackupID(ctx context.Context) (string, error) {
	storageName, server, err := anyServerFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("server-side read latest backup id: %w", err)
	}

	client, err := ss.newBackupClient(ctx, server)
	if err != nil {
		return "", fmt.Errorf("server-side read latest backup id: %w", err)
	}

	response, err := client.ReadLatestBackupID(ctx, &gitalypb.ReadLatestBackupIDRequest{
		StorageName: storageName,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", ErrDoesntExist
		}
		return "", structerr.New("server-side read latest backup id: %w", err)
	}

	return response.GetBackupId(), nil
}

// anyServerFromContext extracts any Gitaly server from the context's injected
// server map. Since WriteBackupID and ReadLatestBackupID operate on the shared
// backup sink, any server in the deployment can handle them.
func anyServerFromContext(ctx context.Context) (storageName string, server storage.ServerInfo, err error) {
	servers, err := storage.ExtractGitalyServers(ctx)
	if err != nil {
		return "", storage.ServerInfo{}, fmt.Errorf("extract gitaly servers: %w", err)
	}

	for name, srv := range servers {
		return name, srv, nil
	}

	return "", storage.ServerInfo{}, fmt.Errorf("no gitaly servers found in context")
}

func (ss ServerSideAdapter) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := ss.pool.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}

func (ss ServerSideAdapter) newBackupClient(ctx context.Context, server storage.ServerInfo) (gitalypb.BackupServiceClient, error) {
	conn, err := ss.pool.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewBackupServiceClient(conn), nil
}
