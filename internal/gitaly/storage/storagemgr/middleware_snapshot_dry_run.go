package storagemgr

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/middleware"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// NewDryRunUnaryInterceptor returns a unary interceptor that collects snapshot statistics
// for repository-scoped RPCs without creating actual snapshots. This is used when transactions
// are disabled and the SnapshotDryRunStats feature flag is enabled.
func NewDryRunUnaryInterceptor(logger log.Logger, registry *protoregistry.Registry, locator storage.Locator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, returnedErr error) {
		if !featureflag.SnapshotDryRunStats.IsEnabled(ctx) {
			return handler(ctx, req)
		}

		if err := collectDryRunStatsForRPC(ctx, logger, registry, locator, info.FullMethod, req.(proto.Message)); err != nil {
			logger.WithError(err).Warn("failed to collect dry-run snapshot statistics")
		}

		return handler(ctx, req)
	}
}

// NewDryRunStreamInterceptor returns a stream interceptor that collects snapshot statistics
// for repository-scoped streaming RPCs without creating actual snapshots.
func NewDryRunStreamInterceptor(logger log.Logger, registry *protoregistry.Registry, locator storage.Locator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Only process repository-scoped RPCs when the feature flag is enabled
		ctx := ss.Context()
		if !featureflag.SnapshotDryRunStats.IsEnabled(ctx) {
			return handler(srv, ss)
		}

		methodInfo, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			// If we can't lookup the method, proceed without collecting stats
			return handler(srv, ss)
		}

		req := methodInfo.NewRequest()
		if err := ss.RecvMsg(req); err != nil {
			// All of the repository scoped streaming RPCs send the repository in the first message.
			// Generally it should be fine to error out in all cases if there is no message sent.
			// To maintain compatibility with tests, we instead invoke the handler to let them return
			// the asserted error messages. Once the transaction management is on by default, we should
			// error out here directly and amend the failing test cases.
			return handler(srv, middleware.NewPeekedStream(ss.Context(), nil, err, ss))
		}

		if err := collectDryRunStatsForRPC(ctx, logger, registry, locator, info.FullMethod, req); err != nil {
			logger.WithError(err).Warn("failed to collect dry-run snapshot statistics for streaming RPC")
		}
		// Continue with the original stream, passing the peeked message
		return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
	}
}

// collectDryRunStatsForRPC collects dry-run statistics for a repository-scoped RPC
func collectDryRunStatsForRPC(ctx context.Context, logger log.Logger, registry *protoregistry.Registry, locator storage.Locator, fullMethod string, req proto.Message) (returnErr error) {
	methodInfo, err := registry.LookupMethod(fullMethod)
	if err != nil {
		return fmt.Errorf("lookup method: %w", err)
	}

	// Only collect stats for repository-scoped RPCs
	if methodInfo.Scope != protoregistry.ScopeRepository {
		return nil
	}

	targetRepo, err := methodInfo.TargetRepo(req)
	if err != nil {
		return fmt.Errorf("extract target repository: %w", err)
	}

	storagePath, err := locator.GetStorageByName(ctx, targetRepo.GetStorageName())
	if err != nil {
		return fmt.Errorf("resolve storage path: %w", err)
	}

	// Create a temporary working directory for the snapshot manager
	tempDir, err := os.MkdirTemp("", "gitaly-dry-run-snapshot-stats-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temp dir: %w", err))
		}
	}()

	// Create a minimal snapshot manager for dry-run statistics
	manager, err := snapshot.NewManager(logger, storagePath, tempDir, snapshot.ManagerMetrics{})
	if err != nil {
		return fmt.Errorf("new snapshot manager: %w", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close snapshot manager: %w", err))
		}
	}()

	if err := manager.CollectDryRunStatistics(ctx, []string{targetRepo.GetRelativePath()}); err != nil {
		return fmt.Errorf("collect dry-run statistics: %w", err)
	}

	return nil
}
