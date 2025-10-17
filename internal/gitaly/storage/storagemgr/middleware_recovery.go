package storagemgr

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/middleware"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// MayHavePendingWAL checks whether transactions have been enabled in the past on the
// storages by checking whether the database directory exists. If so, the repositories
// in the storage may be incomplete if they have pending WAL entries.
func MayHavePendingWAL(storagePaths []string) (bool, error) {
	for _, storagePath := range storagePaths {
		if _, err := os.Stat(databasemgr.DatabaseDirectoryPath(storagePath)); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Database didn't exist.
				continue
			}

			return false, fmt.Errorf("stat: %w", err)
		}

		return true, nil
	}

	return false, nil
}

// TransactionRecoveryMiddleware is a middleware that is used when transactions are disabled after being enabled. Interrupted WAL
// application can leave repositories in a corrupted state, for example if the old 'packed-refs' file was removed but the new one
// wasn't applied before Gitaly failed. If transactions are then disabled, the repository will be left in corrupted state as the WAL
// application won't be finished.
//
// This middleware ensures that if transactions are disabled, any already committed WAL entries will be applied before further
// access to the repositories is allowed. This ensures repositories that were in the middle of WAL application are made whole.
// If a WAL entry can't be applied, the repositories on that partition will remain unavailable until the WAL entry can be
// successfully applied. After the committed WAL entries were successfully applied, the access to the repositories will continue
// without transactions.
type TransactionRecoveryMiddleware struct {
	registry        *protoregistry.Registry
	node            storage.Node
	readyPartitions *sync.Map
}

// NewTransactionRecoveryMiddleware returns a new TransactionRecoveryMiddleware.
func NewTransactionRecoveryMiddleware(registry *protoregistry.Registry, node storage.Node) *TransactionRecoveryMiddleware {
	return &TransactionRecoveryMiddleware{
		registry:        registry,
		node:            node,
		readyPartitions: &sync.Map{},
	}
}

// UnaryServerInterceptor returns a unary interceptor for the middleware.
func (mw *TransactionRecoveryMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if _, ok := NonTransactionalRPCs[info.FullMethod]; ok {
			// Non-transactional RPCs do not target repositories, and don't have a target repository
			// that could have pending WAL entries.
			return handler(ctx, req)
		}

		methodInfo, err := mw.registry.LookupMethod(info.FullMethod)
		if err != nil {
			return nil, fmt.Errorf("lookup method: %w", err)
		}

		if err := mw.applyPendingWAL(ctx, methodInfo, req.(proto.Message)); err != nil {
			return nil, fmt.Errorf("apply pending WAL: %w", err)
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a stream interceptor for the middleware.
func (mw *TransactionRecoveryMiddleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		if _, ok := NonTransactionalRPCs[info.FullMethod]; ok {
			// Non-transactional RPCs do not target repositories, and don't have a target repository
			// that could have pending WAL entries.
			return handler(ctx, ss)
		}

		methodInfo, err := mw.registry.LookupMethod(info.FullMethod)
		if err != nil {
			return fmt.Errorf("lookup method: %w", err)
		}

		req := methodInfo.NewRequest()
		if err := ss.RecvMsg(req); err != nil {
			// All of the repository scoped streaming RPCs send the repository in the first message.
			// If we fail to read the first message, we'll just let the handler handle it.
			return handler(srv, middleware.NewPeekedStream(ss.Context(), nil, err, ss))
		}

		if err := mw.applyPendingWAL(ctx, methodInfo, req); err != nil {
			return fmt.Errorf("apply pending WAL: %w", err)
		}

		return handler(srv, middleware.NewPeekedStream(ctx, req, nil, ss))
	}
}

// applyPendingWAL starts a transaction against the target repository's partition and aborts it. If the transaction begins
// successfully, it's guaranteed that all pending WAL entries in the partition have been applied.
func (mw *TransactionRecoveryMiddleware) applyPendingWAL(ctx context.Context, methodInfo protoregistry.MethodInfo, req proto.Message) error {
	if methodInfo.Scope != protoregistry.ScopeRepository {
		// Only repository scoped RPCs may target repositories with pending WAL entries.
		return nil
	}

	targetRepo, err := methodInfo.TargetRepo(req)
	if err != nil {
		if errors.Is(err, protoregistry.ErrRepositoryFieldNotFound) {
			// If the repository field was not set, it can't target a repository that has pending WAL entries
			// Let the handler handle the situation.
			return nil
		}

		return fmt.Errorf("target repo: %w", err)
	}

	storageHandle, err := mw.node.GetStorage(targetRepo.GetStorageName())
	if err != nil {
		if errors.Is(err, storage.ErrStorageNotFound) {
			// This request was for a storage that isn't configured, and wouldn't thus target a repository
			// with a pending WAL entry.
			return nil
		}

		return fmt.Errorf("get storage: %w", err)
	}

	ptnID, err := storageHandle.GetAssignedPartitionID(targetRepo.GetRelativePath())
	if err != nil {
		if errors.Is(err, storage.ErrPartitionAssignmentNotFound) {
			// The repository wasn't yet assigned to a partition. It thus hasn't been accessed with transactions
			// and can't have pending WAL entries.
			return nil
		}

		return fmt.Errorf("get partition: %w", err)
	}

	key := fmt.Sprintf("%s:%d", targetRepo.GetStorageName(), ptnID)
	if _, ok := mw.readyPartitions.Load(key); ok {
		// The partition has already been checked and all pending WAL entries applied.
		return nil
	}

	if hasPendingWAL, err := storageHandle.HasPendingWAL(ctx, ptnID); err != nil {
		return fmt.Errorf("check WAL entries: %w", err)
	} else if !hasPendingWAL {
		// No WAL entries found, mark the partition as ready without creating a transaction
		mw.readyPartitions.Store(key, struct{}{})
		return nil
	}

	// Start a transaction against the repository. The partition's WAL is applied before beginning
	// transactions which ensures the WAL is fully applied.
	tx, err := storageHandle.Begin(ctx, storage.TransactionOptions{
		ReadOnly:               true,
		RelativePath:           targetRepo.GetRelativePath(),
		ForceExclusiveSnapshot: true,
	})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	mw.readyPartitions.Store(key, struct{}{})

	if err := tx.Rollback(ctx); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	return nil
}
