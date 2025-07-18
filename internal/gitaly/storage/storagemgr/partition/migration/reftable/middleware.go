package reftable

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/middleware"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type migrationRegister interface {
	RegisterMigration(storageName, relativePath string)
	CancelMigration(storageName, relativePath string)
}

// NewUnaryInterceptor is an oppurtunistic middleware to aid in reftable migration.
// It only registers a migration for an incoming ACCESSOR request. If any other
// type of request is received, it tries to cancel the migration if any exist
// and are ongoing.
func NewUnaryInterceptor(logger log.Logger, registry *protoregistry.Registry, register migrationRegister) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, returnedErr error) {
		tx := storage.ExtractTransaction(ctx)

		if tx != nil && featureflag.ReftableMigration.IsEnabled(ctx) {
			methodInfo, err := registry.LookupMethod(info.FullMethod)
			if err != nil {
				return nil, fmt.Errorf("lookup method: %w", err)
			}

			targetRepo, err := methodInfo.TargetRepo(req.(proto.Message))
			if err != nil {
				return nil, fmt.Errorf("extract repository: %w", err)
			}

			targetRepo = tx.OriginalRepository(targetRepo)

			switch methodInfo.Operation {
			case protoregistry.OpAccessor:
				register.RegisterMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
			case protoregistry.OpMutator:
				defer register.RegisterMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
				fallthrough
			default:
				// Cancel any ongoing migrations to avoid conflicts
				// but schedule one to start after we serve the request
				register.CancelMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
			}
		}

		return handler(ctx, req)
	}
}

// NewStreamInterceptor is an oppurtunistic middleware to aid in reftable migration.
// It only registers a migration for an incoming ACCESSOR request. If any other
// type of request is received, it tries to cancel the migration if any exist
// and are ongoing.
//
// For streaming RPCs we consume the first request and wrap it again before calling
// the next handler. This ensures that the next request doesn't miss the first message.
func NewStreamInterceptor(logger log.Logger, registry *protoregistry.Registry, register migrationRegister) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (returnedErr error) {
		tx := storage.ExtractTransaction(ss.Context())

		if tx != nil && featureflag.ReftableMigration.IsEnabled(ss.Context()) {
			methodInfo, err := registry.LookupMethod(info.FullMethod)
			if err != nil {
				return fmt.Errorf("lookup method: %w", err)
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

			targetRepo, err := methodInfo.TargetRepo(req)
			if err != nil {
				return fmt.Errorf("extract repository: %w", err)
			}

			targetRepo = tx.OriginalRepository(targetRepo)

			switch methodInfo.Operation {
			case protoregistry.OpAccessor:
				register.RegisterMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
			case protoregistry.OpMutator:
				defer register.RegisterMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
				fallthrough
			default:
				// Cancel any ongoing migrations to avoid conflicts
				// but schedule one to start after we serve the request
				register.CancelMigration(targetRepo.GetStorageName(), targetRepo.GetRelativePath())
			}

			return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
		}

		return handler(srv, ss)
	}
}
