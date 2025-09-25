package customfieldshandler

import (
	"context"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"google.golang.org/grpc"
)

// UnaryInterceptor returns a Unary Interceptor that initializes and injects a log.CustomFields object into the context
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = log.InitContextCustomFields(ctx)

	res, err := handler(ctx, req)

	return res, err
}

// StreamInterceptor returns a Stream Interceptor that initializes and injects a log.CustomFields object into the context
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	ctx = log.InitContextCustomFields(ctx)

	wrapped := grpcmw.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	return err
}

// FieldsProducer extracts custom fields info from the context and returns it as a logging fields.
func FieldsProducer(ctx context.Context, _ error) log.Fields {
	if fields := log.CustomFieldsFromContext(ctx); fields != nil {
		return fields.Fields()
	}
	return nil
}
