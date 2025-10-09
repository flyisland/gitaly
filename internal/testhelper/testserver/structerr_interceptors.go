package testserver

import (
	"context"
	"errors"
	"sort"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// StructErrUnaryInterceptor is an interceptor for unary RPC calls that injects error metadata as detailed
// error. This is only supposed to be used for testing purposes as error metadata is considered to
// be a server-side detail. No clients should start to rely on it.
func StructErrUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	response, err := handler(ctx, req)
	return response, interceptedError(err)
}

// StructErrStreamInterceptor is an interceptor for streaming RPC calls that injects error metadata as
// detailed error. This is only supposed to be used for testing purposes as error metadata is
// considered to be a server-side detail. No clients should start to rely on it.
func StructErrStreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return interceptedError(handler(srv, stream))
}

func interceptedError(err error) error {
	if metadata := structerr.ExtractMetadata(err); len(metadata) > 0 {
		keys := make([]string, 0, len(metadata))
		for key := range metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		var structErr structerr.Error
		var existingDetails []proto.Message

		code := codes.Unknown
		if errors.As(err, &structErr) {
			code = structErr.Code()
			existingDetails = structErr.Details()
		}

		// This keeps the top-level error context if it was wrapped
		newErr := structerr.New("%s", err.Error()).WithGRPCCode(code)

		for _, detail := range existingDetails {
			newErr = newErr.WithDetail(detail)
		}

		for _, key := range keys {
			newErr = testhelper.WithInterceptedMetadata(newErr, key, metadata[key])
		}

		return newErr
	}

	return err
}
