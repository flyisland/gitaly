package statushandler

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// AbortedErrorUnaryInterceptor intercepts unary RPCs to replace technical
// Aborted error messages with user-friendly alternatives.
func AbortedErrorUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, wrapAbortedError(err)
}

// AbortedErrorStreamInterceptor intercepts streaming RPCs to replace technical
// Aborted error messages with user-friendly alternatives.
func AbortedErrorStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	return wrapAbortedError(err)
}

// wrapAbortedError replaces error messages for Aborted status codes with
// a user-friendly alternative. Aborted errors typically indicate conflicts with
// concurrent operations (like housekeeping tasks), but expose confusing internal
// details to users. This function replaces them with a clearer message while
// preserving the original in metadata for debugging.
func wrapAbortedError(err error) error {
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Aborted {
			var structErr structerr.Error
			if errors.As(err, &structErr) {
				newErr := structErr.WithNewMessage("The operation could not be completed. Please try again.").WithMetadata("error_details", st.Message())
				err = newErr
			} else {
				newErr := structerr.NewAborted("The operation could not be completed. Please try again.").WithMetadata("error_details", st.Message())

				for _, detail := range st.Details() {
					if protoDetail, ok := detail.(proto.Message); ok {
						newErr = newErr.WithDetail(protoDetail)
					}
				}
				err = newErr
			}
		}
	}

	return err
}
