package burdenmonitor

import (
	"context"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"google.golang.org/grpc"
)

type rpcEntryKey struct{}

// rpcEntryFromContext retrieves the RPCEntry associated with the given context.
func rpcEntryFromContext(ctx context.Context) (*RPCEntry, bool) {
	entry, ok := ctx.Value(rpcEntryKey{}).(*RPCEntry)
	return entry, ok
}

func contextWithRPCEntry(ctx context.Context, entry *RPCEntry) context.Context {
	return context.WithValue(ctx, rpcEntryKey{}, entry)
}

// NotifyCommandStarted registers a new command with the burden monitor if command tracking is enabled.
// This should be called when a command starts execution.
func NotifyCommandStarted(ctx context.Context, pid int, name string, startTime time.Time) {
	entry, ok := rpcEntryFromContext(ctx)
	if !ok {
		return
	}

	entry.RegisterCommand(pid, name, startTime)
}

// NotifyCommandCompleted notifies the burden monitor that a command has completed.
// This should be called when a command finishes execution.
func NotifyCommandCompleted(ctx context.Context, pid int, userTime, systemTime time.Duration) {
	entry, ok := rpcEntryFromContext(ctx)
	if !ok {
		return
	}

	entry.MarkCommandCompleted(pid, userTime, systemTime)
}

// UnaryInterceptor returns a gRPC unary server interceptor that tracks RPC execution.
func (bm *BurdenMonitor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if featureflag.BurdenMonitorTrackCommands.IsEnabled(ctx) {
			mi, err := protoregistry.GitalyProtoPreregistered.LookupMethod(info.FullMethod)
			if err != nil {
				// log a warning but continue request handling as normal
				bm.logger.WithError(err).Warn("burden monitor stream interceptor: unable to lookup method info")
				return handler(ctx, req)
			}

			// Only track accessor method in the burden monitor
			if mi.Operation == protoregistry.OpAccessor {
				ctx, entry := bm.RegisterRPC(ctx, info.FullMethod)

				defer bm.DeregisterRPC(entry.ID)
				return handler(ctx, req)

			}
		}

		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream server interceptor that tracks RPC execution.
func (bm *BurdenMonitor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if featureflag.BurdenMonitorTrackCommands.IsEnabled(stream.Context()) {
			mi, err := protoregistry.GitalyProtoPreregistered.LookupMethod(info.FullMethod)
			if err != nil {
				// log a warning but continue request handling as normal
				bm.logger.WithError(err).Warn("burden monitor stream interceptor: unable to lookup method info")
				return handler(srv, stream)
			}

			// Only track accessor method in the burden monitor
			if mi.Operation == protoregistry.OpAccessor {
				ctx, entry := bm.RegisterRPC(stream.Context(), info.FullMethod)
				defer bm.DeregisterRPC(entry.ID)

				wrapped := grpcmw.WrapServerStream(stream)
				wrapped.WrappedContext = ctx

				return handler(srv, wrapped)
			}
		}

		return handler(srv, stream)
	}
}
