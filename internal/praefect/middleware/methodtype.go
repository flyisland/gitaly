package middleware

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/metrics"
	"google.golang.org/grpc"
)

// MethodTypeUnaryInterceptor returns a Unary Interceptor that records the method type of incoming RPC requests
func MethodTypeUnaryInterceptor(r *protoregistry.Registry, logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		observeMethodType(r, logger, info.FullMethod)

		res, err := handler(ctx, req)

		return res, err
	}
}

// MethodTypeStreamInterceptor returns a Stream Interceptor that records the method type of incoming RPC requests
func MethodTypeStreamInterceptor(r *protoregistry.Registry, logger log.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		observeMethodType(r, logger, info.FullMethod)

		err := handler(srv, stream)

		return err
	}
}

func observeMethodType(registry *protoregistry.Registry, logger log.Logger, fullMethod string) {
	if registry.IsInterceptedMethod(fullMethod) {
		return
	}

	mi, err := registry.LookupMethod(fullMethod)
	if err != nil {
		logger.WithField("full_method_name", fullMethod).WithError(err).Debug("error when looking up method info")
	}

	var opType string
	switch mi.Operation {
	case protoregistry.OpAccessor:
		opType = "accessor"
	case protoregistry.OpMutator:
		opType = "mutator"
	case protoregistry.OpMaintenance:
		opType = "maintenance"
	default:
		return
	}

	metrics.MethodTypeCounter.WithLabelValues(opType).Inc()
}
