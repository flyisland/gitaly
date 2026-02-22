package server

import (
	"crypto/tls"
	"fmt"
	"time"

	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/grpcstats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/customfieldshandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/loghandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/requestinfohandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/statushandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	gitalylog "gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	expcredentials "google.golang.org/grpc/experimental/credentials"
	"google.golang.org/grpc/keepalive"
)

type serverConfig struct {
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
}

// Option is an option that can be passed to `New()`.
type Option func(*serverConfig)

// WithUnaryInterceptor adds another interceptor that shall be executed for unary RPC calls.
func WithUnaryInterceptor(interceptor grpc.UnaryServerInterceptor) Option {
	return func(cfg *serverConfig) {
		cfg.unaryInterceptors = append(cfg.unaryInterceptors, interceptor)
	}
}

// WithStreamInterceptor adds another interceptor that shall be executed for streaming RPC calls.
func WithStreamInterceptor(interceptor grpc.StreamServerInterceptor) Option {
	return func(cfg *serverConfig) {
		cfg.streamInterceptors = append(cfg.streamInterceptors, interceptor)
	}
}

// The default CodeToLevel function is at github.com/grpc-ecosystem/go-grpc-middleware/blob/v2.1.0/interceptors/logging/options.go
var levelFunc = func(code codes.Code) logrus.Level {
	switch code {
	case codes.OK, codes.NotFound, codes.Canceled, codes.AlreadyExists,
		codes.InvalidArgument, codes.Unauthenticated:
		return logrus.InfoLevel
	case codes.DeadlineExceeded, codes.PermissionDenied,
		codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.ResourceExhausted:
		return logrus.WarnLevel
	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss,
		codes.Unavailable:
		return logrus.ErrorLevel
	default:
		return logrus.ErrorLevel
	}
}

// New returns a GRPC server instance with a set of interceptors configured.
func (s *GitalyServerFactory) New(external, secure bool, opts ...Option) (*grpc.Server, error) {
	var cfg serverConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	transportCredentials := insecure.NewCredentials()
	// If tls config is specified attempt to extract tls options and use it
	// as a grpc.ServerOption
	if secure {
		cert, err := s.cfg.TLS.Certificate()
		if err != nil {
			return nil, fmt.Errorf("error reading certificate and key paths: %w", err)
		}

		// The Go language maintains a list of cipher suites that do not have known security issues.
		// This list of cipher suites should be used instead of the default list.
		var secureCiphers []uint16
		for _, cipher := range tls.CipherSuites() {
			secureCiphers = append(secureCiphers, cipher.ID)
		}

		transportCredentials = expcredentials.NewTLSWithALPNDisabled(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   s.cfg.TLS.MinVersion.ProtocolVersion(),
			CipherSuites: secureCiphers,
		})
	}

	var backchannelOptions []backchannel.ServerHandshakerOption
	if s.cfg.UseLibP2P() {
		backchannelOptions = append(backchannelOptions, backchannel.WithLibp2pYamux())
	}

	lm := listenmux.New(transportCredentials)
	lm.Register(backchannel.NewServerHandshaker(
		s.logger,
		s.registry,
		[]grpc.DialOption{client.UnaryInterceptor()},
		backchannelOptions...,
	))

	logMsgProducer := grpcmwlogrus.WithMessageProducer(
		loghandler.MessageProducer(
			loghandler.PropagationMessageProducer(grpcmwlogrus.DefaultMessageProducer),
			customfieldshandler.FieldsProducer,
			grpcstats.FieldsProducer,
			featureflag.FieldsProducer,
			structerr.FieldsProducer,
		),
	)

	logMatcher := gitalylog.NewLogMatcher()

	streamServerInterceptors := []grpc.StreamServerInterceptor{
		grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
		requestinfohandler.StreamInterceptor,
		grpcprometheus.StreamServerInterceptor,
		customfieldshandler.StreamInterceptor,
		selector.StreamServerInterceptor(s.logger.WithField("component", "gitaly.StreamServerInterceptor").StreamServerInterceptor(
			grpcmwlogrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
			logMsgProducer,
			grpcmwlogrus.WithLevels(levelFunc),
		), logMatcher),
		loghandler.StreamLogDataCatcherServerInterceptor(),
		sentryhandler.StreamLogHandler(),
		statushandler.AbortedErrorStreamInterceptor,
		statushandler.Stream, // Should be below LogHandler and above AbortedInterceptor in case this returns Aborted in the future
		auth.StreamServerInterceptor(s.cfg.Auth),
	}
	unaryServerInterceptors := []grpc.UnaryServerInterceptor{
		grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
		requestinfohandler.UnaryInterceptor,
		grpcprometheus.UnaryServerInterceptor,
		customfieldshandler.UnaryInterceptor,
		selector.UnaryServerInterceptor(s.logger.WithField("component", "gitaly.UnaryServerInterceptor").UnaryServerInterceptor(
			grpcmwlogrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
			logMsgProducer,
			grpcmwlogrus.WithLevels(levelFunc),
		), logMatcher),
		loghandler.UnaryLogDataCatcherServerInterceptor(),
		sentryhandler.UnaryLogHandler(),
		statushandler.AbortedErrorUnaryInterceptor,
		statushandler.Unary, // Should be below LogHandler and above AbortedInterceptor in case this returns Aborted in the future
		auth.UnaryServerInterceptor(s.cfg.Auth),
	}
	// Should be below auth handler to prevent v2 hmac tokens from timing out while queued
	for _, limitHandler := range s.limitHandlers {
		streamServerInterceptors = append(streamServerInterceptors, limitHandler.StreamInterceptor())
		unaryServerInterceptors = append(unaryServerInterceptors, limitHandler.UnaryInterceptor())
	}

	streamServerInterceptors = append(streamServerInterceptors,
		cache.StreamInvalidator(s.cacheInvalidator, protoregistry.GitalyProtoPreregistered, s.logger),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.StreamPanicHandler(s.logger),
	)

	unaryServerInterceptors = append(unaryServerInterceptors,
		cache.UnaryInvalidator(s.cacheInvalidator, protoregistry.GitalyProtoPreregistered, s.logger),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.UnaryPanicHandler(s.logger),
	)

	streamServerInterceptors = append(streamServerInterceptors, cfg.streamInterceptors...)
	unaryServerInterceptors = append(unaryServerInterceptors, cfg.unaryInterceptors...)

	// Only requests coming through the external API need to be ran transactionalized. Only the HookService calls
	// should arrive through the internal socket. Requests coming from there would already be running in a
	// transaction as the external request that led to the internal socket call would have been transactionalized
	// already.
	if external {
		// When transactions are enabled, it overrides the relative path of the repository to point to the
		// snapshot directory. Which would make the housekeeping related caches unusable. We should use the
		// original relative path when transaction is enabled, but when request is routed through hook back
		// to gitaly, the original repository is not in the context anymore. Therefore, housekeeping should
		// not be configured in the internal gRPC server used for hooks
		if s.housekeepingMiddleware != nil {
			streamServerInterceptors = append(streamServerInterceptors, s.housekeepingMiddleware.StreamServerInterceptor())
			unaryServerInterceptors = append(unaryServerInterceptors, s.housekeepingMiddleware.UnaryServerInterceptor())
		}

		if len(s.txMiddleware.UnaryInterceptors) > 0 {
			unaryServerInterceptors = append(unaryServerInterceptors, s.txMiddleware.UnaryInterceptors...)
		}
		if len(s.txMiddleware.StreamInterceptors) > 0 {
			streamServerInterceptors = append(streamServerInterceptors, s.txMiddleware.StreamInterceptors...)
		}
	}

	serverOptions := []grpc.ServerOption{
		grpc.StatsHandler(tracing.NewGRPCServerStatsHandler(
			otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
		)),
		grpc.StatsHandler(loghandler.PerRPCLogHandler{
			Underlying:     &grpcstats.PayloadBytes{},
			FieldProducers: []loghandler.FieldsProducer{grpcstats.FieldsProducer},
		}),
		grpc.Creds(lm),
		grpc.ChainStreamInterceptor(streamServerInterceptors...),
		grpc.ChainUnaryInterceptor(unaryServerInterceptors...),
		// We deliberately set the server MinTime to significantly less than the client interval of 20
		// seconds to allow for network jitter. We can afford to be forgiving as the maximum number of
		// concurrent clients for a Gitaly server is typically in the hundreds and this volume of
		// keepalives won't add significant load.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
		grpc.WaitForHandlers(true),
	}

	return grpc.NewServer(serverOptions...), nil
}
