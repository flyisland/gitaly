package gitcmd

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/middleware/requestinfohandler"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	// ProtocolV1 is the special value used by Git clients to request protocol v1
	ProtocolV1 = "version=1"
	// ProtocolV2 is the special value used by Git clients to request protocol v2
	ProtocolV2 = "version=2"
)

var gitProtocolRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_git_protocol_requests_total",
		Help: "Counter of Git protocol requests",
	},
	[]string{"grpc_service", "grpc_method", "git_protocol"},
)

// RequestWithGitProtocol holds requests that respond to GitProtocol
type RequestWithGitProtocol interface {
	GetGitProtocol() string
}

// WithGitProtocol checks whether the request has Git protocol v2
// and sets this in the environment.
func WithGitProtocol(logger log.Logger, req RequestWithGitProtocol) CmdOpt {
	return func(ctx context.Context, _ config.Cfg, _ CommandFactory, cc *cmdCfg) error {
		cc.env = append(cc.env, gitProtocolEnv(ctx, logger, req)...)
		return nil
	}
}

func gitProtocolEnv(ctx context.Context, logger log.Logger, req RequestWithGitProtocol) []string {
	var protocol string
	var env []string

	switch gp := req.GetGitProtocol(); gp {
	case ProtocolV2:
		env = append(env, fmt.Sprintf("GIT_PROTOCOL=%s", ProtocolV2))
		protocol = "v2"
	case ProtocolV1:
		env = append(env, fmt.Sprintf("GIT_PROTOCOL=%s", ProtocolV1))
		protocol = "v1"
	case "":
		protocol = "v0"
	default:
		logger.WithField("git_protocol", gp).WarnContext(ctx, "invalid git protocol requested")
		protocol = "invalid"
	}

	service, method := methodFromContext(ctx)
	gitProtocolRequests.WithLabelValues(service, method, protocol).Inc()

	return env
}

func methodFromContext(ctx context.Context) (service string, method string) {
	if info := requestinfohandler.Extract(ctx); info != nil {
		return info.ExtractServiceAndMethodName()
	}

	return "", ""
}
