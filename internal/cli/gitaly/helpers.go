package gitaly

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
)

const (
	// defaultConnectionTimeout is the default timeout for establishing gRPC connections to Gitaly
	defaultConnectionTimeout = 30 * time.Second
)

// dialGitaly creates a gRPC connection to the Gitaly server
func dialGitaly(ctx context.Context, addr, token string, timeout time.Duration, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	opts = append(opts,
		client.UnaryInterceptor(),
		client.StreamInterceptor(),
	)

	if len(token) > 0 {
		opts = append(opts,
			grpc.WithPerRPCCredentials(
				gitalyauth.RPCCredentialsV2(token),
			),
		)
	}

	return client.New(ctx, addr, client.WithGrpcOptions(opts))
}

// loadConfigAndCreateRaftClient loads configuration from the given path and creates a Raft service client
func loadConfigAndCreateRaftClient(ctx context.Context, configPath string) (gitalypb.RaftServiceClient, func(), error) {
	if configPath == "" {
		return nil, nil, errors.New("config file path is required")
	}

	// Load configuration
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("opening config file: %w", err)
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return nil, nil, fmt.Errorf("loading configuration: %w", err)
	}

	// Get Gitaly server address
	address, err := cfg.GetAddressWithScheme()
	if err != nil {
		return nil, nil, fmt.Errorf("determine Gitaly server address: %w", err)
	}

	// Establish gRPC connection
	conn, err := dialGitaly(ctx, address, cfg.Auth.Token, defaultConnectionTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("establish gRPC connection to %s: %w", address, err)
	}

	// Create Raft service client
	raftClient := gitalypb.NewRaftServiceClient(conn)

	// Return client and cleanup function
	cleanup := func() {
		conn.Close()
	}

	return raftClient, cleanup, nil
}
