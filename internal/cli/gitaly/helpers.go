package gitaly

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gitlab.com/gitlab-org/gitaly/v18/internal/cli"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const (
	// defaultConnectionTimeout is the default timeout for establishing gRPC connections to Gitaly
	defaultConnectionTimeout = 30 * time.Second
)

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
	conn, err := cli.Dial(ctx, address, cfg.Auth.GetToken(), defaultConnectionTimeout)
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
