package gitaly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
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

// getAddressWithScheme returns the appropriate address with scheme based on the configuration.
// This helper function is shared across multiple CLI subcommands.
func getAddressWithScheme(cfg config.Cfg) (string, error) {
	switch {
	case cfg.SocketPath != "":
		return "unix:" + cfg.SocketPath, nil
	case cfg.ListenAddr != "":
		return "tcp://" + cfg.ListenAddr, nil
	case cfg.TLSListenAddr != "":
		return "tls://" + cfg.TLSListenAddr, nil
	default:
		return "", errors.New("no address configured")
	}
}

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

// collectPartitionResponses collects all partition responses from a streaming RPC
func collectPartitionResponses(stream gitalypb.RaftService_GetPartitionsClient) ([]*gitalypb.GetPartitionsResponse, error) {
	var partitionResponses []*gitalypb.GetPartitionsResponse
	for {
		partitionResp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Handle context cancellation and timeout more specifically
			if errors.Is(err, context.Canceled) {
				return nil, fmt.Errorf("operation cancelled while receiving partition data: %w", err)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("operation timed out while receiving partition data: %w", err)
			}
			return nil, fmt.Errorf("failed to receive partition data from stream: %w", err)
		}
		partitionResponses = append(partitionResponses, partitionResp)
	}
	return partitionResponses, nil
}

// sortPartitionsByKey sorts partitions by their partition key for consistent output
func sortPartitionsByKey(partitions []*gitalypb.GetPartitionsResponse) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].GetPartitionKey().GetValue() < partitions[j].GetPartitionKey().GetValue()
	})
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
	address, err := getAddressWithScheme(cfg)
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

// validatePartitionKey validates that a partition key is in the expected SHA256 hex format
func validatePartitionKey(partitionKey string) error {
	if partitionKey == "" {
		return nil // Empty key is valid, means no filter
	}

	// Partition keys should be SHA256 hashes (64 hex characters)
	sha256Pattern := regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	if !sha256Pattern.MatchString(partitionKey) {
		return fmt.Errorf("invalid partition key format: expected 64-character SHA256 hex string, got %q", partitionKey)
	}

	return nil
}
