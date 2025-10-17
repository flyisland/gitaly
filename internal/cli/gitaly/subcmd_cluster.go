package gitaly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// sha256Pattern is the compiled regex for validating partition keys (SHA256 hex strings)
var sha256Pattern = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

func newClusterCommand() *cli.Command {
	return &cli.Command{
		Name:      "cluster",
		Usage:     "manage Gitaly cluster",
		UsageText: "gitaly cluster command [command options]",
		Description: `The cluster command provides subcommands for managing and inspecting Gitaly clusters.

Use 'gitaly cluster info' to display cluster statistics and overview.
Use 'gitaly cluster get-partition' to display detailed partition information.`,
		Commands: []*cli.Command{
			newClusterInfoCommand(),
			newClusterGetPartitionCommand(),
		},
	}
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

// validatePartitionKey validates that a partition key is in the expected SHA256 hex format
func validatePartitionKey(partitionKey string) error {
	if partitionKey == "" {
		return nil // Empty key is valid, means no filter
	}

	if !sha256Pattern.MatchString(partitionKey) {
		return fmt.Errorf("invalid partition key format: expected 64-character SHA256 hex string, got %q", partitionKey)
	}

	return nil
}
