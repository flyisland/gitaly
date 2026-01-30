package gitaly

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const (
	flagGetPartitionConfig       = "config"
	flagGetPartitionPartitionKey = "partition-key"
	flagGetPartitionRelativePath = "relative-path"
	flagGetPartitionNoColor      = "no-color"
	flagGetPartitionFormat       = "format"
)

func newClusterGetPartitionCommand() *cli.Command {
	return &cli.Command{
		Name:  "get-partition",
		Usage: "display detailed partition information",
		UsageText: `gitaly cluster get-partition --config <gitaly_config_file> [--partition-key <key>] [--relative-path <path>] [--format <text|json>]

Examples:
  # Get detailed info for a specific partition by key (64-character SHA256 hex)
  gitaly cluster get-partition --config config.toml --partition-key abc123...

  # Get partition info for a specific repository path
  gitaly cluster get-partition --config config.toml --relative-path @hashed/ab/cd/abcd...

  # Output as JSON for programmatic consumption
  gitaly cluster get-partition --config config.toml --partition-key abc123... --format json`,
		Description: `Display detailed information about specific partitions including:
  - Partition key and replica topology
  - Leader/follower status for each replica
  - Health status of replicas (checks if address is configured, not actual reachability)
  - List of repositories in the partition

Use --partition-key to filter by a specific partition key, or --relative-path to find
the partition containing a specific repository. When using --relative-path, the output
shows the partition that contains the specified repository.

Output formats:
  - text (default): Human-readable colored tables and detailed information
  - json: Machine-readable JSON for automation and scripting`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     flagGetPartitionConfig,
				Usage:    "path to Gitaly configuration file",
				Aliases:  []string{"c"},
				Required: true,
			},
			&cli.StringFlag{
				Name:  flagGetPartitionPartitionKey,
				Usage: "filter by specific partition key",
			},
			&cli.StringFlag{
				Name:  flagGetPartitionRelativePath,
				Usage: "show partition info for a specific repository path",
			},
			&cli.BoolFlag{
				Name:  flagGetPartitionNoColor,
				Usage: "disable colored output",
			},
			&cli.StringFlag{
				Name:  flagGetPartitionFormat,
				Usage: "output format: 'text' (default) or 'json'",
				Value: "text",
			},
		},
		Action: getPartitionAction,
	}
}

func getPartitionAction(ctx context.Context, cmd *cli.Command) error {
	configPath := cmd.String(flagGetPartitionConfig)

	// Get filter flags
	partitionKey := cmd.String(flagGetPartitionPartitionKey)
	relativePath := cmd.String(flagGetPartitionRelativePath)
	noColor := cmd.Bool(flagGetPartitionNoColor)
	format := cmd.String(flagGetPartitionFormat)

	// Validate format
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid format %q: must be 'text' or 'json'", format)
	}

	// Validate that at least one filter is provided
	if partitionKey == "" && relativePath == "" {
		return errors.New("either --partition-key or --relative-path must be provided")
	}

	// Validate that only one filter is provided
	if partitionKey != "" && relativePath != "" {
		return errors.New("--partition-key and --relative-path cannot be used together")
	}

	// Validate partition key format if provided
	if err := validatePartitionKey(partitionKey); err != nil {
		return err
	}

	// Create Raft client using shared helper
	raftClient, cleanup, err := loadConfigAndCreateRaftClient(ctx, configPath)
	if err != nil {
		return err
	}
	defer cleanup()

	// Fetch partition data
	partitions, err := fetchPartitionData(ctx, raftClient, partitionKey, relativePath)
	if err != nil {
		return err
	}

	// Prepare output structure
	output := &PartitionDetailsOutput{
		Partitions: partitions,
	}

	// Output based on format
	if format == "json" {
		return output.ToJSON(cmd.Writer)
	}

	// Configure color output for text format
	colorOutput := setupColorOutput(cmd.Writer, noColor)
	return output.ToText(cmd.Writer, colorOutput, partitionKey, relativePath)
}

// fetchPartitionData retrieves partition details from the Raft service
func fetchPartitionData(ctx context.Context, client gitalypb.RaftServiceClient, partitionKey, relativePath string) ([]*gitalypb.GetPartitionsResponse, error) {
	// Get partition details using GetPartitions RPC
	partitionsReq := &gitalypb.GetPartitionsRequest{
		IncludeRelativePaths:  true,
		IncludeReplicaDetails: true,
	}

	// Set filtering based on CLI flags
	if partitionKey != "" {
		partitionsReq.PartitionKey = &gitalypb.RaftPartitionKey{Value: partitionKey}
	}
	if relativePath != "" {
		partitionsReq.RelativePath = relativePath
	}

	partitionsStream, err := client.GetPartitions(ctx, partitionsReq)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve partition information - verify server is running and Raft is enabled: %w", err)
	}

	// Collect all partition responses using helper function
	partitionResponses, err := collectPartitionResponses(partitionsStream)
	if err != nil {
		return nil, err
	}

	return partitionResponses, nil
}
