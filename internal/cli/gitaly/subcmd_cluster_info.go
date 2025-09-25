package gitaly

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
)

const (
	flagClusterInfoConfig       = "config"
	flagClusterInfoPartitionKey = "partition-key"
	flagClusterInfoAuthority    = "authority"
	flagClusterInfoRepository   = "repository"
)

func newClusterInfoCommand() *cli.Command {
	return &cli.Command{
		Name:  "info",
		Usage: "display cluster topology and partition information",
		UsageText: `gitaly cluster info --config <gitaly_config_file> [--partition-key <key>] [--authority <name>] [--repository <path>]

Examples:
  # Show all cluster info with statistics
  gitaly cluster info --config config.toml

  # Filter by specific partition key
  gitaly cluster info --config config.toml --partition-key sha256:abc123...

  # Filter by authority (storage)
  gitaly cluster info --config config.toml --authority storage-1

  # Show partition info for a specific repository
  gitaly cluster info --config config.toml --repository @hashed/ab/cd/abcd...`,
		Description: `Display information about the Gitaly cluster including:
  - Cluster-wide statistics (total partitions, replicas, health)
  - Partition topology with leader/replica status
  - Repository to partition mapping (when using --repository)

Filtering options allow inspection of specific partitions or authorities.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     flagClusterInfoConfig,
				Usage:    "path to Gitaly configuration file",
				Aliases:  []string{"c"},
				Required: true,
			},
			&cli.StringFlag{
				Name:  flagClusterInfoPartitionKey,
				Usage: "filter by specific partition key",
			},
			&cli.StringFlag{
				Name:  flagClusterInfoAuthority,
				Usage: "filter by authority (storage name)",
			},
			&cli.StringFlag{
				Name:  flagClusterInfoRepository,
				Usage: "show partition info for a specific repository path",
			},
		},
		Action: clusterInfoAction,
	}
}

func clusterInfoAction(ctx context.Context, cmd *cli.Command) error {
	configPath := cmd.String(flagClusterInfoConfig)
	if configPath == "" {
		return errors.New("config file path is required")
	}

	// Load configuration
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("opening config file: %w", err)
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Get filter flags
	partitionKey := cmd.String(flagClusterInfoPartitionKey)
	authority := cmd.String(flagClusterInfoAuthority)
	repository := cmd.String(flagClusterInfoRepository)

	// Validate that only compatible filters are used together
	// --partition-key is mutually exclusive with --authority and --repository
	// --authority and --repository can be used together
	if partitionKey != "" && (authority != "" || repository != "") {
		return errors.New("--partition-key cannot be used with --authority or --repository")
	}

	// TODO: Implement the actual cluster info logic
	// This will be implemented later
	_ = cfg

	fmt.Fprintf(cmd.Writer, "Cluster info command (implementation pending)\n")
	fmt.Fprintf(cmd.Writer, "Config: %s\n", configPath)

	if partitionKey != "" {
		fmt.Fprintf(cmd.Writer, "Filtering by partition key: %s\n", partitionKey)
	}
	if authority != "" {
		fmt.Fprintf(cmd.Writer, "Filtering by authority: %s\n", authority)
	}
	if repository != "" {
		fmt.Fprintf(cmd.Writer, "Filtering by repository: %s\n", repository)
	}

	return nil
}
