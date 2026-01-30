package gitaly

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const (
	flagClusterInfoConfig         = "config"
	flagClusterInfoStorage        = "storage"
	flagClusterInfoListPartitions = "list-partitions"
	flagClusterInfoNoColor        = "no-color"
	flagClusterInfoFormat         = "format"
)

func newClusterInfoCommand() *cli.Command {
	return &cli.Command{
		Name:  "info",
		Usage: "display cluster statistics and overview",
		UsageText: `gitaly cluster info --config <gitaly_config_file> [--list-partitions] [--storage <name>] [--format <text|json>]

Examples:
  # Show cluster statistics only (default)
  gitaly cluster info --config config.toml

  # Show cluster statistics with partition overview
  gitaly cluster info --config config.toml --list-partitions

  # Filter by storage (shows partition overview for that storage)
  gitaly cluster info --config config.toml --storage storage-1 --list-partitions

  # Output as JSON for programmatic consumption
  gitaly cluster info --config config.toml --format json

  # Output JSON with partition details
  gitaly cluster info --config config.toml --format json --list-partitions`,
		Description: `Display cluster-wide information including:
  - Cluster statistics (total partitions, replicas, health) - shown by default
  - Per-storage statistics (leader and replica counts)
  - Partition overview table (use --list-partitions)

By default, only cluster statistics are displayed. Use --list-partitions to see
a partition overview table. Use --storage to filter partitions by storage.

Output formats:
  - text (default): Human-readable colored tables and statistics
  - json: Machine-readable JSON for automation and scripting

For detailed partition information, use 'gitaly cluster get-partition'.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     flagClusterInfoConfig,
				Usage:    "path to Gitaly configuration file",
				Aliases:  []string{"c"},
				Required: true,
			},
			&cli.StringFlag{
				Name:  flagClusterInfoStorage,
				Usage: "filter by storage name (show partitions on this storage only)",
			},
			&cli.BoolFlag{
				Name:  flagClusterInfoListPartitions,
				Usage: "display partition overview table (default: only show cluster statistics)",
			},
			&cli.BoolFlag{
				Name:  flagClusterInfoNoColor,
				Usage: "disable colored output",
			},
			&cli.StringFlag{
				Name:  flagClusterInfoFormat,
				Usage: "output format: 'text' (default) or 'json'",
				Value: "text",
			},
		},
		Action: clusterInfoAction,
	}
}

func clusterInfoAction(ctx context.Context, cmd *cli.Command) error {
	configPath := cmd.String(flagClusterInfoConfig)

	// Get filter flags
	storage := cmd.String(flagClusterInfoStorage)
	listPartitions := cmd.Bool(flagClusterInfoListPartitions)
	noColor := cmd.Bool(flagClusterInfoNoColor)
	format := cmd.String(flagClusterInfoFormat)

	// Validate format
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid format %q: must be 'text' or 'json'", format)
	}

	// Create Raft client using shared helper
	raftClient, cleanup, err := loadConfigAndCreateRaftClient(ctx, configPath)
	if err != nil {
		return err
	}
	defer cleanup()

	// Fetch cluster data
	clusterInfoResp, partitions, err := fetchClusterData(ctx, raftClient, storage, listPartitions)
	if err != nil {
		return err
	}

	// Prepare output structure
	output := &ClusterInfoOutput{
		ClusterInfo: clusterInfoResp,
		Partitions:  partitions,
	}

	// Output based on format
	if format == "json" {
		return output.ToJSON(cmd.Writer)
	}

	// Configure color output for text format
	colorOutput := setupColorOutput(cmd.Writer, noColor)
	return output.ToText(cmd.Writer, colorOutput, storage, listPartitions)
}

// fetchClusterData retrieves cluster information and optionally partition details from the Raft service
func fetchClusterData(ctx context.Context, client gitalypb.RaftServiceClient, storage string, listPartitions bool) (*gitalypb.RaftClusterInfoResponse, []*gitalypb.GetPartitionsResponse, error) {
	// Step 1: Always get cluster statistics using GetClusterInfo RPC
	var clusterInfoReq *gitalypb.RaftClusterInfoRequest
	clusterInfoResp, err := client.GetClusterInfo(ctx, clusterInfoReq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve cluster information - verify server is running and Raft is enabled: %w", err)
	}

	// Step 2: Only get partition details if needed (when --list-partitions is set or --storage filter is provided)
	var partitionResponses []*gitalypb.GetPartitionsResponse
	needPartitions := listPartitions || storage != ""

	if needPartitions {
		partitionsReq := &gitalypb.GetPartitionsRequest{
			// Include relative paths to display repository counts in partition overview table
			// TODO: Optimize this to fetch only counts instead of all paths for better performance
			IncludeRelativePaths:  true,
			IncludeReplicaDetails: true,
		}

		// Set storage filter if provided
		if storage != "" {
			partitionsReq.Storage = storage
		}

		partitionsStream, err := client.GetPartitions(ctx, partitionsReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to retrieve partition details - verify server is running and storage %q exists: %w", storage, err)
		}

		// Collect all partition responses using helper function
		partitionResponses, err = collectPartitionResponses(partitionsStream)
		if err != nil {
			return nil, nil, err
		}
	}

	return clusterInfoResp, partitionResponses, nil
}

// colorOutput holds color configuration for terminal output
type colorOutput struct {
	enabled bool
	green   *color.Color
	red     *color.Color
	yellow  *color.Color
	blue    *color.Color
	cyan    *color.Color
	bold    *color.Color
}

// setupColorOutput configures color output based on terminal capabilities and user preferences
func setupColorOutput(writer io.Writer, noColor bool) *colorOutput {
	// Determine if we should use color
	shouldUseColor := !noColor && supportsColor(writer)

	co := &colorOutput{
		enabled: shouldUseColor,
		green:   color.New(color.FgGreen),
		red:     color.New(color.FgRed),
		yellow:  color.New(color.FgYellow),
		blue:    color.New(color.FgBlue),
		cyan:    color.New(color.FgCyan),
		bold:    color.New(color.Bold),
	}

	// Disable color if requested or not supported
	if !shouldUseColor {
		color.NoColor = true
	}

	return co
}

// supportsColor checks if the writer supports color output
func supportsColor(writer io.Writer) bool {
	// Check if it's a file (like os.Stdout/os.Stderr)
	if f, ok := writer.(*os.File); ok {
		return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return false
}

// formatHealthy returns colored text for healthy status
func (co *colorOutput) formatHealthy(text string) string {
	if co.enabled {
		return co.green.Sprint(text)
	}
	return text
}

// formatUnhealthy returns colored text for unhealthy status
func (co *colorOutput) formatUnhealthy(text string) string {
	if co.enabled {
		return co.red.Sprint(text)
	}
	return text
}

// formatWarning returns colored text for warning status
func (co *colorOutput) formatWarning(text string) string {
	if co.enabled {
		return co.yellow.Sprint(text)
	}
	return text
}

// formatHeader returns colored text for headers
func (co *colorOutput) formatHeader(text string) string {
	if co.enabled {
		return co.bold.Sprint(text)
	}
	return text
}

// formatInfo returns colored text for informational content
func (co *colorOutput) formatInfo(text string) string {
	if co.enabled {
		return co.cyan.Sprint(text)
	}
	return text
}

// formatHealthStatus returns a colored health indicator with symbol
func (co *colorOutput) formatHealthStatus(healthy, total int) string {
	if healthy == total && total > 0 {
		symbol := "✓"
		status := fmt.Sprintf("%s Healthy (%d/%d)", symbol, healthy, total)
		return co.formatHealthy(status)
	} else if healthy == 0 {
		symbol := "✗"
		status := fmt.Sprintf("%s Unhealthy (%d/%d)", symbol, healthy, total)
		return co.formatUnhealthy(status)
	}
	symbol := "⚠"
	status := fmt.Sprintf("%s Degraded (%d/%d)", symbol, healthy, total)
	return co.formatWarning(status)
}
