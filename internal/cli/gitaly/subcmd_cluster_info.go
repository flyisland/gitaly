package gitaly

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

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
)

func newClusterInfoCommand() *cli.Command {
	return &cli.Command{
		Name:  "info",
		Usage: "display cluster statistics and overview",
		UsageText: `gitaly cluster info --config <gitaly_config_file> [--list-partitions] [--storage <name>]

Examples:
  # Show cluster statistics only (default)
  gitaly cluster info --config config.toml

  # Show cluster statistics with partition overview
  gitaly cluster info --config config.toml --list-partitions

  # Filter by storage (shows partition overview for that storage)
  gitaly cluster info --config config.toml --storage storage-1 --list-partitions`,
		Description: `Display cluster-wide information including:
  - Cluster statistics (total partitions, replicas, health) - shown by default
  - Per-storage statistics (leader and replica counts)
  - Partition overview table (use --list-partitions)

By default, only cluster statistics are displayed. Use --list-partitions to see
a partition overview table. Use --storage to filter partitions by storage.

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

	// Configure color output
	colorOutput := setupColorOutput(cmd.Writer, noColor)

	// Create Raft client using shared helper
	raftClient, cleanup, err := loadConfigAndCreateRaftClient(ctx, configPath)
	if err != nil {
		return err
	}
	defer cleanup()

	// Display cluster info with optimized RPC calls
	return displayClusterInfo(ctx, cmd.Writer, raftClient, storage, listPartitions, colorOutput)
}

// displayClusterInfo calls GetClusterInfo and optionally GetPartitions RPCs, processes the data, and displays the results
func displayClusterInfo(ctx context.Context, writer io.Writer, client gitalypb.RaftServiceClient, storage string, listPartitions bool, colorOutput *colorOutput) error {
	// Step 1: Always get cluster statistics using GetClusterInfo RPC
	var clusterInfoReq *gitalypb.RaftClusterInfoRequest
	clusterInfoResp, err := client.GetClusterInfo(ctx, clusterInfoReq)
	if err != nil {
		return fmt.Errorf("failed to retrieve cluster information - verify server is running and Raft is enabled: %w", err)
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
			return fmt.Errorf("failed to retrieve partition details - verify server is running and storage %q exists: %w", storage, err)
		}

		// Collect all partition responses using helper function
		partitionResponses, err = collectPartitionResponses(partitionsStream)
		if err != nil {
			return err
		}
	}

	// Step 3: Display results using the RPC responses
	return displayFormattedResults(writer, clusterInfoResp, partitionResponses, storage, listPartitions, colorOutput)
}

// displayFormattedResults displays cluster information using tabular format
func displayFormattedResults(writer io.Writer, clusterInfoResp *gitalypb.RaftClusterInfoResponse, partitions []*gitalypb.GetPartitionsResponse, storage string, listPartitions bool, colorOutput *colorOutput) error {
	fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("=== Gitaly Cluster Information ==="))

	// Display cluster statistics overview
	if clusterInfoResp.GetStatistics() != nil {
		if err := displayClusterStatistics(writer, clusterInfoResp.GetStatistics(), storage, colorOutput); err != nil {
			return err
		}
	}

	// Display partition overview table if requested
	if listPartitions || storage != "" {
		if len(partitions) > 0 {
			// Sort partitions by partition key for consistent output
			sortPartitionsByKey(partitions)

			return displayPartitionTable(writer, partitions, storage, colorOutput)
		} else if storage != "" {
			fmt.Fprintf(writer, "No partitions found for storage: %s\n", storage)
		}
	} else {
		// Suggest showing partitions if not displayed
		fmt.Fprintf(writer, "%s\n", colorOutput.formatInfo("Use --list-partitions to display partition overview table."))
	}

	return nil
}

// displayClusterStatistics displays cluster-wide statistics in a readable format
func displayClusterStatistics(writer io.Writer, stats *gitalypb.ClusterStatistics, storageFilter string, colorOutput *colorOutput) error {
	// Display overall cluster health at the top
	partitionHealth := colorOutput.formatHealthStatus(int(stats.GetHealthyPartitions()), int(stats.GetTotalPartitions()))
	replicaHealth := colorOutput.formatHealthStatus(int(stats.GetHealthyReplicas()), int(stats.GetTotalReplicas()))

	fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("=== Cluster Health Summary ==="))
	fmt.Fprintf(writer, "  Partitions: %s\n", partitionHealth)
	fmt.Fprintf(writer, "  Replicas: %s\n\n", replicaHealth)

	fmt.Fprintf(writer, "%s\n", colorOutput.formatHeader("=== Cluster Statistics ==="))
	fmt.Fprintf(writer, "  Total Partitions: %s\n", colorOutput.formatInfo(fmt.Sprintf("%d", stats.GetTotalPartitions())))
	fmt.Fprintf(writer, "  Total Replicas: %s\n", colorOutput.formatInfo(fmt.Sprintf("%d", stats.GetTotalReplicas())))
	fmt.Fprintf(writer, "  Healthy Partitions: %s\n", colorOutput.formatInfo(fmt.Sprintf("%d", stats.GetHealthyPartitions())))
	fmt.Fprintf(writer, "  Healthy Replicas: %s\n", colorOutput.formatInfo(fmt.Sprintf("%d", stats.GetHealthyReplicas())))
	fmt.Fprintf(writer, "\n")

	if len(stats.GetStorageStats()) > 0 {
		fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("=== Per-Storage Statistics ==="))

		// Filter storage names if a storage filter is specified
		var storageNames []string
		if storageFilter != "" {
			// Only show the filtered storage if it exists
			if _, exists := stats.GetStorageStats()[storageFilter]; exists {
				storageNames = append(storageNames, storageFilter)
			}
		} else {
			// Show all storages if no filter is specified
			for storageName := range stats.GetStorageStats() {
				storageNames = append(storageNames, storageName)
			}
			sort.Strings(storageNames)
		}

		// Create table writer for storage statistics
		tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

		// Table headers
		fmt.Fprintf(tw, "STORAGE\tLEADER COUNT\tREPLICA COUNT\n")
		fmt.Fprintf(tw, "-------\t------------\t-------------\n")

		for _, storageName := range storageNames {
			storageStat := stats.GetStorageStats()[storageName]
			fmt.Fprintf(tw, "%s\t%d\t%d\n",
				storageName,
				storageStat.GetLeaderCount(),
				storageStat.GetReplicaCount())
		}

		// Flush the table and add spacing
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush storage statistics table: %w", err)
		}
		fmt.Fprintf(writer, "\n")
	}
	return nil
}

// displayPartitionTable displays partitions in a tabular format
func displayPartitionTable(writer io.Writer, partitions []*gitalypb.GetPartitionsResponse, storageFilter string, colorOutput *colorOutput) error {
	fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("=== Partition Overview ==="))

	// Create table writer
	tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	// Table headers
	fmt.Fprintf(tw, "PARTITION KEY\tLEADER\tREPLICAS\tHEALTH\tLAST INDEX\tMATCH INDEX\tREPOSITORIES\n")
	fmt.Fprintf(tw, "-------------\t------\t--------\t------\t----------\t-----------\t------------\n")

	for _, partition := range partitions {
		// Find leader and collect replica info
		leader := "None"
		healthyReplicas := 0
		totalReplicas := len(partition.GetReplicas())
		var replicaStorages []string
		var filteredReplicas []string
		var leaderLastIndex, leaderMatchIndex uint64

		for _, replica := range partition.GetReplicas() {
			storageName := replica.GetReplicaId().GetStorageName()
			if replica.GetIsLeader() {
				leader = colorOutput.formatInfo(storageName)
				leaderLastIndex = replica.GetLastIndex()
				leaderMatchIndex = replica.GetMatchIndex()
			}
			if replica.GetIsHealthy() {
				healthyReplicas++
			}
			replicaStorages = append(replicaStorages, storageName)

			// Collect replicas matching storage filter
			if storageFilter != "" && storageName == storageFilter {
				filteredReplicas = append(filteredReplicas, storageName)
			}
		}

		// Format replica list showing storage names
		replicasStr := strings.Join(replicaStorages, ", ")
		if storageFilter != "" && len(filteredReplicas) > 0 {
			// Show all replicas but highlight the filtered ones
			replicasStr = fmt.Sprintf("%s (filtered: %s)", replicasStr, strings.Join(filteredReplicas, ", "))
		}

		// Format health status with color
		var healthStr string
		if healthyReplicas == totalReplicas && totalReplicas > 0 {
			healthStr = colorOutput.formatHealthy(fmt.Sprintf("%d/%d", healthyReplicas, totalReplicas))
		} else if healthyReplicas == 0 {
			healthStr = colorOutput.formatUnhealthy(fmt.Sprintf("%d/%d", healthyReplicas, totalReplicas))
		} else {
			healthStr = colorOutput.formatWarning(fmt.Sprintf("%d/%d", healthyReplicas, totalReplicas))
		}

		// Format last index and match index
		lastIndexStr := "N/A"
		matchIndexStr := "N/A"
		if leader != "None" {
			lastIndexStr = fmt.Sprintf("%d", leaderLastIndex)
			matchIndexStr = fmt.Sprintf("%d", leaderMatchIndex)
		}

		// Format repository count
		repoCount := len(partition.GetRelativePaths())
		repoStr := fmt.Sprintf("%d repos", repoCount)

		// Display full partition key
		partitionKeyDisplay := partition.GetPartitionKey().GetValue()

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			partitionKeyDisplay, leader, replicasStr, healthStr, lastIndexStr, matchIndexStr, repoStr)
	}

	// Flush the partition table
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to flush partition overview table: %w", err)
	}

	return nil
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
