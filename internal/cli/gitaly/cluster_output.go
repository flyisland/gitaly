package gitaly

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

// ClusterInfoOutput represents the structured output for the cluster info command.
// This type defines the contract for JSON output, making it easier to maintain
// compatibility with clients consuming the JSON data.
type ClusterInfoOutput struct {
	ClusterInfo *gitalypb.RaftClusterInfoResponse
	Partitions  []*gitalypb.GetPartitionsResponse
}

// ToJSON outputs the cluster info in JSON format
func (o *ClusterInfoOutput) ToJSON(writer io.Writer) error {
	// Sort partitions by partition key for consistent output
	if len(o.Partitions) > 0 {
		sortPartitionsByKey(o.Partitions)
	}

	// Configure protojson marshaler
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: false, // Omit fields with default values
		UseProtoNames:   false, // Use lowerCamelCase JSON field names
	}

	// Convert protobuf to JSON, then to map for standard JSON marshaling
	clusterInfoBytes, err := marshaler.Marshal(o.ClusterInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	var clusterInfoMap map[string]interface{}
	if err := json.Unmarshal(clusterInfoBytes, &clusterInfoMap); err != nil {
		return fmt.Errorf("failed to unmarshal cluster info: %w", err)
	}

	// Build the output structure
	output := map[string]interface{}{
		"clusterInfo": clusterInfoMap,
	}

	// Add partitions if present
	if len(o.Partitions) > 0 {
		var partitionsArray []map[string]interface{}
		for _, partition := range o.Partitions {
			partitionBytes, err := marshaler.Marshal(partition)
			if err != nil {
				return fmt.Errorf("failed to marshal partition: %w", err)
			}

			var partitionMap map[string]interface{}
			if err := json.Unmarshal(partitionBytes, &partitionMap); err != nil {
				return fmt.Errorf("failed to unmarshal partition: %w", err)
			}

			partitionsArray = append(partitionsArray, partitionMap)
		}
		output["partitions"] = partitionsArray
	}

	// Marshal with indentation for human-friendly output
	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON output: %w", err)
	}

	fmt.Fprintf(writer, "%s\n", string(jsonBytes))
	return nil
}

// ToText outputs the cluster info in human-readable text format
func (o *ClusterInfoOutput) ToText(writer io.Writer, colorOutput *colorOutput, storage string, listPartitions bool) error {
	fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("=== Gitaly Cluster Information ==="))

	// Display cluster statistics overview
	if o.ClusterInfo.GetStatistics() != nil {
		if err := displayClusterStatistics(writer, o.ClusterInfo.GetStatistics(), storage, colorOutput); err != nil {
			return err
		}
	}

	// Display partition overview table if requested
	if listPartitions || storage != "" {
		if len(o.Partitions) > 0 {
			// Sort partitions by partition key for consistent output
			sortPartitionsByKey(o.Partitions)

			return displayPartitionTable(writer, o.Partitions, storage, colorOutput)
		} else if storage != "" {
			fmt.Fprintf(writer, "No partitions found for storage: %s\n", storage)
		}
	} else {
		// Suggest showing partitions if not displayed
		fmt.Fprintf(writer, "%s\n", colorOutput.formatInfo("Use --list-partitions to display partition overview table."))
	}

	return nil
}

// PartitionDetailsOutput represents the structured output for the get-partition command.
// This type defines the contract for JSON output, making it easier to maintain
// compatibility with clients consuming the JSON data.
type PartitionDetailsOutput struct {
	Partitions []*gitalypb.GetPartitionsResponse
}

// ToJSON outputs the partition details in JSON format
func (o *PartitionDetailsOutput) ToJSON(writer io.Writer) error {
	// Sort partitions by partition key for consistent output
	if len(o.Partitions) > 0 {
		sortPartitionsByKey(o.Partitions)
	}

	// Configure protojson marshaler
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: false, // Omit fields with default values
		UseProtoNames:   false, // Use lowerCamelCase JSON field names
	}

	// Build the partitions array
	var partitionsArray []map[string]interface{}
	for _, partition := range o.Partitions {
		partitionBytes, err := marshaler.Marshal(partition)
		if err != nil {
			return fmt.Errorf("failed to marshal partition: %w", err)
		}

		var partitionMap map[string]interface{}
		if err := json.Unmarshal(partitionBytes, &partitionMap); err != nil {
			return fmt.Errorf("failed to unmarshal partition: %w", err)
		}

		partitionsArray = append(partitionsArray, partitionMap)
	}

	// Build the output structure
	output := map[string]interface{}{
		"partitions": partitionsArray,
	}

	// Marshal with indentation for human-friendly output
	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON output: %w", err)
	}

	fmt.Fprintf(writer, "%s\n", string(jsonBytes))
	return nil
}

// ToText outputs the partition details in human-readable text format
func (o *PartitionDetailsOutput) ToText(writer io.Writer, colorOutput *colorOutput, partitionKey, relativePath string) error {
	// Display detailed partition information
	if len(o.Partitions) > 0 {
		// Sort partitions by partition key for consistent output
		sortPartitionsByKey(o.Partitions)

		if relativePath != "" {
			fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader(fmt.Sprintf("=== Partition Details for Repository: %s ===", relativePath)))
		} else if partitionKey != "" {
			fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader(fmt.Sprintf("=== Partition Details for Key: %s ===", partitionKey)))
		}

		for i, partition := range o.Partitions {
			if i > 0 {
				fmt.Fprintf(writer, "\n")
			}

			fmt.Fprintf(writer, "Partition: %s\n\n", colorOutput.formatInfo(partition.GetPartitionKey().GetValue()))

			// Display replicas in tabular format
			if len(partition.GetReplicas()) > 0 {
				tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

				fmt.Fprintf(tw, "STORAGE\tROLE\tHEALTH\tLAST INDEX\tMATCH INDEX\n")
				fmt.Fprintf(tw, "-------\t----\t------\t----------\t-----------\n")

				for _, replica := range partition.GetReplicas() {
					var role string
					if replica.GetIsLeader() {
						role = colorOutput.formatInfo("Leader")
					} else {
						role = "Follower"
					}
					var health string
					if replica.GetIsHealthy() {
						health = colorOutput.formatHealthy("Healthy")
					} else {
						health = colorOutput.formatUnhealthy("Unhealthy")
					}
					fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\n",
						replica.GetReplicaId().GetStorageName(),
						role,
						health,
						replica.GetLastIndex(),
						replica.GetMatchIndex())
				}

				_ = tw.Flush()
				fmt.Fprintf(writer, "\n")
			}

			// Display repositories in tabular format
			if len(partition.GetRelativePaths()) > 0 {
				fmt.Fprintf(writer, "%s\n\n", colorOutput.formatHeader("Repositories:"))

				tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

				fmt.Fprintf(tw, "REPOSITORY PATH\n")
				fmt.Fprintf(tw, "---------------\n")

				for _, path := range partition.GetRelativePaths() {
					fmt.Fprintf(tw, "%s\n", path)
				}

				_ = tw.Flush()
			}
		}
	} else {
		fmt.Fprintf(writer, "No partitions found matching the specified criteria.\n")
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
