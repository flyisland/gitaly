package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"path/filepath"
)

// ComputePartition generates the old directory structure path based on the partition ID
func ComputePartition(id string) string {
	hasher := sha256.New()
	hasher.Write([]byte(id))
	hash := hex.EncodeToString(hasher.Sum(nil))

	return filepath.Join(
		hash[0:2],
		hash[2:4],
		id,
	)
}

// HashRaftPartitionPath creates a hash of the raft partition's path
func HashRaftPartitionPath(hasher hash.Hash, basePath, raftPartitionPath string) string {
	hash := hex.EncodeToString(hasher.Sum(nil))
	return filepath.Join(
		basePath,
		// These two levels balance the state directories into smaller
		// subdirectories to keep the directory sizes reasonable.
		hash[0:2],
		hash[2:4],
		raftPartitionPath,
	)
}

// GetRaftPartitionName formats the storage name and partition ID into a raft partition name
func GetRaftPartitionName(storageName, partitionID string) string {
	return storageName + "_" + partitionID
}
