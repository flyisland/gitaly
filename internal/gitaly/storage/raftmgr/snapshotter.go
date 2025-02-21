package raftmgr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	arc "gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	logging "gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const snapSuffix = ".snap"

// Snapshot is a structure that holds state about a temporary file that is used
// to hold a snapshot. By using an intermediate file we avoid holding everything
// in memory. Index and Term are used to identify when the snapshot was taken.
type Snapshot struct {
	file     *os.File
	metadata SnapshotMetadata
}

// SnapshotMetadata holds the last index and term corresponding to when the snapshot was taken
type SnapshotMetadata struct {
	index       storage.LSN
	term        uint64
	partitionID storage.PartitionID
}

// RaftSnapshotter manages the creation and handling of snapshots in a Raft network.
// It provides thread-safe operations for snapshot management.
type RaftSnapshotter struct {
	sync.Mutex
	logger  logging.Logger
	dir     string
	metrics SnapshotterMetrics
}

// Snapshotter is an interface to implement snapshotting in raft
type Snapshotter interface {
	sync.Locker
	materializeSnapshot(snapshot SnapshotMetadata, tx storage.Transaction) (_ *Snapshot, returnErr error)
}

// NewRaftSnapshotter creates a new Snapshotter
func NewRaftSnapshotter(cfg config.Raft, logger logging.Logger, metrics SnapshotterMetrics) (Snapshotter, error) {
	logger = logger.WithField("component", "raft.snapshotter")
	logger.Info("Initializing Raft Snapshotter")

	// Create the snapshot directory if it doesn't exist
	if err := os.MkdirAll(cfg.SnapshotDir, mode.Directory); err != nil {
		return nil, fmt.Errorf("create snapshot directory: %w", err)
	}

	return &RaftSnapshotter{
		logger:  logger,
		dir:     cfg.SnapshotDir,
		metrics: metrics,
	}, nil
}

// writeTarball writes the kv state of db and all folders/files from a partition's root to disk
func writeTarball(partitionRoot string, kvFile *os.File, w io.Writer) error {
	builder := arc.NewTarBuilder(partitionRoot, w)

	if err := builder.VirtualFileWithContents("kv-state", kvFile); err != nil {
		return fmt.Errorf("tar builder: virtual file: %w", err)
	}

	if err := builder.RecursiveDir(".", "fs", true); err != nil {
		return fmt.Errorf("tar builder: recursive dir: %w", err)
	}

	if err := builder.Close(); err != nil {
		return fmt.Errorf("tar builder: close: %w", err)
	}
	return nil
}

// materializeSnapshot materializes the snapshot inside a transaction and writes to a compressed tar
func (rs *RaftSnapshotter) materializeSnapshot(snapshotMetadata SnapshotMetadata, tx storage.Transaction) (_ *Snapshot, returnErr error) {
	saveSnapTimer := prometheus.NewTimer(rs.metrics.snapSaveSec)

	// Make a tmp file in snapshot dir
	tmpFile := fmt.Sprintf("%016d-%016d-%016d%s", snapshotMetadata.partitionID, snapshotMetadata.term, snapshotMetadata.index, ".tmp")
	archive, err := os.Create(filepath.Join(rs.dir, tmpFile))
	if err != nil {
		return nil, fmt.Errorf("create snapshot file: %w", err)
	}

	rs.logger.WithField("path", archive.Name()).Info("Start snapshot creation")

	// Clean up tmp file if any errors arise after this point
	var keep bool
	defer func() {
		if keep {
			return
		}
		if err := os.Remove(archive.Name()); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("clean up temp snapshot: %w", err))
			return
		}
	}()

	// Take copy of partition's kv state
	kvFile, err := storage.CreateKvFile(tx)
	if err != nil {
		return nil, fmt.Errorf("write kv file: %w", err)
	}
	defer func() {
		if err := kvFile.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close temp KV file: %w", err))
		}
	}()
	if err := writeTarball(tx.FS().Root(), kvFile, archive); err != nil {
		return nil, fmt.Errorf("write tarball: %w", err)
	}

	if err := archive.Sync(); err != nil {
		return nil, fmt.Errorf("sync archive to disk: %w", err)
	}

	// Finalize the archive.
	if err := archive.Close(); err != nil {
		return nil, fmt.Errorf("finalize snapshot: %w", err)
	}

	snapshotName := strings.Replace(tmpFile, filepath.Ext(tmpFile), snapSuffix, 1)
	outputDestination := filepath.Join(rs.dir, snapshotName)

	saveSnapTimer.ObserveDuration()
	rs.logger.WithField("path", outputDestination).Info("Snapshot saved")

	// Now that we've written all files to the archive, we can rename from a tmp file to a final snapshot
	if err := os.Rename(archive.Name(), outputDestination); err != nil {
		return nil, fmt.Errorf("rename temporary file: %w", err)
	}

	// Keep the temporary file for later use
	keep = true

	// After closing the archive
	f, err := os.Open(outputDestination)
	if err != nil {
		return nil, fmt.Errorf("open archive for verification: %w", err)
	}
	defer f.Close()

	return &Snapshot{
		file: f,
		metadata: SnapshotMetadata{
			index: snapshotMetadata.index,
			term:  snapshotMetadata.term,
		},
	}, nil
}
