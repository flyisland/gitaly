package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// ManifestPath returns the manifest file's path in the log entry.
func ManifestPath(logEntryPath string) string {
	return filepath.Join(logEntryPath, "MANIFEST")
}

// ReadManifest returns the log entry's manifest from the given position in the log.
func ReadManifest(stateDir string) (*gitalypb.LogEntry, error) {
	manifestBytes, err := os.ReadFile(ManifestPath(stateDir))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var logEntry gitalypb.LogEntry
	if err := proto.Unmarshal(manifestBytes, &logEntry); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &logEntry, nil
}

// WriteManifest writes the log entry's manifest to the disk.
func WriteManifest(ctx context.Context, stateDir string, manifest *gitalypb.LogEntry) error {
	manifestBytes, err := proto.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Finalize the log entry by writing the MANIFEST file into the log entry's directory.
	manifestPath := ManifestPath(stateDir)
	if err := os.WriteFile(manifestPath, manifestBytes, mode.File); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}

// RemoveManifest removes the existing manifest file.
func RemoveManifest(ctx context.Context, stateDir string) error {
	return os.Remove(ManifestPath(stateDir))
}
