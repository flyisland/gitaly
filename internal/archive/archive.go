package archive

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	// readDirEntriesPageSize is an amount of fs.DirEntry(s) to read
	// from the opened file descriptor of the directory.
	readDirEntriesPageSize = 32
)

// WriteTarball writes a tarball to an `io.Writer` for the provided path
// containing the specified archive members. Members should be specified
// relative to `path`.
//
// Symlinks will be included in the archive. Permissions are normalised to
// allow global read/write.
func WriteTarball(ctx context.Context, logger log.Logger, writer io.Writer, path string, members ...string) error {
	builder := NewTarBuilder(path, writer)
	builder.allowSymlinks = true

	for _, member := range members {
		_ = builder.RecursiveDir(member, "", true)
	}

	if err := builder.Close(); err != nil {
		return fmt.Errorf("write tarball: %w", err)
	}

	return nil
}
