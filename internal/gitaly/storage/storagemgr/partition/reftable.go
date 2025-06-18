package partition

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
)

// reftableDirectoryPath returns the reftable directory's location given a repository path.
func reftableDirectoryPath(repositoryPath string) string {
	return filepath.Join(repositoryPath, "reftable")
}

// getLatestReftableName reads the filename of the latest table from the tables.list file.
func getLatestReftableName(reftableDir string) (string, error) {
	f, err := os.Open(filepath.Join(reftableDir, "tables.list"))
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	// This is the pattern of a table file's name. We use it to compute how far from
	// the end of the file we need to seek to read the last table in the file.
	const tableLineLength = int64(len("0x000000000001-0x000000000002-RANDOM3.ref\n")) + 1

	if _, err := f.Seek(-tableLineLength, io.SeekEnd); err != nil {
		return "", fmt.Errorf("seek: %w", err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return "", fmt.Errorf("read all: %w", err)
	}

	// Each table line ends in a newline. Trim it as it's not part of the table name.
	latestTable := string(bytes.TrimSuffix(data, []byte("\n")))

	// This is purely for sanity checking that we're not reading garbage from the file.
	if !reftable.ReftableTableNameRegex.MatchString(latestTable) {
		return "", fmt.Errorf("invalid reftable name: %q", latestTable)
	}

	return latestTable, nil
}

// preventReftableCompaction writes a .lock file on the latest reftable
// in the repository. This prevents new tables written in a transaction
// from being merged with the existing tables. Modifying the existing
// tables would lead to conflicts if multiple transactions do so concurrently.
//
// This is a workaround as Git does not provide a way to prevent autocompaction
// of tables. Issue: https://gitlab.com/gitlab-org/git/-/issues/350
func preventReftableCompaction(repositoryPath string) error {
	reftableDir := reftableDirectoryPath(repositoryPath)

	latestTableName, err := getLatestReftableName(reftableDir)
	if err != nil {
		return fmt.Errorf("get latest reftable name: %w", err)
	}

	return os.WriteFile(filepath.Join(reftableDir, latestTableName+".lock"), nil, mode.File)
}

// allowReftableCompaction enables compaction of table files that existed in the
// repository when the transaction started. This is done by removing the .lock file
// written for the latest table when beginning a write transaction.
func allowReftableCompaction(repositoryPath string) error {
	reftableDir := reftableDirectoryPath(repositoryPath)

	latestTableName, err := getLatestReftableName(reftableDir)
	if err != nil {
		return fmt.Errorf("get latest reftableName: %w", err)
	}

	return os.Remove(filepath.Join(reftableDir, latestTableName+".lock"))
}
