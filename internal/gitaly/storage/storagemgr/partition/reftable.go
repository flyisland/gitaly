package partition

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/safe"
)

// reftableRecorder records the new reftables written during a transaction, and implements
// logic to resequence and stage them into the WAL entry.
type reftableRecorder struct {
	snapshotRepoPath string
	latestTable      reftable.Name
}

// newReftableRecorder returns a new reftable recorder.
func newReftableRecorder(snapshotRepoPath string) (*reftableRecorder, error) {
	latestTable, err := getLatestReftableName(reftableDirectoryPath(snapshotRepoPath))
	if err != nil {
		return nil, fmt.Errorf("get latest reftable name: %w", err)
	}

	return &reftableRecorder{
		snapshotRepoPath: snapshotRepoPath,
		latestTable:      latestTable,
	}, nil
}

// stageTables stages the tables written out by the transaction into the WAL entry. The tables are
// resequenced so their update indexes after the tables in the target repository. Concurrent reference writes in
// different transactions operating on the same base snapshot would produce the same update indexes for the tables.
// Resequencing the tables resolves the update index conflicts and enables the table files to committed to the
// repository one after another.
//
// This method patches the tables.list and the .ref files in-place and stages them into the transaction. The
// snapshot repository of the transaction is broken after this invocation.
func (r reftableRecorder) stageTables(ctx context.Context, targetRepositoryPath string, tx *Transaction) error {
	if len(tx.referenceUpdates) == 0 {
		return nil
	}

	defer trace.StartRegion(ctx, "stageTables").End()

	tables, err := reftable.ReadTablesList(r.snapshotRepoPath)
	if err != nil {
		return fmt.Errorf("read tables list: %w", err)
	}

	var newlyCreatedTables []reftable.Name
	for i, tableName := range tables {
		if tableName == r.latestTable {
			newlyCreatedTables = tables[i+1:]
			break
		}
	}

	if newlyCreatedTables == nil {
		// The resequencing logic identifies the new tables written during a transaction by them
		// coming after the latest table that existed when the transaction started. The table is locked
		// when the transaction is began, and expected to be present.
		return errors.New("latest table was not found")
	} else if len(newlyCreatedTables) == 0 {
		// The transaction didn't write any tables.
		return nil
	}

	targetTablesList, err := reftable.ReadTablesList(targetRepositoryPath)
	if err != nil {
		return fmt.Errorf("read target tables.list: %w", err)
	}

	// delta is the amount we need to bump up the update indexes in the tables written by the transaction during
	// snapshot. This way they'll be resequenced to have indexes that come immediately after the latest max update
	// index in the repository.
	delta := targetTablesList[len(targetTablesList)-1].MaxUpdateIndex + 1 - newlyCreatedTables[0].MinUpdateIndex

	reftableDirRelativePath := filepath.Join(tx.relativePath, "reftable")
	for _, originalTableName := range newlyCreatedTables {
		resequencedTableName := originalTableName
		resequencedTableName.MinUpdateIndex += delta
		resequencedTableName.MaxUpdateIndex += delta

		if err := func() (returnedErr error) {
			table, err := reftable.ParseTable(filepath.Join(r.snapshotRepoPath, "reftable", originalTableName.String()))
			if err != nil {
				return fmt.Errorf("parse table: %w", err)
			}

			defer func() {
				if err := table.Close(); err != nil {
					returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
				}
			}()

			if err := table.PatchUpdateIndexes(
				resequencedTableName.MinUpdateIndex,
				resequencedTableName.MaxUpdateIndex,
			); err != nil {
				return fmt.Errorf("patch update indexes: %w", err)
			}

			return nil
		}(); err != nil {
			return err
		}

		// Log the table that has now been resequenced. The source file still has the original
		// name but the destination is the resequenced name.
		if err := tx.walEntry.CreateFile(
			filepath.Join(r.snapshotRepoPath, "reftable", originalTableName.String()),
			filepath.Join(reftableDirRelativePath, resequencedTableName.String()),
		); err != nil {
			return fmt.Errorf("create resequenced table: %w", err)
		}

		targetTablesList = append(targetTablesList, resequencedTableName)
	}

	targetTablesListString := make([]string, len(targetTablesList))
	for i := range targetTablesList {
		targetTablesListString[i] = targetTablesList[i].String()
	}

	resequencedTablesListPath := filepath.Join(r.snapshotRepoPath, "reftable", "tables.list.resequenced")
	if err := os.WriteFile(resequencedTablesListPath, []byte(strings.Join(targetTablesListString, "\n")+"\n"), mode.File); err != nil {
		return fmt.Errorf("replace tables.list file: %w", err)
	}

	if err := safe.NewSyncer().Sync(ctx, resequencedTablesListPath); err != nil {
		return fmt.Errorf("sync patched tables.list: %w", err)
	}

	tablesListRelativePath := filepath.Join(reftableDirRelativePath, "tables.list")
	tx.walEntry.RemoveDirectoryEntry(tablesListRelativePath)
	if err := tx.walEntry.CreateFile(resequencedTablesListPath, tablesListRelativePath); err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	return nil
}

// reftableDirectoryPath returns the reftable directory's location given a repository path.
func reftableDirectoryPath(repositoryPath string) string {
	return filepath.Join(repositoryPath, "reftable")
}

// getLatestReftableName reads the filename of the latest table from the tables.list file.
func getLatestReftableName(reftableDir string) (reftable.Name, error) {
	f, err := os.Open(filepath.Join(reftableDir, "tables.list"))
	if err != nil {
		return reftable.Name{}, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	// This is the pattern of a table file's name. We use it to compute how far from
	// the end of the file we need to seek to read the last table in the file.
	const tableLineLength = int64(len("0x000000000001-0x000000000002-RANDOM3.ref\n")) + 1

	if _, err := f.Seek(-tableLineLength, io.SeekEnd); err != nil {
		return reftable.Name{}, fmt.Errorf("seek: %w", err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return reftable.Name{}, fmt.Errorf("read all: %w", err)
	}

	// Each table line ends in a newline. Trim it as it's not part of the table name.
	latestTable, err := reftable.ParseName(string(bytes.TrimSuffix(data, []byte("\n"))))
	if err != nil {
		return reftable.Name{}, fmt.Errorf("parse name: %w", err)
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

	return os.WriteFile(filepath.Join(reftableDir, latestTableName.String()+".lock"), nil, mode.File)
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

	return os.Remove(filepath.Join(reftableDir, latestTableName.String()+".lock"))
}
