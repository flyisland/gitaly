package wal

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// Entry represents a write-ahead log entry.
type Entry struct {
	// fileIDSequence is a sequence used to generate unique IDs for files
	// staged as part of this entry.
	fileIDSequence uint64
	// operations are the operations this entry consists of.
	operations operations
	// stateDirectory is the directory where the entry's state is stored.
	stateDirectory string
}

func newIrregularFileStagedError(mode fs.FileMode) error {
	return structerr.NewInvalidArgument("irregular file staged").WithMetadata("mode", mode.String())
}

// NewEntry returns a new Entry that can be used to construct a write-ahead
// log entry.
func NewEntry(stateDirectory string) *Entry {
	return &Entry{stateDirectory: stateDirectory}
}

// Directory returns the absolute path of the directory where the log entry is staging its state.
func (e *Entry) Directory() string {
	return e.stateDirectory
}

// Operations returns the operations of the log entry.
func (e *Entry) Operations() []*gitalypb.LogEntry_Operation {
	return e.operations
}

// stageFile stages a file into the WAL entry by linking it in the state directory.
// The file's name in the state directory is returned and can be used to link the file
// subsequently into the correct location.
func (e *Entry) stageFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("lstat: %w", err)
	}

	// Error out if there is an attempt to stage someting other than a regular file, ie.
	// symlink, directory or anything else.
	if !info.Mode().IsRegular() {
		return "", newIrregularFileStagedError(info.Mode().Type())
	}

	// Strip the write permissions of the files. Our snapshot isolation relies on files not
	// being modified. Also strip permissions of other users than Gitaly's user.
	//
	// ModeExecutable is used as the mask since it has the widest permission bits we allow
	// with both read and execute permissions set.
	actualPerms := info.Mode().Perm()
	if expectedPerms := actualPerms & (mode.Executable); actualPerms != expectedPerms {
		if err := os.Chmod(path, expectedPerms); err != nil {
			return "", fmt.Errorf("chmod: %w", err)
		}
	}

	e.fileIDSequence++

	// We use base 36 as it produces shorter names and thus smaller log entries.
	// The file names within the log entry are not important as the manifest records the
	// actual name the file will be linked as.
	fileName := strconv.FormatUint(e.fileIDSequence, 36)
	if err := os.Link(path, filepath.Join(e.stateDirectory, fileName)); err != nil {
		return "", fmt.Errorf("link: %w", err)
	}

	return fileName, nil
}

// SetKey adds an operation to set a key with a value in the partition's key-value store.
func (e *Entry) SetKey(key, value []byte) {
	e.operations.setKey(key, value)
}

// DeleteKey adds an operation to delete a key from the partition's key-value store.
func (e *Entry) DeleteKey(key []byte) {
	e.operations.deleteKey(key)
}

// CreateDirectory records creation of a single directory.
func (e *Entry) CreateDirectory(relativePath string) {
	e.operations.createDirectory(relativePath)
}

// CreateFile stages the file at the source and adds an operation to link it
// to the given destination relative path in the storage.
func (e *Entry) CreateFile(sourceAbsolutePath string, relativePath string) error {
	stagedFile, err := e.stageFile(sourceAbsolutePath)
	if err != nil {
		return fmt.Errorf("stage file: %w", err)
	}

	e.operations.createHardLink(stagedFile, relativePath, false)
	return nil
}

// CreateLink records a creation of a hard link to an exisiting file in the partition.
func (e *Entry) CreateLink(sourceRelativePath, destinationRelativePath string) {
	e.operations.createHardLink(sourceRelativePath, destinationRelativePath, true)
}

// RemoveDirectoryEntry records the removal of the directory entry at the given path.
func (e *Entry) RemoveDirectoryEntry(relativePath string) {
	e.operations.removeDirectoryEntry(relativePath)
}
