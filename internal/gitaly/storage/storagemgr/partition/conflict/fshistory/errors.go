package fshistory

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// ReadWriteConflictError is returned when transaction performed a read on an
// inode that was modified by a concurrent transaction.
type ReadWriteConflictError struct {
	Path     string
	ReadLSN  storage.LSN
	WriteLSN storage.LSN
}

// Error returns the error message.
func (ReadWriteConflictError) Error() string {
	return "path was modified after read"
}

// NewReadWriteConflictError returns an error detailing a conflicting read.
func NewReadWriteConflictError(path string, readLSN, writeLSN storage.LSN) error {
	return structerr.NewAborted("%w", ReadWriteConflictError{Path: path, ReadLSN: readLSN, WriteLSN: writeLSN}).WithMetadataItems(
		structerr.MetadataItem{Key: "path", Value: path},
		structerr.MetadataItem{Key: "read_lsn", Value: readLSN},
		structerr.MetadataItem{Key: "write_lsn", Value: writeLSN},
	)
}

// NotFoundError is returned when a given path is not found.
type NotFoundError struct {
	Path string
}

// Error returns the error message.
func (NotFoundError) Error() string {
	return "path not found"
}

func newNotFoundError(path string) error {
	return structerr.NewAborted("%w", NotFoundError{Path: path}).WithMetadata("path", path)
}

// NotDirectoryError is returned when an element in a walked
// path is not a directory.
type NotDirectoryError struct {
	Path string
}

// Error returns the error message.
func (NotDirectoryError) Error() string {
	return "not a directory"
}

func newNotDirectoryError(path string) error {
	return structerr.NewAborted("%w", NotDirectoryError{Path: path}).WithMetadata("path", path)
}

// DirectoryNotEmptyError is returned when attempting to remove a
// directory that is not empty.
type DirectoryNotEmptyError struct {
	Path string
}

// Error returns the error message.
func (DirectoryNotEmptyError) Error() string {
	return "directory not empty"
}

func newDirectoryNotEmptyError(path string) error {
	return structerr.NewAborted("%w", DirectoryNotEmptyError{Path: path}).WithMetadata("path", path)
}

// AlreadyExistsError is returned when attempting when a file
// or directory already exists at the path.
type AlreadyExistsError struct {
	Path string
}

// Error returns the error message.
func (AlreadyExistsError) Error() string {
	return "already exists"
}

func newAlreadyExistsError(path string) error {
	return structerr.NewAborted("%w", AlreadyExistsError{Path: path}).WithMetadata("path", path)
}
