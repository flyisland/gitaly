// Package mode contains the file modes that are supported by the storage. All files and
// directories written to the storage must use one of these modes.
package mode

import (
	"io/fs"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode/permission"
)

const (
	// Directory is the mode directories are stored with in the storage.
	// It gives the owner read, write, and execute permissions on directories.
	Directory fs.FileMode = fs.ModeDir | permission.OwnerRead | permission.OwnerWrite | permission.OwnerExecute
	// Executable is the mode executable files are stored with in the storage.
	// It gives the owner read and execute permissions on the executable files.
	Executable fs.FileMode = permission.OwnerRead | permission.OwnerExecute
	// File is the mode files are stored with in the storage.
	// It gives the owner read permissions on the files.
	File fs.FileMode = permission.OwnerRead
)
