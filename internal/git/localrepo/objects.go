package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

// ObjectType is an Enum for the type of object of
// the ls-tree entry, which can be can be tree, blob or commit
type ObjectType int

// Enum values for ObjectType
const (
	Unknown ObjectType = iota
	Tree
	Blob
	Submodule
)

// ObjectTypeFromString translates a string representation of the object type into an ObjectType enum.
func ObjectTypeFromString(s string) ObjectType {
	switch s {
	case "tree":
		return Tree
	case "blob":
		return Blob
	case "commit":
		return Submodule
	default:
		return Unknown
	}
}

// InvalidObjectError is returned when trying to get an object id that is invalid or does not exist.
type InvalidObjectError string

func (err InvalidObjectError) Error() string { return fmt.Sprintf("invalid object %q", string(err)) }

// ReadObjectInfo attempts to read the object info based on a revision.
func (repo *Repo) ReadObjectInfo(ctx context.Context, rev git.Revision) (*catfile.ObjectInfo, error) {
	infoReader, cleanup, err := repo.catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("getting object info reader: %w", err)
	}
	defer cleanup()

	objectInfo, err := infoReader.Info(ctx, rev)
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return nil, InvalidObjectError(rev)
		}
		return nil, fmt.Errorf("getting object info: %w", err)
	}

	return objectInfo, nil
}

// ReadObject reads an object from the repository's object database. InvalidObjectError
// is returned if the oid does not refer to a valid object.
func (repo *Repo) ReadObject(ctx context.Context, oid git.ObjectID) ([]byte, error) {
	objectReader, cancel, err := repo.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("create object reader: %w", err)
	}
	defer cancel()

	object, err := objectReader.Object(ctx, oid.Revision())
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return nil, InvalidObjectError(oid.String())
		}
		return nil, fmt.Errorf("get object from reader: %w", err)
	}

	data, err := io.ReadAll(object)
	if err != nil {
		return nil, fmt.Errorf("read object from reader: %w", err)
	}

	return data, nil
}

// WalkObjects walks the object graph starting from heads and writes to the output the objects it
// sees. Objects that are encountered during the walk and exist in the object database are written
// out as `<oid> <path>\n` with the `<path>` being sometimes present if applicable. Objects that
// were encountered during the walk but do not exist in the object database are written out
// as `?<oid>\n`.
func (repo *Repo) WalkObjects(ctx context.Context, heads io.Reader, output io.Writer) error {
	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "rev-list",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--objects"},
				gitcmd.Flag{Name: "--missing=print"},
				gitcmd.Flag{Name: "--stdin"},
			},
		},
		gitcmd.WithStdin(heads),
		gitcmd.WithStdout(output),
		gitcmd.WithStderr(&stderr),
	); err != nil {
		return structerr.New("rev-list: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// PackObjects takes in object IDs separated by newlines. It packs the objects into a pack file and
// writes it into the output.
func (repo *Repo) PackObjects(ctx context.Context, objectIDs io.Reader, output io.Writer) error {
	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "pack-objects",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "-q"},
				gitcmd.Flag{Name: "--stdout"},
			},
		},
		gitcmd.WithStdin(objectIDs),
		gitcmd.WithStderr(&stderr),
		gitcmd.WithStdout(output),
	); err != nil {
		return structerr.New("pack objects: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// UnpackObjects unpacks the objects from the pack file to the repository's object database.
func (repo *Repo) UnpackObjects(ctx context.Context, packFile io.Reader) error {
	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "unpack-objects",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "-q"},
			},
		},
		gitcmd.WithStdin(packFile),
		gitcmd.WithStderr(stderr),
	); err != nil {
		return structerr.New("unpack objects: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// ListObjects writes into output a newline separated list of all object IDs in the
// repository. Objects IDs are returned only once even there are multiple copies of
// the object in the repository. The output is not ordered.
func (repo *Repo) ListObjects(ctx context.Context, output io.Writer) error {
	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		gitcmd.Command{
			Name: "cat-file",
			Flags: []gitcmd.Option{
				gitcmd.Flag{Name: "--batch-check=%(objectname)"},
				gitcmd.Flag{Name: "--batch-all-objects"},
				gitcmd.Flag{Name: "--buffer"},
				gitcmd.Flag{Name: "--unordered"},
			},
		},
		gitcmd.WithStderr(stderr),
		gitcmd.WithStdout(output),
	); err != nil {
		return structerr.New("cat-file: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}
