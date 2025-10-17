package catfile

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	pathPkg "path"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

type revisionPath struct{ revision, path string }

// TreeEntryFinder is a struct for searching through a tree with caching.
type TreeEntryFinder struct {
	objectReader ObjectContentReader
	treeCache    map[revisionPath][]*gitalypb.TreeEntry
}

// NewTreeEntryFinder initializes a TreeEntryFinder with an empty tree cache.
func NewTreeEntryFinder(objectReader ObjectContentReader) *TreeEntryFinder {
	return &TreeEntryFinder{
		objectReader: objectReader,
		treeCache:    make(map[revisionPath][]*gitalypb.TreeEntry),
	}
}

// FindByRevisionAndPath returns a TreeEntry struct for the object present at the revision/path pair.
func (tef *TreeEntryFinder) FindByRevisionAndPath(ctx context.Context, revision, path string) (*gitalypb.TreeEntry, error) {
	span, ctx := tracing.StartSpanIfHasParent(
		ctx,
		"catfile.FindByRevisionAndPatch",
		tracing.Tags{"revision": revision, "path": path},
	)
	defer span.Finish()

	dir := pathPkg.Dir(path)
	cacheKey := revisionPath{revision: revision, path: dir}
	entries, ok := tef.treeCache[cacheKey]

	if !ok {
		var err error
		entries, err = TreeEntries(ctx, tef.objectReader, revision, dir)
		if err != nil {
			return nil, err
		}

		tef.treeCache[cacheKey] = entries
	}

	for _, entry := range entries {
		if string(entry.GetPath()) == path {
			return entry, nil
		}
	}

	return nil, nil
}

func extractEntryInfoFromTreeData(
	objectHash git.ObjectHash,
	treeData io.Reader,
	commitOid, rootPath, oid string,
) ([]*gitalypb.TreeEntry, error) {
	if len(oid) == 0 {
		return nil, fmt.Errorf("empty tree oid")
	}

	bufReader := bufio.NewReader(treeData)

	var entries []*gitalypb.TreeEntry
	oidBuf := &bytes.Buffer{}

	oidSize := int64(objectHash.Hash().Size())

	for {
		modeBytes, err := bufReader.ReadBytes(' ')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || len(modeBytes) <= 1 {
			return nil, fmt.Errorf("read entry mode: %w", err)
		}
		modeBytes = modeBytes[:len(modeBytes)-1]

		filename, err := bufReader.ReadBytes('\x00')
		if err != nil || len(filename) <= 1 {
			return nil, fmt.Errorf("read entry path: %w", err)
		}
		filename = filename[:len(filename)-1]

		oidBuf.Reset()
		if _, err := io.CopyN(oidBuf, bufReader, oidSize); err != nil {
			return nil, fmt.Errorf("read entry oid: %w", err)
		}

		treeEntry, err := git.NewTreeEntry(commitOid, rootPath, filename, oidBuf.Bytes(), modeBytes)
		if err != nil {
			return nil, fmt.Errorf("new entry info: %w", err)
		}

		entries = append(entries, treeEntry)
	}

	return entries, nil
}

// TreeEntries returns the entries of a tree in given revision and path.
func TreeEntries(
	ctx context.Context,
	objectReader ObjectContentReader,
	revision, path string,
) (_ []*gitalypb.TreeEntry, returnedErr error) {
	span, ctx := tracing.StartSpanIfHasParent(
		ctx,
		"catfile.TreeEntries",
		tracing.Tags{"revision": revision, "path": path},
	)
	defer span.Finish()

	if path == "." {
		path = ""
	}

	// If we ask 'git cat-file' for a path outside the repository tree it
	// blows up with a fatal error. So, we avoid asking for this.
	if strings.HasPrefix(filepath.Clean(path), "../") {
		return nil, nil
	}

	treeObj, err := objectReader.Object(ctx, git.Revision(fmt.Sprintf("%s:%s", revision, path)))
	if err != nil {
		if errors.As(err, &NotFoundError{}) {
			return nil, nil
		}
		return nil, err
	}

	objectHash, err := git.ObjectHashByFormat(treeObj.Format)
	if err != nil {
		return nil, fmt.Errorf("looking up object hash: %w", err)
	}

	// The tree entry may not refer to a subtree, but instead to a blob. Historically, we have
	// simply ignored such objects altogether and didn't return an error, so we keep the same
	// behaviour.
	if treeObj.Type != "tree" {
		if _, err := io.Copy(io.Discard, treeObj); err != nil && returnedErr == nil {
			return nil, fmt.Errorf("discarding object: %w", err)
		}

		return nil, nil
	}

	entries, err := extractEntryInfoFromTreeData(objectHash, treeObj, revision, path, treeObj.Oid.String())
	if err != nil {
		return nil, err
	}

	return entries, nil
}
