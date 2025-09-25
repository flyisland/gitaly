package wal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/wal/reftree"
)

// ReferenceRecorder records the file system operations performed by reference transactions.
//
// It holds the pre-image of a repository in memory. The pre-image is kept in memory as we don't
// have a good hook point to capture it before the reference transactions perform changes.
// Reference transaction hook's prepared stage is too late as locks have been already acquired when
// it is called. The locking process may have created directories already, so we can't determine
// whether the directories existed already or not. 'pre-receive' hook is also not viable as it's
// not called before every reference transaction in Gitaly.
//
// RecordReferenceUpdates should be called after each committed reference transaction to determine
// the changes made, and to update the pre-image for a possible subsequent reference transaction.
//
// After the all changes are done, CommitPackedRefs should be called to commit the packed-refs changes
// potentially performed. It's only logged once at the end to avoid logging it multiple times.
type ReferenceRecorder struct {
	snapshotRoot string
	relativePath string
	zeroOID      git.ObjectID

	preImage                *reftree.Node
	preImagePackedRefsPath  string
	postImagePackedRefsPath string
	entry                   *Entry
}

// NewReferenceRecorder returns a new reference recorder.
func NewReferenceRecorder(tmpDir string, entry *Entry, snapshotRoot, relativePath string, zeroOID git.ObjectID) (*ReferenceRecorder, error) {
	preImage := reftree.New()

	repoRoot := filepath.Join(snapshotRoot, relativePath)
	startPoint := filepath.Join(snapshotRoot, relativePath, "refs")
	if err := filepath.WalkDir(startPoint, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && path == startPoint {
				return nil
			}

			return err
		}

		relativePath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return fmt.Errorf("rel: %w", err)
		}

		return preImage.InsertNode(relativePath, false, entry.IsDir())
	}); err != nil {
		return nil, err
	}

	preImagePackedRefsPath := filepath.Join(tmpDir, "packed-refs")
	postImagePackedRefsPath := filepath.Join(repoRoot, "packed-refs")
	if err := os.Link(postImagePackedRefsPath, preImagePackedRefsPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("record pre-image packed-refs: %w", err)
	}

	return &ReferenceRecorder{
		snapshotRoot:            snapshotRoot,
		relativePath:            relativePath,
		zeroOID:                 zeroOID,
		preImage:                preImage,
		preImagePackedRefsPath:  preImagePackedRefsPath,
		postImagePackedRefsPath: postImagePackedRefsPath,
		entry:                   entry,
	}, nil
}

// RecordReferenceUpdates records the file system Git performed in the log entry.
//
// The file system operations are recorded by first looking at the pre-image of the repository. We go through
// through each of the paths that could be modified in the transaction and recording whether they exist.
// If `refs/heads/main` is updated, we walk each of the components to see whether they existed before
// the reference transaction. For example, we'd check `refs`, `refs/heads` and `refs/heads/main` and record
// their existence in the pre-image.
//
// While going through the reference changes, we'll build two tree representations of the references being updated
// The first tree consists of operations that create a file, namely refererence updates and creations. The second
// tree consists of operations that may delete files, so reference deletions.
//
// We then figure out the reference transaction performed by walking the two trees we built.
//
// File and directory creations are identified by walking from the refs directory downwards in the hierarchy. Each
// directory and file that was did not exist in the pre-image state is recorded as a creation.
//
// File and directory removals are identified by walking from the leaf nodes, the references, towards the root 'refs'.
// directory. Each file and directory that existed in the pre-image but doesn't exist anymore is recorded as having
// been removed.
//
// Each recorded operation is staged against the target relative path by omitting the snapshotPrefix. This works as
// the snapshots retain the directory hierarchy of the original storage.
//
// The method assumes the reference directory is in good shape and doesn't have hierarchies of empty directories. It
// thus doesn't record the deletion of such directories. If a directory such as `refs/heads/parent/child/subdir`
// exists, and a reference called `refs/heads/parent` is created, we don't handle the deletions of empty directories.
func (r *ReferenceRecorder) RecordReferenceUpdates(ctx context.Context, refTX git.ReferenceUpdates) error {
	if len(refTX) == 0 {
		return nil
	}

	creations := reftree.New()
	deletions := reftree.New()
	defaultBranchUpdated := false
	preImagePaths := map[string]struct{}{}
	for reference, update := range refTX {
		if reference.String() == "HEAD" {
			defaultBranchUpdated = true
			continue
		}

		tree := creations
		if update.NewOID == r.zeroOID {
			tree = deletions
		}

		if err := tree.InsertReference(reference.String()); err != nil {
			return fmt.Errorf("insert into tree: %w", err)
		}

		for component, remainder, haveMore := "", reference.String(), true; haveMore; {
			var newComponent string
			newComponent, remainder, haveMore = strings.Cut(remainder, "/")

			component = filepath.Join(component, newComponent)
			if !r.preImage.Contains(component) {
				// The path did not exist before applying the transaction.
				// None of its children exist either.
				break
			}

			preImagePaths[component] = struct{}{}
		}
	}

	// Record default branch updates
	if defaultBranchUpdated {
		targetRelativePath := filepath.Join(r.relativePath, "HEAD")

		r.entry.operations.removeDirectoryEntry(targetRelativePath)
		fileID, err := r.entry.stageFile(filepath.Join(r.snapshotRoot, targetRelativePath))
		if err != nil {
			return fmt.Errorf("stage file: %w", err)
		}

		r.entry.operations.createHardLink(fileID, targetRelativePath, false)
	}

	// Record creations
	if err := creations.WalkPreOrder(func(path string, isDir bool) error {
		targetRelativePath := filepath.Join(r.relativePath, path)
		if isDir {
			if _, ok := preImagePaths[path]; ok {
				// The directory existed already in the pre-image and doesn't need to be recreated.
				return nil
			}

			r.entry.operations.createDirectory(targetRelativePath)
			if err := r.preImage.InsertNode(path, false, isDir); err != nil {
				return fmt.Errorf("insert created directory node: %w", err)
			}

			return nil
		}

		if _, ok := preImagePaths[path]; ok {
			// The file already existed in the pre-image. This means the file has been updated
			// and we should remove the previous file before we can link the updated one in place.
			r.entry.operations.removeDirectoryEntry(targetRelativePath)
			if err := r.preImage.RemoveNode(path); err != nil {
				return fmt.Errorf("remove node: %w", err)
			}
		}

		fileID, err := r.entry.stageFile(filepath.Join(r.snapshotRoot, r.relativePath, path))
		if err != nil {
			return fmt.Errorf("stage file: %w", err)
		}

		r.entry.operations.createHardLink(fileID, targetRelativePath, false)
		if err := r.preImage.InsertNode(path, false, isDir); err != nil {
			return fmt.Errorf("insert created file node: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walk creations pre-order: %w", err)
	}

	// Check if the deletion created directories. This can happen if Git has special cased a directory
	// to not be deleted, such as 'refs/heads', 'refs/tags' and 'refs/remotes'. Git may create the
	// directory to create a lock and leave it in place after the transaction.
	if err := deletions.WalkPreOrder(func(path string, isDir bool) error {
		info, err := os.Stat(filepath.Join(r.snapshotRoot, r.relativePath, path))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat for dir permissions: %w", err)
		}

		if _, ok := preImagePaths[path]; info != nil && !ok {
			r.entry.operations.createDirectory(filepath.Join(r.relativePath, path))
			if err := r.preImage.InsertNode(path, false, isDir); err != nil {
				return fmt.Errorf("insert node: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walk deletion directory creations pre-order: %w", err)
	}

	// Walk down from the leaves of reference deletions towards the root.
	if err := deletions.WalkPostOrder(func(path string, isDir bool) error {
		if _, ok := preImagePaths[path]; !ok {
			// If the path did not exist in the pre-image, no need to check further. The reference deletion
			// was targeting a reference in the packed-refs file, and not in the loose reference hierarchy.
			return nil
		}

		if _, err := os.Stat(filepath.Join(r.snapshotRoot, r.relativePath, path)); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stat for deletion: %w", err)
			}

			// The path doesn't exist in the post-image after the transaction was applied.
			r.entry.operations.removeDirectoryEntry(filepath.Join(r.relativePath, path))
			if err := r.preImage.RemoveNode(path); err != nil {
				return fmt.Errorf("remove node: %w", err)
			}
			return nil
		}

		// The path existed. Continue checking other paths whether they've been removed or not.
		return nil
	}); err != nil {
		return fmt.Errorf("walk deletions post-order: %w", err)
	}

	return nil
}

// StagePackedRefs should be called once there are no more changes to perform. It checks the
// packed-refs file for modifications, and logs it if it has been modified.
func (r *ReferenceRecorder) StagePackedRefs() error {
	preImageInode, err := GetInode(r.preImagePackedRefsPath)
	if err != nil {
		return fmt.Errorf("pre-image inode: %w", err)
	}

	postImageInode, err := GetInode(r.postImagePackedRefsPath)
	if err != nil {
		return fmt.Errorf("post-imaga inode: %w", err)
	}

	if preImageInode == postImageInode {
		return nil
	}

	packedRefsRelativePath := filepath.Join(r.relativePath, "packed-refs")
	if preImageInode > 0 {
		r.entry.operations.removeDirectoryEntry(packedRefsRelativePath)
	}

	if postImageInode > 0 {
		fileID, err := r.entry.stageFile(r.postImagePackedRefsPath)
		if err != nil {
			return fmt.Errorf("stage packed-refs: %w", err)
		}

		r.entry.operations.createHardLink(fileID, packedRefsRelativePath, false)
	}

	return nil
}
