package repoutil

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
)

// CustomHooksDir is the directory in which the custom hooks are stored in the repository.
// It's also the directory where the hooks are stored in the TAR archive containing the hooks.
const CustomHooksDir = "custom_hooks"

// GetCustomHooks fetches the git hooks for a repository. The hooks are written
// to writer as a tar archive containing a `custom_hooks` directory. If no
// hooks are present in the repository, the response will have no data.
func GetCustomHooks(
	ctx context.Context,
	logger log.Logger,
	repoPath string,
	writer io.Writer,
) error {
	if _, err := os.Lstat(filepath.Join(repoPath, CustomHooksDir)); os.IsNotExist(err) {
		return nil
	}

	if err := archive.WriteTarball(ctx, logger, writer, repoPath, CustomHooksDir); err != nil {
		return structerr.NewInternal("archiving hooks: %w", err)
	}

	return nil
}

// ExtractHooks unpacks a tar file containing custom hooks into a `custom_hooks`
// directory at the specified path. If stripPrefix is set, the hooks are extracted directly
// to the target directory instead of in a `custom_hooks` directory in the target directory.
func ExtractHooks(ctx context.Context, logger log.Logger, reader io.Reader, path string, stripPrefix bool) error {
	// GNU tar does not accept an empty file as a valid tar archive and produces
	// an error. Since an empty hooks tar is symbolic of a repository having no
	// hooks, the reader is peeked to check if there is any data present.
	buf := bufio.NewReader(reader)
	if _, err := buf.Peek(1); errors.Is(err, io.EOF) {
		return nil
	}

	stripComponents := "0"
	if stripPrefix {
		stripComponents = "1"
	}

	cmdArgs := []string{"-xf", "-", "-C", path, "--strip-components", stripComponents, CustomHooksDir}

	var stderrBuilder strings.Builder
	cmd, err := command.New(ctx, logger, append([]string{"tar"}, cmdArgs...),
		command.WithStdin(buf),
		command.WithStderr(&stderrBuilder))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		stderr := stderrBuilder.String()

		// GNU and BSD tar versions have differing errors when attempting to
		// extract specified members from a valid tar archive. If the tar
		// archive is valid the errors for GNU and BSD tar should have the
		// same prefix, which can be checked to validate whether the expected
		// content is present in the archive for extraction.
		if strings.HasPrefix(stderr, "tar: custom_hooks: Not found in archive") {
			return nil
		}

		return structerr.New("waiting for tar command completion: %w", err).WithMetadata("stderr", stderr)
	}

	return nil
}

// SetCustomHooks transactionally and atomically sets custom hooks for a
// repository. The provided reader should be a tarball containing the custom
// hooks to be extracted to the specified Git repository.
func SetCustomHooks(
	ctx context.Context,
	logger log.Logger,
	locator storage.Locator,
	txManager transaction.Manager,
	reader io.Reader,
	repo storage.Repository,
) error {
	repoPath, err := locator.GetRepoPath(ctx, repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	var originalCustomHooksRelativePath string
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		originalRelativePath, err := filepath.Rel(tx.FS().Root(), repoPath)
		if err != nil {
			return fmt.Errorf("original relative path: %w", err)
		}

		originalCustomHooksRelativePath = filepath.Join(originalRelativePath, CustomHooksDir)

		// Log a deletion of the existing custom hooks so they are removed before the
		// new ones are put in place.
		if err := storage.RecordDirectoryRemoval(
			tx.FS(), tx.FS().Root(), originalCustomHooksRelativePath,
		); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("record custom hook removal: %w", err)
		}
	}

	// Cast a preparing vote to acquire a distributed lock
	preparingVoteHash := voting.NewVoteHash()
	if _, err := preparingVoteHash.Write([]byte("preparing set custom hooks")); err != nil {
		return fmt.Errorf("preparing vote hash: %w", err)
	}
	if err := voteCustomHooks(ctx, txManager, &preparingVoteHash, voting.Preparing); err != nil {
		return fmt.Errorf("casting preparing vote: %w", err)
	}

	// The `custom_hooks` directory in the repository is locked to prevent
	// concurrent modification of hooks.
	hooksLock, err := safe.NewLockingDirectory(repoPath, CustomHooksDir)
	if err != nil {
		return fmt.Errorf("creating hooks lock: %w", err)
	}

	if err := hooksLock.Lock(); err != nil {
		return fmt.Errorf("locking hooks: %w", err)
	}
	defer func() {
		// If the `.lock` file is not removed from the `custom_hooks` directory,
		// future modifications to the repository's hooks will be prevented. If
		// this occurs, the `.lock` file will have to be manually removed.
		if err := hooksLock.Unlock(); err != nil {
			logger.WithError(err).ErrorContext(ctx, "failed to unlock hooks")
		}
	}()

	// Create a temporary directory to write the new hooks to and also
	// temporarily store the current repository hooks. This enables "atomic"
	// directory swapping by acting as an intermediary storage location between
	// moves.
	tmpDir, err := tempdir.NewWithoutContext(repo.GetStorageName(), logger, locator)
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(tmpDir.Path()); err != nil {
			logger.WithError(err).WarnContext(ctx, "failed to remove temporary directory")
		}
	}()

	if err := ExtractHooks(ctx, logger, reader, tmpDir.Path(), false); err != nil {
		return fmt.Errorf("extracting hooks: %w", err)
	}

	tempHooksPath := filepath.Join(tmpDir.Path(), CustomHooksDir)

	// No hooks will be extracted if the tar archive is empty. If this happens
	// it means the repository should be set with an empty `custom_hooks`
	// directory. Create `custom_hooks` in the temporary directory so that any
	// existing repository hooks will be replaced with this empty directory.
	if err := os.Mkdir(tempHooksPath, mode.Directory); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("making temp hooks directory: %w", err)
	}

	preparedVote, err := newDirectoryVote(tempHooksPath)
	if err != nil {
		return fmt.Errorf("generating prepared vote: %w", err)
	}

	// Cast prepared vote with hash of the extracted archive in the temporary
	// `custom_hooks` directory.
	if err := voteCustomHooks(ctx, txManager, preparedVote, voting.Prepared); err != nil {
		return fmt.Errorf("casting prepared vote: %w", err)
	}

	repoHooksPath := filepath.Join(repoPath, CustomHooksDir)
	prevHooksPath := filepath.Join(tmpDir.Path(), "previous_hooks")

	// If the `custom_hooks` directory exists in the repository, move the
	// current hooks to `previous_hooks` in the temporary directory.
	if err := os.Rename(repoHooksPath, prevHooksPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("moving current hooks to temp: %w", err)
	}

	syncer := safe.NewSyncer()

	if storage.NeedsSync(ctx) {
		// Sync the custom hooks in the temporary directory before being moved into
		// the repository. This makes the move atomic as there is no state where the
		// move succeeds, but the hook files themselves are not yet on the disk, or
		// are partially written.
		if err := syncer.SyncRecursive(ctx, tempHooksPath); err != nil {
			return fmt.Errorf("syncing extracted custom hooks: %w", err)
		}
	}

	// Move `custom_hooks` from the temporary directory to the repository.
	if err := os.Rename(tempHooksPath, repoHooksPath); err != nil {
		return fmt.Errorf("moving new hooks to repo: %w", err)
	}

	if storage.NeedsSync(ctx) {
		// Sync the parent directory after a move to ensure the directory entry of the
		// hooks directory is flushed to the disk.
		if err := syncer.SyncParent(ctx, repoHooksPath); err != nil {
			return fmt.Errorf("syncing custom hooks parent directory: %w", err)
		}
	}

	committedVote, err := newDirectoryVote(repoHooksPath)
	if err != nil {
		return fmt.Errorf("generating committed vote: %w", err)
	}

	// Cast committed vote with hash of the extracted archive in the repository
	// `custom_hooks` directory.
	if err := voteCustomHooks(ctx, txManager, committedVote, voting.Committed); err != nil {
		return fmt.Errorf("casting committed vote: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := storage.RecordDirectoryCreation(
			tx.FS(), originalCustomHooksRelativePath,
		); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("record custom hook creation: %w", err)
		}
	}

	return nil
}

// newDirectoryVote creates a voting.VoteHash by walking the specified path and
// generating a hash based on file name, permissions, and data.
func newDirectoryVote(basePath string) (*voting.VoteHash, error) {
	voteHash := voting.NewVoteHash()

	if err := filepath.WalkDir(basePath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return fmt.Errorf("getting relative path: %w", err)
		}

		// Write file relative path to hash.
		_, _ = voteHash.Write([]byte(relPath))

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("getting file info: %w", err)
		}

		// Write file permissions to hash.
		permBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(permBytes, uint32(info.Mode()))
		_, _ = voteHash.Write(permBytes)

		if entry.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file: %w", err)
		}
		defer file.Close()

		// Copy file data to hash.
		if _, err = io.Copy(voteHash, file); err != nil {
			return fmt.Errorf("copying file to hash: %w", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}

	return &voteHash, nil
}

// voteCustomHooks casts a vote symbolic of the custom hooks received. If there
// is no transaction voting is skipped.
func voteCustomHooks(
	ctx context.Context,
	txManager transaction.Manager,
	v *voting.VoteHash,
	phase voting.Phase,
) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	vote, err := v.Vote()
	if err != nil {
		return err
	}

	if err := txManager.Vote(ctx, tx, vote, phase); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}
