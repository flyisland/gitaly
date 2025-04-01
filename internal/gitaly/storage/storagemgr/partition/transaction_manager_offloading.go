package partition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gocloud.dev/gcerrors"
)

const (
	packFileDir = "pack"
	objectsDir  = "objects"
	configFile  = "config"
)

var (
	errOffloadingObjectUpload = errors.New("upload to offloading storage")
	errOffloadingOnRepacking  = errors.New("repack for offloading")
)

// runOffloading models offloading tasks. It stores configuration information needed to offload a repository.
type runOffloading struct {
	config housekeepingcfg.OffloadingConfig
}

// OffloadRepository configures a transaction to run an offloading task
// by setting the runOffloading struct.
func (txn *Transaction) OffloadRepository(cfg housekeepingcfg.OffloadingConfig) {
	if txn.runHousekeeping == nil {
		txn.runHousekeeping = &runHousekeeping{}
	}
	txn.runHousekeeping.runOffloading = &runOffloading{
		config: cfg,
	}
}

// prepareOffloading implements the core offloading logic within the transaction manager.
// It must be called inside a snapshot repository and performs these steps:
//
//   - Repacks the repository based on the provided filter. After repacking,
//     the repository's objects are split into two groups: one group for uploading to the
//     offloading storage and another to be kept locally.
//
//   - Uploads objects marked for offloading.
//
//   - Updates configurations (git config file, alternates file).
//
//   - Records all file changes in the WAL.
func (mgr *TransactionManager) prepareOffloading(ctx context.Context, transaction *Transaction) (returnedErr error) {
	if transaction.runHousekeeping.runOffloading == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareOffloading", nil)
	defer span.Finish()

	// Loading configurations for offloading
	cfg := transaction.runHousekeeping.runOffloading.config

	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	// workingRepoPath is the current repository path which we are performing operations on.
	// In the context of transaction, workingRepoPath is a snapshot repository.
	workingRepoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	// Capture the list of pack-files before repacking.
	oldPackFiles, err := mgr.collectPackFiles(ctx, workingRepoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	// Creating a temporary directory where we will put the pack files (the ones to be offloaded)
	filterToBase, err := os.MkdirTemp(workingRepoPath, "gitaly-offloading-*")
	if err != nil {
		return fmt.Errorf("create directory %s: %w", filterToBase, err)
	}
	filterToDir := filepath.Join(filterToBase, objectsDir, packFileDir)
	if err := os.MkdirAll(filterToDir, mode.Directory); err != nil {
		return fmt.Errorf("create directory %s: %w", filterToDir, err)
	}

	// Repack the repository
	if err := housekeeping.PerformRepackingForOffloading(ctx, workingRepository, cfg.Filter, filterToDir); err != nil {
		return errors.Join(errOffloadingOnRepacking, err)
	}
	packFilesToUpload, err := mgr.collectPackFiles(ctx, filterToBase)
	if err != nil {
		return fmt.Errorf("collect old pack files: %w", err)
	}

	uploadedPackFiles := make([]string, 0, len(packFilesToUpload))
	defer func() {
		// If returnedErr is non-nil, attempt to remove the uploaded file.
		// This is a best-effort cleanup; we can't guarantee successful deletion.
		// If there is an error, the error is returned to the caller together with the returnedErr.
		// Any undeleted files will eventually be removed by the garbage collection job.
		if returnedErr != nil {
			deletionErrors := mgr.sink.DeleteObjects(ctx, cfg.Prefix, uploadedPackFiles)
			for _, err := range deletionErrors {
				if gcerrors.Code(err) != gcerrors.NotFound {
					returnedErr = errors.Join(returnedErr, err)
				}
			}
		}
	}()
	for file := range packFilesToUpload {
		if err := mgr.sink.Upload(ctx, filepath.Join(filterToDir, file), cfg.Prefix); err != nil {
			return errors.Join(errOffloadingObjectUpload, err)
		}
		uploadedPackFiles = append(uploadedPackFiles, file)
	}

	promisorRemoteURL := filepath.Join(cfg.SinkURL, cfg.Prefix)
	if err := housekeeping.SetOffloadingGitConfig(ctx, workingRepository, promisorRemoteURL, cfg.Filter, nil); err != nil {
		return fmt.Errorf("setting offloading git config: %w", err)
	}

	// Add cache entry
	alternatesInWorkingRepo := stats.AlternatesFilePath(workingRepoPath)
	// We are calculating the relative path of the cache entry based on the original repo path instead of the
	// snapshot repo path here. This is because the alternate file will eventually be copied back to the
	// original repo after offloading is completed. If we calculate relative to the snapshot repo path,
	// it will not work.
	relativeCacheEntry, err := filepath.Rel(filepath.Join(cfg.OriginalRepo, objectsDir), cfg.CachePath)
	if err != nil {
		return fmt.Errorf("find relative cache entry: %w", err)
	}
	if err := housekeeping.AddCacheAlternateEntry(alternatesInWorkingRepo, relativeCacheEntry); err != nil {
		return fmt.Errorf("adding cache alternate entry: %w", err)
	}

	for file := range oldPackFiles {
		transaction.walEntry.RemoveDirectoryEntry(filepath.Join(transaction.relativePath, objectsDir, packFileDir, file))
	}

	newPackFilesToStay, err := mgr.collectPackFiles(ctx, workingRepoPath)
	if err != nil {
		return fmt.Errorf("collect new pack files: %w", err)
	}
	for file := range newPackFilesToStay {
		fileRelativePath := filepath.Join(transaction.relativePath, objectsDir, packFileDir, file)
		if err := transaction.walEntry.CreateFile(
			filepath.Join(transaction.snapshot.Root(), fileRelativePath),
			fileRelativePath,
		); err != nil {
			return fmt.Errorf("record pack-file creation: %w", err)
		}
	}

	transaction.walEntry.RemoveDirectoryEntry(filepath.Join(transaction.relativePath, configFile))
	if err := transaction.walEntry.CreateFile(
		filepath.Join(workingRepoPath, configFile),
		filepath.Join(transaction.relativePath, configFile),
	); err != nil {
		return fmt.Errorf("record config file replacement: %w", err)
	}

	if _, err := os.Stat(stats.AlternatesFilePath(workingRepoPath)); !os.IsNotExist(err) {
		transaction.walEntry.RemoveDirectoryEntry(stats.AlternatesFilePath(transaction.relativePath))
	}

	if err := transaction.walEntry.CreateFile(
		stats.AlternatesFilePath(workingRepoPath),
		stats.AlternatesFilePath(transaction.relativePath),
	); err != nil {
		return fmt.Errorf("record alternates file replacement: %w", err)
	}

	return nil
}
