package housekeeping

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
)

const (
	// OffloadingPromisorRemote is the name of the Git remote used for offloading.
	OffloadingPromisorRemote = "offload"

	// offloadingCacheDir represents the directory where offloading cache are stored.
	offloadingCacheDir = "offloading_cache"
)

// SetOffloadingGitConfig updates the Git configuration file to enable a repository for offloading by configuring
// settings specific to establishing a promisor remote.
//
// The praefectTxManager represents the voting-based Praefect transaction manager.
// When this helper function is used within the storage/partition transaction manager,
// praefectTxManager will naturally be nil. This is due to the differing underlying mechanisms
// between the storage/partition transaction manager and the praefectTxManager.
//
// As a result, the logic in repo.SetConfig will be adjusted to handle this scenario.
// This explanation will become obsolete and can be removed in the future.
func SetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo, url, filter string, praefectTxManager transaction.Manager) error {
	if url == "" {
		return fmt.Errorf("set offloading config: promisor remote url missing")
	}
	if filter == "" {
		return fmt.Errorf("set offloading config: filter missing")
	}

	configMap := map[string]string{
		fmt.Sprintf("remote.%s.url", OffloadingPromisorRemote):                url,
		fmt.Sprintf("remote.%s.promisor", OffloadingPromisorRemote):           "true",
		fmt.Sprintf("remote.%s.fetch", OffloadingPromisorRemote):              fmt.Sprintf("+refs/heads/*:refs/remotes/%s/*", OffloadingPromisorRemote),
		fmt.Sprintf("remote.%s.partialclonefilter", OffloadingPromisorRemote): filter,
	}

	for k, v := range configMap {
		if err := repo.SetConfig(ctx, k, v, praefectTxManager); err != nil {
			return fmt.Errorf("set Git config: %w", err)
		}
	}
	return nil
}

// ResetOffloadingGitConfig is the reverse operation of SetOffloadingGitConfig.
// It removes all Git configuration entries related to the offloading remote.
//
// The praefectTxManager represents the voting-based Praefect transaction manager.
// When this helper function is used within the storage/partition transaction manager,
// praefectTxManager will naturally be nil. This is due to the differing underlying mechanisms
// between the storage/partition transaction manager and the praefectTxManager.
//
// As a result, the logic in repo.SetConfig will be adjusted to handle this scenario.
// This explanation will become obsolete and can be removed in the future.
func ResetOffloadingGitConfig(ctx context.Context, repo *localrepo.Repo, praefectTxManager transaction.Manager) error {
	// Using regex here is safe as all offloading promisor remote-related configurations
	// are prefixed with "remote.<OffloadingPromisorRemote>". This follows the logic
	// in `SetOffloadingGitConfig`. If any new logic is added to `SetOffloadingGitConfig`
	// in the future, the reset logic should be updated accordingly.
	regex := fmt.Sprintf("remote.%s", OffloadingPromisorRemote)

	if err := repo.UnsetMatchingConfig(ctx, regex, praefectTxManager); err != nil {
		return fmt.Errorf("unset Git config: %w", err)
	}
	return nil
}

// PerformRepackingForOffloading performs a full repacking task using the git-repack(1) command
// for the purpose of offloading. It also adds a .promisor file to mark the pack file
// as a promisor pack file, which is used specifically for offloading.
func PerformRepackingForOffloading(ctx context.Context, repo *localrepo.Repo, filter, filterToDir string) error {
	count, err := stats.LooseObjects(ctx, repo)
	if err != nil {
		return fmt.Errorf("count loose objects: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("loose objects when performing repack for offloading")
	}

	repackOpts := []gitcmd.Option{
		// Do a full repack.
		gitcmd.Flag{Name: "-a"},
		// Delete loose objects made redundant by this repack.
		gitcmd.Flag{Name: "-d"},
		// Don't include objects part of alternate.
		gitcmd.Flag{Name: "-l"},

		// Apply the specified filter to determine which objects to include in the repack.
		gitcmd.ValueFlag{Name: "--filter", Value: filter},

		// Relocate the pack files containing objects that match the specified filter to the designated directory.
		// The "pack" serves as a file prefix. The relocated pack files will reside in
		// filterToDir, and their filenames will adhere to the format:
		// pack-<hash>.[pack, idx, rev]
		gitcmd.ValueFlag{Name: "--filter-to", Value: filepath.Join(filterToDir, "pack")},
	}

	repackCfg := config.RepackObjectsConfig{
		WriteBitmap:         false,
		WriteMultiPackIndex: true,
	}

	if err := PerformRepack(ctx, repo, repackCfg, repackOpts...); err != nil {
		return fmt.Errorf("perform repack for offloading: %w", err)
	}

	// Add .promisor file. The .promisor file is used to identify pack files
	// as promisor pack files. This file has no content and serves solely as a
	// marker.
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return fmt.Errorf("getting repository path: %w", err)
	}
	packfilesPath := filepath.Join(repoPath, "objects", "pack")
	entries, err := os.ReadDir(packfilesPath)
	if err != nil {
		return fmt.Errorf("reading packfiles directory: %w", err)
	}

	// The .promisor file should have the same name as the pack file.
	var packFile []string
	for _, entry := range entries {
		entryName := entry.Name()
		if strings.HasPrefix(entryName, "pack-") && strings.HasSuffix(entryName, ".pack") {
			packFile = append(packFile, strings.TrimSuffix(entryName, ".pack"))
		}

	}
	if len(packFile) != 1 {
		return fmt.Errorf("expect just one pack file")
	}
	err = os.WriteFile(filepath.Join(repoPath, "objects", "pack", packFile[0]+".promisor"), []byte{}, mode.File)
	if err != nil {
		return fmt.Errorf("create promisor file: %w", err)
	}

	return nil
}

// AddCacheAlternateEntry adds the cacheEntry to the `.git/objects/info/alternates` file at alternateFile.
// The caller is responsible to provide a valid cacheEntry which should be a relative path from the cache dir to the
// original offloaded repo's object directory.
func AddCacheAlternateEntry(alternateFile, cacheEntry string) (returnedErr error) {
	cacheEntry = "\n" + cacheEntry + "\n"
	tempFilePath := alternateFile + ".tmp"
	tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, mode.File)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() {
		if err := tempFile.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close file: %w", err))
		}
	}()

	if _, err := os.Stat(alternateFile); os.IsNotExist(err) {
		// When alternate not exist, just append cacheEntry to temp file. And later rename the temp file.
		if _, err := tempFile.WriteString(cacheEntry); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
	} else {
		// When alternate exist. Copy the content in alternate file to temp file, then append cacheEntry
		// to temp file. And later rename the temp file.

		alternateContent, err := os.ReadFile(alternateFile)
		if err != nil {
			return fmt.Errorf("read file: %w", err)
		}

		// Already has an offloading cache dir entry, no need to add another.
		if strings.Contains(string(alternateContent), offloadingCacheDir) {
			return fmt.Errorf("offloading cache entry is already added")
		}

		alternateContent = append(alternateContent, []byte(cacheEntry)...)
		if _, err := tempFile.Write(alternateContent); err != nil {
			return fmt.Errorf("write file: %w", err)
		}

	}

	if err := os.Rename(tempFilePath, alternateFile); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}
	return nil
}

// RemoveCacheAlternateEntry removes any line in the alternates file that contains the string "offloading_cache".
func RemoveCacheAlternateEntry(alternateFile string) (returnedErr error) {
	file, err := os.Open(alternateFile)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
		}
	}()

	var lines []string
	var hasOffloadingCacheEntry bool
	scanner := bufio.NewScanner(file)

	// Since the alternate file is not likely big, we can read the entire alternate file into memory.
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, offloadingCacheDir) {
			lines = append(lines, scanner.Text())
		} else {
			hasOffloadingCacheEntry = true
		}
	}

	if !hasOffloadingCacheEntry {
		// We don't need to modify alternates file since
		// no entry contains the string "offloading_cache".
		return nil
	}

	tempFilePath := alternateFile + ".tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() {
		if err := tempFile.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
		}
	}()

	// Write the updated lines to the temp file
	for _, line := range lines {
		if _, err := tempFile.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
	}
	if err := os.Rename(tempFilePath, alternateFile); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	return nil
}
