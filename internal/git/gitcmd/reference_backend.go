package gitcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
)

var regexpReferenceBackend = regexp.MustCompile(`^[ \t]*(?i:refstorage)[ \t]*=[ \t]*(.*)[ \t;#]?`)

// DetectReferenceBackend detects the reference backend used by the repository.
// It plucks out the first refstorage configuration value from the repository's configuration
// file and doesn't validate the configuration.
//
// Note: It is recommended to use localrepo.ReferenceBackend since that value is
// cached for a given repository.
func DetectReferenceBackend(ctx context.Context, repoPath string) (_ git.ReferenceBackend, returnedErr error) {
	configFile, err := os.Open(filepath.Join(repoPath, "config"))
	if err != nil {
		return git.ReferenceBackend{}, fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err := configFile.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
		}
	}()

	backendName := git.ReferenceBackendFiles.Name
	scanner := bufio.NewScanner(configFile)
	for scanner.Scan() {
		if matches := regexpReferenceBackend.FindSubmatch(scanner.Bytes()); len(matches) > 0 {
			backendName = string(matches[1])
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return git.ReferenceBackend{}, fmt.Errorf("scan: %w", err)
	}

	return git.ReferenceBackendByName(backendName)
}
