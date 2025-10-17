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

var regexpObjectFormat = regexp.MustCompile(`^[ \t]*(?i:objectformat)[ \t]*=[ \t]*(.*)[ \t;#]?`)

// DetectObjectHash detects the object-hash used by the given repository. It plucks out the first
// objectformat configuration value from the repository's configuration file and doesn't validate
// the configuration.
//
// Note: It is recommended to use localrepo.ObjectHash since that value is
// cached for a given repository.
func DetectObjectHash(ctx context.Context, repoPath string) (_ git.ObjectHash, returnedErr error) {
	configFile, err := os.Open(filepath.Join(repoPath, "config"))
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err := configFile.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
		}
	}()

	objectFormat := git.ObjectHashSHA1.Format
	scanner := bufio.NewScanner(configFile)
	for scanner.Scan() {
		if matches := regexpObjectFormat.FindSubmatch(scanner.Bytes()); len(matches) > 0 {
			objectFormat = string(matches[1])
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return git.ObjectHash{}, fmt.Errorf("scan: %w", err)
	}

	return git.ObjectHashByFormat(objectFormat)
}
