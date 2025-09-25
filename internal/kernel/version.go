package kernel

import (
	"fmt"
	"regexp"
	"strconv"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

// Version models a kernel version.
type Version struct {
	// Major is the major version of the kernel.
	Major int
	// Minor is the minor version of the kernel.
	Minor int
}

// IsAtLeast checks whether the running kernel is at the given version or later.
func IsAtLeast(expected Version) (bool, error) {
	actual, err := getVersion()
	if err != nil {
		return false, fmt.Errorf("get version: %w", err)
	}

	return isAtLeast(expected, actual), nil
}

func isAtLeast(expected, actual Version) bool {
	if actual.Major > expected.Major {
		return true
	}

	return actual.Major == expected.Major && actual.Minor >= expected.Minor
}

// As the kernel version doesn't change across invocations, we're only retrieving it during
// the first call to getVersion, caching it, and using the cached version for all other calls.
var (
	cachedVersion Version
	errCached     error
	cacheOnce     sync.Once
)

func getVersion() (Version, error) {
	cacheOnce.Do(func() {
		release, err := getRelease()
		if err != nil {
			errCached = err
			return
		}

		cachedVersion, errCached = parseRelease(release)
	})

	return cachedVersion, errCached
}

var releaseRegexp = regexp.MustCompile(`^([0-9]+)\.([0-9]+)\.([0-9]+)`)

func parseRelease(release string) (Version, error) {
	matches := releaseRegexp.FindStringSubmatch(release)
	if len(matches) == 0 {
		return Version{}, structerr.New("unexpected release format").WithMetadata("release", release)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return Version{}, structerr.New("parse major: %w", err)
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return Version{}, structerr.New("parse minor: %w", err)
	}

	return Version{
		Major: major,
		Minor: minor,
	}, nil
}
