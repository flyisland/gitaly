package git

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
)

// Version represents the version of git itself.
type Version struct {
	// versionString is the string representation of the version. The string representation is
	// not directly derived from the parsed version information because we do not extract all
	// information from the original version string. Deriving it from parsed information would
	// thus potentially lose information.
	versionString       string
	major, minor, patch uint32
	rc                  bool
	gl                  uint32
}

// NewVersion constructs a new Git version from the given components.
func NewVersion(major, minor, patch, gl uint32) Version {
	return Version{
		versionString: fmt.Sprintf("%d.%d.%d.gl%d", major, minor, patch, gl),
		major:         major,
		minor:         minor,
		patch:         patch,
		gl:            gl,
	}
}

// NewRCVersion constructs a new Git RC version from the given components.
func NewRCVersion(major, minor, patch, gl uint32) Version {
	return Version{
		versionString: fmt.Sprintf("%d.%d.%d.gl%d.RC", major, minor, patch, gl),
		major:         major,
		minor:         minor,
		patch:         patch,
		gl:            gl,
		rc:            true,
	}
}

// ParseVersionOutput parses output returned by git-version(1). It is expected to be in the format
// "git version 2.39.1.gl1".
func ParseVersionOutput(versionOutput []byte) (Version, error) {
	trimmedVersionOutput := string(bytes.Trim(versionOutput, " \n"))
	versionString := strings.SplitN(trimmedVersionOutput, " ", 3)
	if len(versionString) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %q", string(versionOutput))
	}

	version, err := ParseVersion(versionString[2])
	if err != nil {
		return Version{}, fmt.Errorf("cannot parse git version: %w", err)
	}

	return version, nil
}

// String returns the string representation of the version.
func (v Version) String() string {
	return v.versionString
}

// IsCatfileObjectTypeFilterSupported checks whether the current Git version supports the `git cat-file --filter=`
// option.
func (v Version) IsCatfileObjectTypeFilterSupported() bool {
	return v.GreaterOrEqual(NewVersion(2, 49, 0, 2))
}

// UsesBatchedUpdates checks whether the current Git version uses batched updates in 'git-fetch(1)' and
// 'git-receive-pack(1)'.
func (v Version) UsesBatchedUpdates(ctx context.Context) bool {
	return v.GreaterOrEqual(NewVersion(2, 51, 0, 0)) || featureflag.GitMaster.IsEnabled(ctx)
}

// LessThan determines whether the version is older than another version.
func (v Version) LessThan(other Version) bool {
	switch {
	case v.major < other.major:
		return true
	case v.major > other.major:
		return false

	case v.minor < other.minor:
		return true
	case v.minor > other.minor:
		return false

	case v.patch < other.patch:
		return true
	case v.patch > other.patch:
		return false

	case v.rc && !other.rc:
		return true
	case !v.rc && other.rc:
		return false

	case v.gl < other.gl:
		return true
	case v.gl > other.gl:
		return false

	default:
		// this should only be reachable when versions are equal
		return false
	}
}

// Equal determines whether the version is the same as another version.
func (v Version) Equal(other Version) bool {
	return v == other
}

// GreaterOrEqual determines whether the version is newer than or equal to another version.
func (v Version) GreaterOrEqual(other Version) bool {
	return !v.LessThan(other)
}

// ParseVersion parses a git version string.
func ParseVersion(versionStr string) (Version, error) {
	versionSplit := strings.SplitN(versionStr, ".", 4)
	if len(versionSplit) < 3 {
		return Version{}, fmt.Errorf("expected major.minor.patch in %q", versionStr)
	}

	ver := Version{
		versionString: versionStr,
	}

	for i, v := range []*uint32{&ver.major, &ver.minor, &ver.patch} {
		var n64 uint64

		if versionSplit[i] == "GIT" {
			// Git falls back to vx.x.GIT if it's unable to describe the current version
			// or if there's a version file. We should just treat this as "0", even
			// though it may have additional commits on top.
			n64 = 0
		} else {
			rcSplit := strings.SplitN(versionSplit[i], "-", 2)

			var err error
			n64, err = strconv.ParseUint(rcSplit[0], 10, 32)
			if err != nil {
				return Version{}, err
			}

			if len(rcSplit) == 2 && strings.HasPrefix(rcSplit[1], "rc") {
				ver.rc = true
			}
		}

		*v = uint32(n64)
	}

	if len(versionSplit) == 4 {
		if strings.HasPrefix(versionSplit[3], "rc") {
			ver.rc = true
		} else if strings.HasPrefix(versionSplit[3], "gl") {
			gitlabPatchLevel := versionSplit[3][2:]

			gl, err := strconv.ParseUint(gitlabPatchLevel, 10, 32)
			if err != nil {
				return Version{}, err
			}

			ver.gl = uint32(gl)
		}
	}

	return ver, nil
}
