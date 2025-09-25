package snapshot

import (
	"context"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
)

var (
	// regexIncludePatterns contains the include path patterns.
	// When adding a new pattern to this list, ensure that all its prefix directories
	// are also added to the pattern.
	// For example, if you add a pattern like "^xx/yy/.*", you must also add "xx" and "xx/yy"
	// to pattern to maintain proper directory structure in the filter index.
	regexIncludePatterns = []*regexp.Regexp{
		// special case: including the repository root itself
		regexp.MustCompile(`^\.$`),

		// files that are allowed to be in the repo root
		regexp.MustCompile(`^config$`),
		regexp.MustCompile(`^\.gitaly-full-repack-timestamp$`),
		regexp.MustCompile(`^gitaly-language.stats$`),
		regexp.MustCompile(`^HEAD$`),
		regexp.MustCompile(`^packed-refs$`),

		// directories and files inside the objects directory
		regexp.MustCompile(`^objects$`),
		regexp.MustCompile(`^objects/[0-9a-f]{2}$`),
		regexp.MustCompile(`^objects/[0-9a-f]{2}/[0-9a-f]{38}(?:[0-9a-f]{24})?$`),
		regexp.MustCompile(`^objects/info$`),
		regexp.MustCompile(`^objects/info/commit-graphs$`),
		regexp.MustCompile(`^objects/info/commit-graphs/commit-graph-chain$`),
		regexp.MustCompile(`^objects/info/commit-graphs/.*\.graph$`),
		regexp.MustCompile(`^objects/info/alternates$`),
		regexp.MustCompile(`^objects/info/commit-graph$`),
		regexp.MustCompile(`^objects/pack$`),
		regexp.MustCompile(`^objects/pack/multi-pack-index$`),
		regexp.MustCompile(`^objects/pack/.*\.(pack|idx|rev|bitmap)$`),

		// include everything in custom_hooks/.
		regexp.MustCompile(`^custom_hooks$`),
		regexp.MustCompile(`^custom_hooks/.*`),

		// include reftable/table.list and files end in .ref only.
		regexp.MustCompile(`^reftable$`),
		regexp.MustCompile(`^reftable/tables.list$`),
		regexp.MustCompile(`^reftable/0x([[:xdigit:]]{12,16})-0x([[:xdigit:]]{12,16})-([0-9a-zA-Z]{8}).ref$`),

		// include everything in refs/. The exclusion patterns will filter out .lock files.
		regexp.MustCompile(`^refs$`),
		regexp.MustCompile(`^refs/.*`),
	}

	// regexExcludePatterns contains the path patterns used to exclude files from the snapshot.
	// Any path matching a pattern in this list will be filtered out.
	regexExcludePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^refs/.*\.lock$`),
	}
)

// Filter is an interface to determine whether a given path should be included in a snapshot.
type Filter interface {
	Matches(path string) bool
}

// FilterFunc is a function that implements the Filter interface.
type FilterFunc func(path string) bool

// Matches determines whether the path matches the filter criteria based on the provided function.
func (f FilterFunc) Matches(path string) bool {
	return f(path)
}

// NewDefaultFilter include everything.
func NewDefaultFilter(ctx context.Context) FilterFunc {
	return func(path string) bool {
		// When running leftover migration, we want to include all files to fully migrate the repository.
		if featureflag.LeftoverMigration.IsEnabled(ctx) {
			return true
		}

		// Don't include worktrees in the snapshot. All the worktrees in the repository should be leftover
		// state from before transaction management was introduced as the transactions would create their
		// worktrees in the snapshot.
		if path == housekeeping.WorktreesPrefix || path == housekeeping.GitlabWorktreePrefix {
			return false
		}
		return true
	}
}

// NewRegexSnapshotFilter creates a regex based filter to determine which files should be included in
// a repository snapshot based on a set of predefined regex patterns.
func NewRegexSnapshotFilter() FilterFunc {
	return func(path string) bool {
		for _, includePattern := range regexIncludePatterns {
			if includePattern.MatchString(path) {
				// If the file matches any include pattern, we still need to check if it also matches an exclude pattern.
				for _, excludePattern := range regexExcludePatterns {
					if excludePattern.MatchString(path) {
						return false
					}
				}

				// No exclude pattern applies, so the file will be included.
				return true
			}
		}

		// The file does not match any include pattern, so it will be excluded.
		return false
	}
}
