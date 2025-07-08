package git

import (
	"fmt"
	"regexp"
)

var (
	// ReferenceBackendReftables denotes the new binary format based
	// reftable backend.
	// See https://www.git-scm.com/docs/reftable for more details.
	ReferenceBackendReftables = ReferenceBackend{
		Name: "reftable",
		updateRefErrorRegexs: updateRefErrorRegexs{
			InTransactionConflictRegex:   regexp.MustCompile(`^fatal: .*: cannot process '(.*)' and '(.*)' at the same time\n$`),
			MismatchingStateRegex:        regexp.MustCompile(`^fatal: (?:.*: )?cannot lock ref '(.*)': is at (.*) but expected (.*)\n$`),
			MultipleUpdatesRegex:         regexp.MustCompile(`^fatal: .*: multiple updates for ref '(.*)' not allowed\n$`),
			NonCommitObjectRegex:         regexp.MustCompile(`^fatal: .*: trying to write non-commit object (.*) to branch '(.*)'\n`),
			NonExistentObjectRegex:       regexp.MustCompile(`^fatal: .*: trying to write ref '(.*)' with nonexistent object (.*)\n$`),
			RefInvalidFormatRegex:        regexp.MustCompile(`^fatal: invalid ref format: (.*)\n$`),
			RefLockedRegex:               regexp.MustCompile(`^fatal: (prepare|commit): cannot lock references\n`),
			ReferenceAlreadyExistsRegex:  regexp.MustCompile(`^fatal: .*: cannot lock ref '(.*)': reference already exists\n$`),
			ReferenceExistsConflictRegex: regexp.MustCompile(`^fatal: .*: '(.*)' exists; cannot create '(.*)'\n$`),
			// Reftable uses the same error as RefLockedRegex when there is an
			// existing tables.list.lock present in the repo.
			PackedRefsLockedRegex: regexp.MustCompile(""),
		},
	}

	// ReferenceBackendFiles denotes the traditional filesystem based
	// reference backend.
	ReferenceBackendFiles = ReferenceBackend{
		Name: "files",
		updateRefErrorRegexs: updateRefErrorRegexs{
			InTransactionConflictRegex:   regexp.MustCompile(`^fatal: .*: cannot process '(.*)' and '(.*)' at the same time\n$`),
			MismatchingStateRegex:        regexp.MustCompile(`^fatal: (?:.*: )?cannot lock ref '(.*)': is at (.*) but expected (.*)\n$`),
			MultipleUpdatesRegex:         regexp.MustCompile(`^fatal: .*: multiple updates for ref '(.*)' not allowed\n$`),
			NonCommitObjectRegex:         regexp.MustCompile(`^fatal: .*: cannot update ref '.*': trying to write non-commit object (.*) to branch '(.*)'\n`),
			NonExistentObjectRegex:       regexp.MustCompile(`^fatal: .*: cannot update ref '.*': trying to write ref '(.*)' with nonexistent object (.*)\n$`),
			RefInvalidFormatRegex:        regexp.MustCompile(`^fatal: invalid ref format: (.*)\n$`),
			RefLockedRegex:               regexp.MustCompile(`^fatal: (prepare|commit): cannot lock ref '(.+?)': Unable to create '.*': File exists.`),
			ReferenceAlreadyExistsRegex:  regexp.MustCompile(`^fatal: .*: cannot lock ref '(.*)': reference already exists\n$`),
			ReferenceExistsConflictRegex: regexp.MustCompile(`^fatal: .*: cannot lock ref '.*': '(.*)' exists; cannot create '(.*)'\n$`),
			PackedRefsLockedRegex:        regexp.MustCompile(`(packed-refs\.lock': File exists\.\n)|(packed-refs\.new: File exists\n$)`),
		},
	}
)

type updateRefErrorRegexs struct {
	InTransactionConflictRegex   *regexp.Regexp
	MismatchingStateRegex        *regexp.Regexp
	MultipleUpdatesRegex         *regexp.Regexp
	NonCommitObjectRegex         *regexp.Regexp
	NonExistentObjectRegex       *regexp.Regexp
	RefInvalidFormatRegex        *regexp.Regexp
	RefLockedRegex               *regexp.Regexp
	ReferenceAlreadyExistsRegex  *regexp.Regexp
	ReferenceExistsConflictRegex *regexp.Regexp
	PackedRefsLockedRegex        *regexp.Regexp
}

// ReferenceBackend is the specific backend implementation of references.
type ReferenceBackend struct {
	updateRefErrorRegexs
	// Name is the name of the reference backend.
	Name string
}

// ReferenceBackendByName maps the output of `rev-parse --show-ref-format` to
// Gitaly's internal reference backend types.
func ReferenceBackendByName(name string) (ReferenceBackend, error) {
	switch name {
	case ReferenceBackendReftables.Name:
		return ReferenceBackendReftables, nil
	case ReferenceBackendFiles.Name:
		return ReferenceBackendFiles, nil
	default:
		return ReferenceBackend{}, fmt.Errorf("unknown reference backend: %q", name)
	}
}
