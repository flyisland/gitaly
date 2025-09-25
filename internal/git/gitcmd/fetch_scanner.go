package gitcmd

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
)

// RefUpdateType represents the type of update a FetchStatusLine is. The
// valid types are documented here: https://git-scm.com/docs/git-fetch/2.30.0#Documentation/git-fetch.txt-flag
type RefUpdateType byte

// Valid checks whether the RefUpdateType is one of the seven valid types of update
func (t RefUpdateType) Valid() bool {
	_, ok := ValidRefUpdateTypes[t]

	return ok
}

// FetchStatusLine represents a line of status output from `git fetch`, as
// documented here: https://git-scm.com/docs/git-fetch/2.30.0#_output. Each
// line is a change to a git reference in the local repository that was caused
// by the fetch
type FetchStatusLine struct {
	// Type encodes the kind of change that git fetch has made
	Type RefUpdateType
	// Summary is a brief description of the change. This may be text such as
	// [new tag], or a compact-form SHA range showing the old and new values of
	// the updated reference, depending on the type of update
	Summary string
	// From is usually the name of the remote ref being fetched from, missing
	// the refs/<type>/ prefix. If a ref is being deleted, this will be "(none)"
	From string
	// To is the name of the local ref being updated, missing the refs/<type>/
	// prefix.
	To string
	// Reason optionally contains human-readable information about the change. It
	// is typically used to explain why making a given change failed (e.g., the
	// type will be RefUpdateTypeUpdateFailed). It may be empty.
	Reason string
}

const (
	// RefUpdateTypeFastForwardUpdate represents a 'fast forward update' fetch status line
	RefUpdateTypeFastForwardUpdate RefUpdateType = ' '
	// RefUpdateTypeForcedUpdate represents a 'forced update' fetch status line
	RefUpdateTypeForcedUpdate RefUpdateType = '+'
	// RefUpdateTypePruned represents a 'pruned' fetch status line
	RefUpdateTypePruned RefUpdateType = '-'
	// RefUpdateTypeTagUpdate represents a 'tag update' fetch status line
	RefUpdateTypeTagUpdate RefUpdateType = 't'
	// RefUpdateTypeFetched represents a 'fetched' fetch status line. This
	// indicates that a new reference has been created in the local repository
	RefUpdateTypeFetched RefUpdateType = '*'
	// RefUpdateTypeUpdateFailed represents an 'update failed' fetch status line
	RefUpdateTypeUpdateFailed RefUpdateType = '!'
	// RefUpdateTypeUnchanged represents an 'unchanged' fetch status line
	RefUpdateTypeUnchanged RefUpdateType = '='
)

// ValidRefUpdateTypes returns a list of struct of the seven valid types of update
var ValidRefUpdateTypes = map[RefUpdateType]struct{}{
	RefUpdateTypeFastForwardUpdate: {},
	RefUpdateTypeForcedUpdate:      {},
	RefUpdateTypePruned:            {},
	RefUpdateTypeTagUpdate:         {},
	RefUpdateTypeFetched:           {},
	RefUpdateTypeUpdateFailed:      {},
	RefUpdateTypeUnchanged:         {},
}

// IsTagAdded returns true if this status line indicates a new tag was added
func (f FetchStatusLine) IsTagAdded() bool {
	return f.Type == RefUpdateTypeFetched && f.Summary == "[new tag]"
}

// IsTagUpdated returns true if this status line indicates a tag was changed
func (f FetchStatusLine) IsTagUpdated() bool {
	return f.Type == RefUpdateTypeTagUpdate
}

// FetchScanner scans the output of `git fetch`, allowing information about
// the updated refs to be gathered
type FetchScanner struct {
	scanner  *bufio.Scanner
	lastLine FetchStatusLine
}

// NewFetchScanner returns a new FetchScanner
func NewFetchScanner(r io.Reader) *FetchScanner {
	return &FetchScanner{scanner: bufio.NewScanner(r)}
}

// Scan looks for the next fetch status line in the reader supplied to
// NewFetchScanner(). Any lines that are not valid status lines are discarded
// without error. It returns true if you should call Scan() again, and false if
// scanning has come to an end.
func (f *FetchScanner) Scan() bool {
	for f.scanner.Scan() {
		// Silently ignore non-matching lines
		line, ok := parseFetchStatusLine(f.scanner.Bytes())
		if !ok {
			continue
		}

		f.lastLine = line
		return true
	}

	return false
}

// Err returns any error encountered while scanning the reader supplied to
// NewFetchScanner(). Note that lines not matching the expected format are not
// an error.
func (f *FetchScanner) Err() error {
	return f.scanner.Err()
}

// StatusLine returns the most recent fetch status line encountered by the
// FetchScanner. It changes after each call to Scan(), unless there is an error.
func (f *FetchScanner) StatusLine() FetchStatusLine {
	return f.lastLine
}

// parseFetchStatusLine parses lines outputted by git-fetch(1), which are expected
// to be in the format " <flag> <summary> <from> -> <to> [<reason>]"
func parseFetchStatusLine(line []byte) (FetchStatusLine, bool) {
	var blank FetchStatusLine
	var out FetchStatusLine

	// Handle the flag very strictly, since status and non-status text mingle
	if len(line) < 4 || line[0] != ' ' || line[2] != ' ' {
		return blank, false
	}

	out.Type, line = RefUpdateType(line[1]), line[3:]
	if !out.Type.Valid() {
		return blank, false
	}

	// Get the summary, which may be composed of multiple words
	if line[0] == '[' {
		end := bytes.IndexByte(line, ']')
		if end < 0 || len(line) <= end+2 {
			return blank, false
		}

		out.Summary, line = string(line[0:end+1]), line[end+1:]
	} else {
		end := bytes.IndexByte(line, ' ')
		if end < 0 || len(line) <= end+1 {
			return blank, false
		}

		out.Summary, line = string(line[0:end]), line[end:]
	}

	// We're now scanning the "<from> -> <to>" part, where "<from>" is the remote branch name
	// while "<to>" is the local branch name transformed by the refspec. As branches cannot
	// contain whitespace, it's fine to scan by word now.
	scanner := bufio.NewScanner(bytes.NewReader(line))
	scanner.Split(bufio.ScanWords)

	// From field
	if !scanner.Scan() {
		return blank, false
	}
	out.From = scanner.Text()

	// Hardcoded -> delimiter
	if !scanner.Scan() || !bytes.Equal(scanner.Bytes(), []byte("->")) {
		return blank, false
	}

	// To field
	if !scanner.Scan() {
		return blank, false
	}
	out.To = scanner.Text()

	// Reason field - optional, the rest of the line. This implementation will
	// squeeze multiple spaces into one, but that shouldn't be a big problem
	var reason []string
	for scanner.Scan() {
		reason = append(reason, scanner.Text())
	}
	out.Reason = strings.Join(reason, " ")

	return out, true
}

// FetchPorcelainStatusLine represents a line of status output from `git fetch` when the porcelain
// option is enabled.
type FetchPorcelainStatusLine struct {
	Type      RefUpdateType
	OldOID    git.ObjectID
	NewOID    git.ObjectID
	Reference string
}

// FetchPorcelainScanner scans the output of `git fetch` when the porcelain option is enabled,
// allowing information about references to be gathered.
type FetchPorcelainScanner struct {
	scanner  *bufio.Scanner
	lastLine FetchPorcelainStatusLine
	hash     git.ObjectHash
	err      error
}

// NewFetchPorcelainScanner returns a FetchPorcelainScanner.
func NewFetchPorcelainScanner(r io.Reader, hash git.ObjectHash) *FetchPorcelainScanner {
	return &FetchPorcelainScanner{scanner: bufio.NewScanner(r), hash: hash}
}

// Scan parses the next fetch status line from the fetch command output.
func (f *FetchPorcelainScanner) Scan() bool {
	if f.scanner.Scan() {
		line, err := parseFetchPorcelainStatusLine(f.scanner.Bytes(), f.hash)
		if err != nil {
			f.err = err
			return false
		}

		f.lastLine = line
		return true
	}

	if f.scanner.Err() != nil {
		f.err = f.scanner.Err()
	}

	return false
}

// Err returns any error encountered while scanning.
func (f *FetchPorcelainScanner) Err() error {
	return f.err
}

// StatusLine returns the status line information from the last scanned line.
func (f *FetchPorcelainScanner) StatusLine() FetchPorcelainStatusLine {
	return f.lastLine
}

// parseFetchPorcelainStatusLine parses a fetch status line.
// The line format is as follows: <flag> <old-object-id> <new-object-id> <local-reference>
// Each token is delimited by a single space and each line ends with a new-line. Further format
// documentation can be found at https://git-scm.com/docs/git-fetch#_output.
func parseFetchPorcelainStatusLine(line []byte, hash git.ObjectHash) (FetchPorcelainStatusLine, error) {
	var status FetchPorcelainStatusLine

	// Unfortunately, the reference update type must be checked and parsed first. This is because
	// one of the possible values of the reference update type is <space>, which is also the same
	// character used as the delimiter for the remaining tokens in the status line. Therefore, the
	// line length is checked to make sure there will not be an out-of-bounds error when checking
	// for the remaining tokens and the update reference type is parsed/validated.
	if len(line) < 3 || line[1] != ' ' {
		return FetchPorcelainStatusLine{}, errors.New("invalid status line")
	}

	// First character in the line represents the status of the reference.
	status.Type = RefUpdateType(line[0])
	if !status.Type.Valid() {
		return FetchPorcelainStatusLine{}, fmt.Errorf("invalid reference update type: %q", status.Type)
	}

	// The remaining tokens in the status line can be safely split by the <space> delimiter.
	refStatus := bytes.Split(line[2:], []byte(" "))
	if len(refStatus) != 3 {
		return FetchPorcelainStatusLine{}, errors.New("invalid status line")
	}

	var err error
	status.OldOID, err = hash.FromHex(string(refStatus[0]))
	if err != nil {
		return FetchPorcelainStatusLine{}, fmt.Errorf("constructing old OID: %w", err)
	}

	status.NewOID, err = hash.FromHex(string(refStatus[1]))
	if err != nil {
		return FetchPorcelainStatusLine{}, fmt.Errorf("constructing new OID: %w", err)
	}

	status.Reference = string(refStatus[2])
	if err := git.ValidateReference(status.Reference); err != nil {
		return FetchPorcelainStatusLine{}, fmt.Errorf("validating reference: %w", err)
	}

	return status, nil
}
