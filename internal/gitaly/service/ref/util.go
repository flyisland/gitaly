package ref

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strings"
	"syscall"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var localBranchFormatFields = []string{
	"%(refname)",
	"%(objectname)",
}

func parseRef(ref []byte, length int) ([][]byte, error) {
	elements := bytes.Split(ref, []byte("\x00"))
	if len(elements) != length {
		return nil, fmt.Errorf("error parsing ref %q", ref)
	}
	return elements, nil
}

func parseCommit(ref []byte) [][]byte {
	return bytes.Split(ref, []byte("\x00"))
}

func trimEmail(email []byte) []byte {
	return bytes.Trim(email, "<>")
}

func buildAllBranchesBranch(ctx context.Context, objectReader catfile.ObjectContentReader, elements [][]byte) (*gitalypb.FindAllBranchesResponse_Branch, error) {
	target, err := catfile.GetCommit(ctx, objectReader, git.Revision(elements[1]))
	if err != nil {
		return nil, err
	}

	return &gitalypb.FindAllBranchesResponse_Branch{
		Name:   elements[0],
		Target: target.GitCommit,
	}, nil
}

func buildBranch(ctx context.Context, objectReader catfile.ObjectContentReader, elements [][]byte) (*gitalypb.Branch, error) {
	target, err := catfile.GetCommit(ctx, objectReader, git.Revision(elements[1]))
	if err != nil {
		return nil, err
	}

	return &gitalypb.Branch{
		Name:         elements[0],
		TargetCommit: target.GitCommit,
	}, nil
}

func buildBranchWithoutCatfile(elements [][]byte) (*gitalypb.Branch, error) {
	var commit gitalypb.GitCommit
	var author, committer gitalypb.CommitAuthor

	if len(elements) != len(fullCommitFields) {
		return nil, fmt.Errorf("invalid field count: expected %d, got %d", len(fullCommitFields), len(elements))
	}

	for i, element := range elements {
		switch i {
		case 1:
			commit.Id = string(element)
		case 2:
			commit.Subject = element
		case 3:
			if len(element) > helper.MaxCommitOrTagMessageSize {
				element = element[:helper.MaxCommitOrTagMessageSize]
			}

			commit.Body = element
			commit.BodySize = int64(len(element))
		case 4:
			author.Name = element
		case 5:
			author.Email = trimEmail(element)
		case 6:
			authorDateSec := git.ParseDateSeconds(string(element))
			author.Date = &timestamppb.Timestamp{Seconds: authorDateSec}
		case 7:
			author.Timezone = element
		case 8:
			committer.Name = element
		case 9:
			committer.Email = trimEmail(element)
		case 10:
			committerDateSec := git.ParseDateSeconds(string(element))
			committer.Date = &timestamppb.Timestamp{Seconds: committerDateSec}
		case 11:
			committer.Timezone = element
		case 12:
			commit.SignatureType = git.DetectSignatureType(string(element))
		case 13:
			commit.TreeId = string(element)
		case 14:
			// applicable to commits with 2+ parents eg. "abc123 def456" → ["abc123", "def456"]
			parentIDs := strings.Fields(string(element))
			if len(parentIDs) > 0 {
				commit.ParentIds = parentIDs
			}
		}
	}

	commit.Author = &author
	commit.Committer = &committer

	return &gitalypb.Branch{
		Name:         elements[0],
		TargetCommit: &commit,
	}, nil
}

func newFindLocalBranchesWriter(stream gitalypb.RefService_FindLocalBranchesServer, objectReader catfile.ObjectContentReader) lines.Sender {
	return func(refs [][]byte) error {
		ctx := stream.Context()
		var response *gitalypb.FindLocalBranchesResponse

		var branches []*gitalypb.Branch

		for _, ref := range refs {
			elements, err := parseRef(ref, len(localBranchFormatFields))
			if err != nil {
				return err
			}

			branch, err := buildBranch(ctx, objectReader, elements)
			if err != nil {
				return err
			}

			branches = append(branches, branch)
		}

		response = &gitalypb.FindLocalBranchesResponse{LocalBranches: branches}

		return stream.Send(response)
	}
}

func newFindAllBranchesWriter(stream gitalypb.RefService_FindAllBranchesServer, objectReader catfile.ObjectContentReader) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*gitalypb.FindAllBranchesResponse_Branch
		ctx := stream.Context()

		for _, ref := range refs {
			elements, err := parseRef(ref, len(localBranchFormatFields))
			if err != nil {
				return err
			}
			branch, err := buildAllBranchesBranch(ctx, objectReader, elements)
			if err != nil {
				return err
			}
			branches = append(branches, branch)
		}
		return stream.Send(&gitalypb.FindAllBranchesResponse{Branches: branches})
	}
}

func newFindAllRemoteBranchesWriter(stream gitalypb.RefService_FindAllRemoteBranchesServer, objectReader catfile.ObjectContentReader) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*gitalypb.Branch
		ctx := stream.Context()

		for _, ref := range refs {
			elements, err := parseRef(ref, len(localBranchFormatFields))
			if err != nil {
				return err
			}
			branch, err := buildBranch(ctx, objectReader, elements)
			if err != nil {
				return err
			}
			branches = append(branches, branch)
		}

		return stream.Send(&gitalypb.FindAllRemoteBranchesResponse{Branches: branches})
	}
}

type findRefsOpts struct {
	cmdArgs []gitcmd.Option
	delim   byte
	lines.SenderOpts
	sortBy string
}

// Iterator is an iterator that iterates over a set of references
type Iterator interface {
	Next() bool
	Err() error
	Ref() *gitalypb.Branch
	Close() error
}

type commitIterator struct {
	reader         *bufio.Reader
	err            error
	currentBranch  *gitalypb.Branch
	stderr         bytes.Buffer
	numLines       int
	foundPageToken bool
	opts           *findRefsOpts
	done           bool
	cmd            *command.Command
	lineDelimiter  []byte
	accumulated    []byte
	buffer         []byte
}

var fullCommitFields = []string{
	"%(refname)",
	"%(objectname)",
	"%(subject)",
	"%(contents)",
	"%(authorname)",
	"%(authoremail)",
	"%(authordate:unix)",
	"%(authordate:format:%z)",
	"%(committername)",
	"%(committeremail)",
	"%(committerdate:unix)",
	"%(committerdate:format:%z)",
	"%(contents:signature)",
	"%(tree)",
	"%(parent)",
}

// NewBranchIterator creates a new iterator that populates branch information
func NewBranchIterator(
	ctx context.Context,
	repo gitcmd.RepositoryExecutor,
	opts *findRefsOpts,
	patterns []string,
) (Iterator, error) {
	// An extra character is necessary for the delimiter between lines
	// because there might be \n characters in the commit body.
	extraDelimiterChar := "\x03"
	c := &commitIterator{
		stderr:         bytes.Buffer{},
		opts:           opts,
		foundPageToken: !opts.PageTokenError,
		lineDelimiter:  []byte(extraDelimiterChar + "\n"),
		accumulated:    []byte{},
		buffer:         make([]byte, 4096),
	}

	options := []gitcmd.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		gitcmd.Flag{Name: "--format=" + strings.Join(fullCommitFields, "%00") + extraDelimiterChar},
	}

	if opts.sortBy != "" {
		options = append(options, gitcmd.Flag{Name: "--sort=" + opts.sortBy})
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "for-each-ref",
		Flags: options,
		Args:  patterns,
	}, gitcmd.WithSetupStdout(), gitcmd.WithStderr(&c.stderr))
	if err != nil {
		return nil, fmt.Errorf("spawning for-each-ref: %w", err)
	}

	c.cmd = cmd

	reader := bufio.NewReader(cmd)

	c.reader = reader

	return c, nil
}

// Next will advance the reader to the next line of git for-each-ref's output
// that includes all commit fields where each line is delimited by \n\x003.
func (c *commitIterator) Next() bool {
	if c.numLines >= c.opts.Limit {
		c.done = true
	}

	if c.done {
		return false
	}

	for {
		n, err := c.reader.Read(c.buffer)
		if n > 0 {
			c.accumulated = append(c.accumulated, c.buffer[:n]...)
		}

		if err != nil && err != io.EOF {
			c.err = err
			return false
		}

		if len(c.accumulated) == 0 {
			return false
		}

		// Look for delimiter
		for {
			idx := bytes.Index(c.accumulated, c.lineDelimiter)
			if idx == -1 {
				break
			}

			// Found delimiter
			record := c.accumulated[:idx]

			if !c.foundPageToken {
				c.foundPageToken = c.opts.IsPageToken(record)
				c.accumulated = c.accumulated[idx+len(c.lineDelimiter):]
				break
			}

			c.numLines++

			branch, err := buildBranchWithoutCatfile(bytes.Split(record, []byte("\x00")))
			if err != nil {
				c.err = err
				return false
			}

			c.currentBranch = branch

			// Remove processed part
			c.accumulated = c.accumulated[idx+len(c.lineDelimiter):]
			return true
		}
	}
}

func (c *commitIterator) Ref() *gitalypb.Branch {
	return c.currentBranch
}

func (c *commitIterator) Err() error {
	return c.err
}

func (c *commitIterator) Close() error {
	if c.opts.PageTokenError && !c.foundPageToken {
		return fmt.Errorf("sending lines: %w", lines.ErrInvalidPageToken)
	}

	if err := c.cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// When we have a limit set up and have sent all references upstream then the call to `Wait()` may
			// indeed cause us to tear down the still-running git-for-each-ref(1) process. Because we close stdout
			// before sending a signal the end result may be that the process will die with EPIPE because it failed
			// to write to stdout.
			//
			// This is an expected error though, and thus we ignore it here.
			status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
			if ok && status.Signaled() && status.Signal() == syscall.SIGPIPE {
				return nil
			}

			return structerr.New("listing failed with exit code %d", status.ExitStatus()).
				WithMetadata("stderr", c.stderr.String())
		}

		return fmt.Errorf("waiting for for-each-ref: %w", err)
	}

	return nil
}

func (s *server) findRefsWithIterator(
	ctx context.Context,
	chunker *chunk.Chunker,
	repo gitcmd.RepositoryExecutor,
	patterns []string,
	opts *findRefsOpts,
) (err error) {
	iterator, err := NewBranchIterator(ctx, repo, opts, patterns)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, iterator.Close())
	}()

	for iterator.Next() {
		if err := chunker.Send(iterator.Ref()); err != nil {
			return fmt.Errorf("sending refs: %w", err)
		}
	}

	if err := iterator.Err(); err != nil {
		return fmt.Errorf("iterating refs: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return fmt.Errorf("flushing refs: %w", err)
	}

	return nil
}

func (s *server) findRefs(ctx context.Context, writer lines.Sender, repo gitcmd.RepositoryExecutor, patterns []string, opts *findRefsOpts) error {
	var options []gitcmd.Option

	if len(opts.cmdArgs) == 0 {
		options = append(options, gitcmd.Flag{Name: "--format=%(refname)"}) // Default format
	} else {
		options = append(options, opts.cmdArgs...)
	}

	var stderr strings.Builder
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "for-each-ref",
		Flags: options,
		Args:  patterns,
	}, gitcmd.WithSetupStdout(), gitcmd.WithStderr(&stderr))
	if err != nil {
		return fmt.Errorf("spawning for-each-ref: %w", err)
	}

	if err := lines.Send(cmd, writer, lines.SenderOpts{
		IsPageToken:    opts.IsPageToken,
		Delimiter:      opts.delim,
		Limit:          opts.Limit,
		PageTokenError: opts.PageTokenError,
	}); err != nil {
		return fmt.Errorf("sending lines: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// When we have a limit set up and have sent all references upstream then the call to `Wait()` may
			// indeed cause us to tear down the still-running git-for-each-ref(1) process. Because we close stdout
			// before sending a signal the end result may be that the process will die with EPIPE because it failed
			// to write to stdout.
			//
			// This is an expected error though, and thus we ignore it here.
			status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
			if ok && status.Signaled() && status.Signal() == syscall.SIGPIPE {
				return nil
			}

			return structerr.New("listing failed with exit code %d", status.ExitStatus()).
				WithMetadata("stderr", stderr.String())
		}

		return fmt.Errorf("waiting for for-each-ref: %w", err)
	}

	return nil
}

type paginationOpts struct {
	// Limit allows to set the maximum numbers of elements
	Limit int
	// IsPageToken allows control over which results are sent as part of the
	// response. When IsPageToken evaluates to true for the first time,
	// results will start to be sent as part of the response. This function
	// will	be called with an empty slice previous to sending the first line
	// in order to allow sending everything right from the beginning.
	IsPageToken func([]byte) bool
	// When PageTokenError is true then the response will return an error when
	// PageToken is not found.
	PageTokenError bool
}

func buildPaginationOpts(ctx context.Context, p *gitalypb.PaginationParameter) *paginationOpts {
	opts := &paginationOpts{}
	opts.IsPageToken = func(_ []byte) bool { return true }
	opts.Limit = math.MaxInt32

	if p == nil {
		return opts
	}

	if p.GetLimit() >= 0 {
		opts.Limit = int(p.GetLimit())
	}

	if p.GetPageToken() != "" {
		opts.IsPageToken = func(line []byte) bool {
			// Only use the first part of the line before \x00 separator
			if nullByteIndex := bytes.IndexByte(line, 0); nullByteIndex != -1 {
				line = line[:nullByteIndex]
			}

			return bytes.Equal(line, []byte(p.GetPageToken()))
		}
		opts.PageTokenError = true
	}

	return opts
}

func buildFindRefsOpts(ctx context.Context, p *gitalypb.PaginationParameter) *findRefsOpts {
	opts := buildPaginationOpts(ctx, p)

	refsOpts := &findRefsOpts{delim: '\n'}
	refsOpts.Limit = opts.Limit
	refsOpts.IsPageToken = opts.IsPageToken
	refsOpts.PageTokenError = opts.PageTokenError

	return refsOpts
}
