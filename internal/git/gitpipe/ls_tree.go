package gitpipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
)

// lsTreeConfig is configuration for the LsTree pipeline step.
type lsTreeConfig struct {
	recursive  bool
	typeFilter func(*localrepo.TreeEntry) bool
	skipResult func(*RevisionResult) (bool, error)
}

// LsTreeOption is an option for the LsTree pipeline step.
type LsTreeOption func(cfg *lsTreeConfig)

// LsTreeWithRecursive will make LsTree recursive into subtrees.
func LsTreeWithRecursive() LsTreeOption {
	return func(cfg *lsTreeConfig) {
		cfg.recursive = true
	}
}

// LsTreeWithBlobFilter configures LsTree to only pass through blob objects.
func LsTreeWithBlobFilter() LsTreeOption {
	return func(cfg *lsTreeConfig) {
		cfg.typeFilter = func(e *localrepo.TreeEntry) bool { return e.IsBlob() }
	}
}

// LsTreeWithSkip will execute the given function for each RevisionResult processed by the
// pipeline. If the callback returns `true`, then the object will be skipped and not passed down
// the pipeline.
func LsTreeWithSkip(skipResult func(*RevisionResult) (bool, error)) LsTreeOption {
	return func(cfg *lsTreeConfig) {
		cfg.skipResult = skipResult
	}
}

// LsTree runs git-ls-tree(1) for the given revisions. The returned channel will
// contain all object IDs listed by this command. This might include:
//   - Blobs
//   - Trees, unless you're calling it with LsTreeWithRecursive()
//   - Submodules, referring to the commit of the submodule
func LsTree(
	ctx context.Context,
	repo *localrepo.Repo,
	revision string,
	options ...LsTreeOption,
) RevisionIterator {
	var cfg lsTreeConfig
	for _, option := range options {
		option(&cfg)
	}

	resultChan := make(chan RevisionResult)
	go func() {
		defer close(resultChan)

		objectHash, err := repo.ObjectHash(ctx)
		if err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("detecting object hash: %w", err),
			})
			return
		}

		flags := []gitcmd.Option{
			gitcmd.Flag{Name: "-z"},
		}

		if cfg.recursive {
			flags = append(flags, gitcmd.Flag{Name: "-r"})
		}

		var stderr strings.Builder
		cmd, err := repo.Exec(ctx,
			gitcmd.Command{
				Name:  "ls-tree",
				Flags: flags,
				Args:  []string{revision},
			},
			gitcmd.WithStderr(&stderr),
			gitcmd.WithSetupStdout(),
		)
		if err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("spawning ls-tree: %w", err),
			})
			return
		}

		parser := localrepo.NewParser(cmd, objectHash)
		for {
			entry, err := parser.NextEntry()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				sendRevisionResult(ctx, resultChan, RevisionResult{
					err: fmt.Errorf("scanning ls-tree output: %w", err),
				})
				return
			}

			if cfg.typeFilter != nil && !cfg.typeFilter(entry) {
				continue
			}

			result := RevisionResult{
				OID:        entry.OID,
				ObjectName: []byte(entry.Path),
			}

			if cfg.skipResult != nil {
				skip, err := cfg.skipResult(&result)
				if err != nil {
					sendRevisionResult(ctx, resultChan, RevisionResult{
						err: fmt.Errorf("ls-tree skip: %w", err),
					})
					return
				}
				if skip {
					continue
				}
			}

			if isDone := sendRevisionResult(ctx, resultChan, result); isDone {
				return
			}
		}

		if err := cmd.Wait(); err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("ls-tree pipeline command: %w, stderr: %q", err, stderr.String()),
			})
			return
		}
	}()

	return &revisionIterator{
		ctx: ctx,
		ch:  resultChan,
	}
}
