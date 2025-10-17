package gitcmd

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// Repository is the common interface of different repository implementations.
type Repository interface {
	// ResolveRevision tries to resolve the given revision to its object
	// ID. This uses the typical DWIM mechanism of git, see gitrevisions(1)
	// for accepted syntax. This will not verify whether the object ID
	// exists. To do so, you can peel the reference to a given object type,
	// e.g. by passing `refs/heads/master^{commit}`.
	ResolveRevision(ctx context.Context, revision git.Revision) (git.ObjectID, error)
	// HasBranches returns whether the repository has branches.
	HasBranches(ctx context.Context) (bool, error)
	// GetDefaultBranch returns the default branch of the repository.
	GetDefaultBranch(ctx context.Context) (git.ReferenceName, error)
	// HeadReference returns the reference that HEAD points to for the
	// repository.
	HeadReference(ctx context.Context) (git.ReferenceName, error)
}

// RepositoryExecutor is an interface which allows execution of Git commands in a specific
// repository.
type RepositoryExecutor interface {
	storage.Repository
	Exec(ctx context.Context, cmd Command, opts ...CmdOpt) (*command.Command, error)
	ExecAndWait(ctx context.Context, cmd Command, opts ...CmdOpt) error
	GitVersion(ctx context.Context) (git.Version, error)
	ObjectHash(ctx context.Context) (git.ObjectHash, error)
	ReferenceBackend(ctx context.Context) (git.ReferenceBackend, error)
}
