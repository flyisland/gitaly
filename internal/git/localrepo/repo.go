package localrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/trace"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// Repo represents a local Git repository.
type Repo struct {
	storage.Repository
	logger        log.Logger
	locator       storage.Locator
	gitCmdFactory gitcmd.CommandFactory
	catfileCache  catfile.Cache

	detectObjectHash func(context.Context) (git.ObjectHash, error)
	detectRefBackend func(context.Context) (git.ReferenceBackend, error)
}

// New creates a new Repo from its protobuf representation.
func New(logger log.Logger, locator storage.Locator, gitCmdFactory gitcmd.CommandFactory, catfileCache catfile.Cache, repo storage.Repository) *Repo {
	var (
		detectObjectHashOnce sync.Once
		objectHash           git.ObjectHash
		objectHashErr        error

		detectRefBackendOnce sync.Once
		refBackend           git.ReferenceBackend
		refBackendErr        error
	)

	return &Repo{
		Repository:    repo,
		logger:        logger,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,

		// These are implemented as closures in order to make it safe to share these functions between
		// other localrepo instances derived from this one. The closures hide the details and avoid
		// copying the sync.Once used to facilitate the caching.
		detectObjectHash: func(ctx context.Context) (git.ObjectHash, error) {
			detectObjectHashOnce.Do(func() {
				path, err := locator.GetRepoPath(ctx, repo)
				if err != nil {
					objectHashErr = fmt.Errorf("get repo path: %w", err)
					return
				}

				objectHash, objectHashErr = gitcmd.DetectObjectHash(ctx, path)
			})

			return objectHash, objectHashErr
		},
		detectRefBackend: func(ctx context.Context) (git.ReferenceBackend, error) {
			detectRefBackendOnce.Do(func() {
				path, err := locator.GetRepoPath(ctx, repo)
				if err != nil {
					refBackendErr = fmt.Errorf("get repo path: %w", err)
					return
				}

				refBackend, refBackendErr = gitcmd.DetectReferenceBackend(ctx, path)
			})

			return refBackend, refBackendErr
		},
	}
}

// NewFrom creates a new Repo from its protobuf representation using dependencies of another Repo.
func NewFrom(other *Repo, repo storage.Repository) *Repo {
	return New(other.logger, other.locator, other.gitCmdFactory, other.catfileCache, repo)
}

// Quarantine return the repository quarantined. The quarantine directory becomes the repository's
// main object directory and the original object directory is configured as an alternate.
func (repo *Repo) Quarantine(ctx context.Context, quarantineDirectory string) (*Repo, error) {
	pbRepo, ok := repo.Repository.(*gitalypb.Repository)
	if !ok {
		return nil, fmt.Errorf("unexpected repository type %t", repo.Repository)
	}

	repoPath, err := repo.locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return nil, fmt.Errorf("repo path: %w", err)
	}

	quarantinedRepo, err := quarantine.Apply(repoPath, pbRepo, quarantineDirectory)
	if err != nil {
		return nil, fmt.Errorf("apply quarantine: %w", err)
	}

	quarantined := NewFrom(repo, quarantinedRepo)
	// Share the object hash and reference backend detection with the parent to avoid
	// re-resolving them.
	quarantined.detectObjectHash = repo.detectObjectHash
	quarantined.detectRefBackend = repo.detectRefBackend

	return quarantined, nil
}

// QuarantineOnly returns the repository with only the quarantine directory configured as an object
// directory by dropping the alternate object directories. Returns an error if the repository doesn't
// have a quarantine directory configured.
//
// Only the alternates configured in the *gitalypb.Repository object are dropped, not the alternates
// that could be in `objects/info/alternates`. Dropping the configured alternates does however also
// implicitly remove the `objects/info/alternates` in the alternate object directory since the file
// would exist there. The quarantine directory itself would not typically contain an
// `objects/info/alternates` file.
func (repo *Repo) QuarantineOnly() (*Repo, error) {
	pbRepo, ok := repo.Repository.(*gitalypb.Repository)
	if !ok {
		return nil, fmt.Errorf("unexpected repository type %t", repo.Repository)
	}

	cloneRepo := proto.Clone(pbRepo).(*gitalypb.Repository)
	cloneRepo.GitAlternateObjectDirectories = nil
	if cloneRepo.GetGitObjectDirectory() == "" {
		return nil, errors.New("repository wasn't quarantined")
	}

	return New(
		repo.logger,
		repo.locator,
		repo.gitCmdFactory,
		repo.catfileCache,
		cloneRepo,
	), nil
}

// NewTestRepo constructs a Repo. It is intended as a helper function for tests which assembles
// dependencies ad-hoc from the given config.
func NewTestRepo(tb testing.TB, cfg config.Cfg, repo storage.Repository, factoryOpts ...gitcmd.ExecCommandFactoryOption) *Repo {
	tb.Helper()

	if cfg.SocketPath != testcfg.UnconfiguredSocketPath {
		repo = gittest.RewrittenRepository(tb, testhelper.Context(tb), cfg, &gitalypb.Repository{
			StorageName:                   repo.GetStorageName(),
			RelativePath:                  repo.GetRelativePath(),
			GitObjectDirectory:            repo.GetGitObjectDirectory(),
			GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
		})
	}

	//nolint:forbidigo // We can't use the testhelper package here given that this is production code, so we can't
	//use `teshelper.NewDiscardingLogEntry()`.
	logrusLogger := logrus.New()
	logrusLogger.Out = io.Discard
	logger := log.FromLogrusEntry(logrus.NewEntry(logrusLogger))

	gitCmdFactory, cleanup, err := gitcmd.NewExecCommandFactory(cfg, logger, factoryOpts...)
	tb.Cleanup(cleanup)
	require.NoError(tb, err)

	catfileCache := catfile.NewCache(cfg)
	tb.Cleanup(catfileCache.Stop)

	locator := config.NewLocator(cfg)

	return New(logger, locator, gitCmdFactory, catfileCache, repo)
}

// Exec creates a git command with the given args and Repo, executed in the
// Repo. It validates the arguments in the command before executing.
func (repo *Repo) Exec(ctx context.Context, cmd gitcmd.Command, opts ...gitcmd.CmdOpt) (*command.Command, error) {
	refBackend, err := repo.ReferenceBackend(ctx)
	if err != nil {
		return nil, err
	}
	opts = append(opts, gitcmd.WithReferenceBackend(refBackend))

	return repo.gitCmdFactory.New(ctx, repo, cmd, opts...)
}

// ExecAndWait is similar to Exec, but waits for the command to exit before
// returning.
func (repo *Repo) ExecAndWait(ctx context.Context, cmd gitcmd.Command, opts ...gitcmd.CmdOpt) error {
	command, err := repo.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}

	return command.Wait()
}

// GitVersion returns the Git version in use.
func (repo *Repo) GitVersion(ctx context.Context) (git.Version, error) {
	return repo.gitCmdFactory.GitVersion(ctx)
}

func errorWithStderr(err error, stderr []byte) error {
	if len(stderr) == 0 {
		return err
	}
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}

// StorageTempDir returns the temporary dir for the storage where the repo is on.
// When this directory does not exist yet, it's being created.
func (repo *Repo) StorageTempDir() (string, error) {
	tempPath, err := repo.locator.TempDir(repo.GetStorageName())
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(tempPath, mode.Directory); err != nil {
		return "", err
	}

	return tempPath, nil
}

// ObjectHash detects the object hash used by this particular repository.
func (repo *Repo) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	defer trace.StartRegion(ctx, "ObjectHash").End()
	return repo.detectObjectHash(ctx)
}

// ReferenceBackend detects the reference backend used by this repository.
func (repo *Repo) ReferenceBackend(ctx context.Context) (git.ReferenceBackend, error) {
	defer trace.StartRegion(ctx, "ReferenceBackend").End()
	return repo.detectRefBackend(ctx)
}

// IsOffloaded determines whether a repository is offloaded.
// Currently, this is indicated by the presence of exactly one value under the
// "remote.offload.url" configuration key.
//
// A repository is considered offloaded if and only if this key is defined and
// contains exactly one remote URL. If the key is missing, undefined, or has
// multiple values, the repository is considered not offloaded or invalidly offloaded.
func (repo *Repo) IsOffloaded(ctx context.Context) (bool, string, error) {
	defer trace.StartRegion(ctx, "OffloadState").End()
	offloadURL, err := repo.GetConfigValues(ctx, "remote.offload.url")
	if err != nil {
		if errors.Is(err, git.ErrNotFound) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("detect offloading: %w", err)
	}
	if len(offloadURL) == 1 && offloadURL[0] != "" {
		return true, offloadURL[0], nil
	}
	return false, "", fmt.Errorf("offload URL must be a single non-empty string")
}
