package repository

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	migrationid "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration/id"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrInvalidSourceRepository is returned when attempting to replicate from an invalid source repository.
var ErrInvalidSourceRepository = status.Error(codes.NotFound, "invalid source repository")

func (s *server) getReferenceBackend(
	ctx context.Context,
	repoClient gitalypb.RepositoryServiceClient,
	source *gitalypb.Repository,
) (*git.ReferenceBackend, error) {
	resp, err := repoClient.RepositoryInfo(ctx, &gitalypb.RepositoryInfoRequest{Repository: source})
	if err != nil {
		return nil, fmt.Errorf("repository info: %w", err)
	}

	switch resp.GetReferences().GetReferenceBackend() {
	case gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_FILES:
		return &git.ReferenceBackendFiles, nil
	case gitalypb.RepositoryInfoResponse_ReferencesInfo_REFERENCE_BACKEND_REFTABLE:
		return &git.ReferenceBackendReftables, nil
	default:
		return nil, fmt.Errorf("unknown reference backend")
	}
}

// ReplicateRepository replicates data from a source repository to target repository. On the target
// repository, this operation ensures synchronization of the following components:
//
// - Git config
// - Git attributes
// - Custom Git hooks,
// - References and objects
func (s *server) ReplicateRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	if err := validateReplicateRepository(ctx, s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repoClient, err := s.newRepoClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return nil, structerr.NewInternal("new client: %w", err)
	}

	// We're checking for repository existence up front such that we can give a conclusive error
	// in case it doesn't. Otherwise, the error message returned to the client would depend on
	// the order in which the sync functions were executed. Most importantly, given that
	// `syncRepository` uses FetchInternalRemote which in turn uses gitaly-ssh, this code path
	// cannot pass up NotFound errors given that there is no communication channel between
	// Gitaly and gitaly-ssh.
	request, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: in.GetSource(),
	})
	if err != nil {
		return nil, structerr.NewInternal("checking for repo existence: %w", err)
	}
	if !request.GetExists() {
		return nil, ErrInvalidSourceRepository
	}

	// When creating a replica, we extract the tar of the source repository into
	// the target repository. So we need to ensure that they use the same reference
	// backend.
	sourceBackend, err := s.getReferenceBackend(ctx, repoClient, in.GetSource())
	if err != nil {
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			return nil, ErrInvalidSourceRepository
		}

		return nil, structerr.NewInternal("source reference backend: %w", err)
	}

	// While the target repository is created using the same reference backend as
	// the source. For newly created repositories, we want to migrate them to
	// reftables, if we've enabled the flag to use reftables for new repositories.
	repoCreated := false

	if err := s.locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		repoPath, err := s.locator.GetRepoPath(ctx, in.GetRepository(), storage.WithRepositoryVerificationSkipped())
		if err != nil {
			return nil, structerr.NewInternal("%w", err)
		}

		repoCreated = true

		if err = s.create(ctx, in, sourceBackend, repoPath); err != nil {
			if errors.Is(err, ErrInvalidSourceRepository) {
				return nil, ErrInvalidSourceRepository
			}

			return nil, structerr.NewInternal("%w", err)
		}
	}

	// The partitioning hint should not be forwarded to other Gitaly nodes as the path is irrelevant for them.
	outgoingCtx := storage.ContextWithoutPartitioningHint(ctx)
	outgoingCtx = metadata.IncomingToOutgoing(outgoingCtx)

	if err := s.replicateRepository(outgoingCtx, in.GetSource(), in.GetRepository()); err != nil {
		return nil, structerr.NewInternal("replicating repository: %w", err)
	}

	// ReplicateRepository sets the backend of the newly created repository to be the same as that of the source
	// repository. Although it is a new repository, it doesn't follow the featureflag for using reftables for new
	// repositories. So let's migrate the repository.
	if tx := storage.ExtractTransaction(ctx); tx != nil && repoCreated {
		shouldBeReftables := featureflag.NewRepoReftableBackend.IsEnabled(ctx)
		if shouldBeReftables && sourceBackend.Name != git.ReferenceBackendReftables.Name {
			migrator := migration.NewReferenceBackendMigration(migrationid.Reftable,
				git.ReferenceBackendReftables, s.localRepoFactory, nil)

			if err := migrator.Fn(ctx,
				tx,
				in.GetRepository().GetStorageName(),
				in.GetRepository().GetRelativePath(),
			); err != nil {
				return nil, structerr.NewInternal("migration failed: %w", err)
			}
		}
	}

	return &gitalypb.ReplicateRepositoryResponse{}, nil
}

func (s *server) replicateRepository(ctx context.Context, source, target *gitalypb.Repository) error {
	if err := s.syncGitconfig(ctx, source, target, func(ctx context.Context, path string, content io.Reader) error {
		if err := s.writeFile(ctx, path, content); err != nil {
			return err
		}

		if tx := storage.ExtractTransaction(ctx); tx != nil {
			originalConfigRelativePath, err := filepath.Rel(tx.FS().Root(), path)
			if err != nil {
				return fmt.Errorf("original config relative path: %w", err)
			}

			if err := tx.FS().RecordRemoval(originalConfigRelativePath); err != nil {
				return fmt.Errorf("record old config removal: %w", err)
			}

			if err := tx.FS().RecordFile(originalConfigRelativePath); err != nil {
				return fmt.Errorf("record new config creation: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("synchronizing gitconfig: %w", err)
	}

	if err := s.syncReferences(ctx, source, target); err != nil {
		return fmt.Errorf("synchronizing references: %w", err)
	}

	if err := s.syncCustomHooks(ctx, source, target); err != nil {
		return fmt.Errorf("synchronizing custom hooks: %w", err)
	}

	return nil
}

func validateReplicateRepository(ctx context.Context, locator storage.Locator, in *gitalypb.ReplicateRepositoryRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository(), storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return err
	}

	if in.GetSource() == nil {
		return errors.New("source repository cannot be empty")
	}

	if in.GetRepository().GetStorageName() == in.GetSource().GetStorageName() {
		return errors.New("repository and source have the same storage")
	}

	return nil
}

func (s *server) create(
	ctx context.Context,
	in *gitalypb.ReplicateRepositoryRequest,
	sourceBackend *git.ReferenceBackend,
	repoPath string,
) error {
	// if the directory exists, remove it
	if _, err := os.Stat(repoPath); err == nil {
		tempDir, err := tempdir.NewWithoutContext(in.GetRepository().GetStorageName(), s.logger, s.locator)
		if err != nil {
			return err
		}

		if err = os.Rename(repoPath, filepath.Join(tempDir.Path(), filepath.Base(repoPath))); err != nil {
			return fmt.Errorf("error deleting invalid repo: %w", err)
		}

		s.logger.WithField("repo_path", repoPath).WarnContext(ctx, "removed invalid repository")
	}

	if err := s.createFromSnapshot(ctx, sourceBackend, in.GetSource(), in.GetRepository()); err != nil {
		return fmt.Errorf("could not create repository from snapshot: %w", err)
	}

	return nil
}

func (s *server) createFromSnapshot(
	ctx context.Context,
	sourceBackend *git.ReferenceBackend,
	source, target *gitalypb.Repository,
) error {
	if err := repoutil.Create(ctx, s.logger, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.repositoryCounter, target, func(repo *gitalypb.Repository) error {
		if err := s.extractSnapshot(ctx, source, repo); err != nil {
			return fmt.Errorf("extracting snapshot: %w", err)
		}

		// The archive extracted above does not contain the configuration file. If SHA256 is used, this
		// would lead to an invalid repository as the object format configuration is not present. This
		// leads to failures with transactions as we need to pack the object directory after the repository
		// is created. Sync the config here to ensure the correct object format is configured before
		// returning. We write it directly to disk without voting as the voting is handled by the voting
		// round of the repository creation. We also don't want to record the config creating operation
		// separately as it is already recorded by `repoutil.Create` when it records the entire repository.
		//
		// We only run this with transactions as the file update is not atomic without transactions.
		if tx := storage.ExtractTransaction(ctx); tx != nil {
			if err := s.syncGitconfig(ctx, source, repo, func(ctx context.Context, path string, content io.Reader) (returnedErr error) {
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.File)
				if err != nil {
					return fmt.Errorf("open: %w", err)
				}

				defer func() {
					if err := file.Close(); err != nil {
						returnedErr = errors.Join(returnedErr, fmt.Errorf("close: %w", err))
					}
				}()

				if _, err := io.Copy(file, content); err != nil {
					return fmt.Errorf("copy: %w", err)
				}

				return nil
			}); err != nil {
				return fmt.Errorf("sync gitconfig: %w", err)
			}
		}

		return nil
	}, repoutil.WithReferenceBackend(*sourceBackend)); err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		if err := s.migrationStateManager.RecordKeyCreation(
			tx,
			target.GetRelativePath(),
		); err != nil {
			return fmt.Errorf("recording migration key: %w", err)
		}
	}

	return nil
}

func (s *server) extractSnapshot(ctx context.Context, source, target *gitalypb.Repository) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}

	stream, err := repoClient.GetSnapshot(ctx, &gitalypb.GetSnapshotRequest{Repository: source})
	if err != nil {
		return fmt.Errorf("get snapshot: %w", err)
	}

	// We need to catch a possible 'invalid repository' error from GetSnapshot. On an empty read,
	// we read the first message from the stream here to get access to the possible 'invalid repository' error.
	firstBytes, err := stream.Recv()
	if err != nil {
		switch {
		case structerr.GRPCCode(err) == codes.NotFound && strings.Contains(err.Error(), "GetRepoPath: not a git repository:"):
			// The error condition exists for backwards compatibility purposes, only,
			// and can be removed in the next release.
			return ErrInvalidSourceRepository
		case structerr.GRPCCode(err) == codes.NotFound && strings.Contains(err.Error(), storage.ErrRepositoryNotFound.Error()):
			return ErrInvalidSourceRepository
		case structerr.GRPCCode(err) == codes.FailedPrecondition && strings.Contains(err.Error(), storage.ErrRepositoryNotValid.Error()):
			return ErrInvalidSourceRepository
		default:
			return fmt.Errorf("first snapshot read: %w", err)
		}
	}

	snapshotReader := io.MultiReader(
		bytes.NewReader(firstBytes.GetData()),
		streamio.NewReader(func() ([]byte, error) {
			resp, err := stream.Recv()
			return resp.GetData(), err
		}),
	)

	targetPath, err := s.locator.GetRepoPath(ctx, target, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return fmt.Errorf("target path: %w", err)
	}

	// Extract tar using Go's tar package
	if err := s.extractTarToDirectory(ctx, snapshotReader, targetPath); err != nil {
		return fmt.Errorf("extract tar: %w", err)
	}

	return nil
}

// extractTarToDirectory extracts a tar archive to the specified directory using Go's tar package
func (s *server) extractTarToDirectory(ctx context.Context, reader io.Reader, targetDir string) error {
	tarReader := tar.NewReader(reader)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		targetPath := filepath.Join(targetDir, header.Name)

		if !strings.HasPrefix(targetPath, filepath.Clean(targetDir)+string(os.PathSeparator)) &&
			targetPath != filepath.Clean(targetDir) {
			return fmt.Errorf("invalid file path in tar: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("creating directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			if err := s.extractFile(ctx, tarReader, targetPath, header); err != nil {
				return fmt.Errorf("extracting file %s: %w", targetPath, err)
			}

		case tar.TypeSymlink:
			if filepath.IsAbs(header.Linkname) {
				return fmt.Errorf("absolute symlink not allowed: %s -> %s", header.Name, header.Linkname)
			}

			// Remove existing file/symlink if it exists
			if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing existing file for symlink %s: %w", targetPath, err)
			}

			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return fmt.Errorf("creating symlink %s -> %s: %w", targetPath, header.Linkname, err)
			}

		case tar.TypeLink:
			linkTarget := filepath.Join(targetDir, header.Linkname)

			if !strings.HasPrefix(linkTarget, filepath.Clean(targetDir)+string(os.PathSeparator)) &&
				linkTarget != filepath.Clean(targetDir) {
				return fmt.Errorf("invalid hard link target: %s", header.Linkname)
			}

			// Remove existing file if it exists
			if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing existing file for hard link %s: %w", targetPath, err)
			}

			if err := os.Link(linkTarget, targetPath); err != nil {
				return fmt.Errorf("creating hard link %s -> %s: %w", targetPath, linkTarget, err)
			}

		default:
			// Skip unsupported file types (devices, FIFOs, etc.)
			s.logger.WithField("file", header.Name).WithField("type", header.Typeflag).
				WarnContext(ctx, "skipping unsupported file type in tar archive")
		}

		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeDir {
			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("setting permissions for %s: %w", targetPath, err)
			}
		}
	}

	return nil
}

// extractFile extracts a regular file from the tar archive
func (s *server) extractFile(ctx context.Context, tarReader *tar.Reader, targetPath string, header *tar.Header) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), mode.Directory); err != nil {
		return fmt.Errorf("creating parent directory: %w", err)
	}

	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			s.logger.WithField("file", targetPath).WithError(closeErr).
				WarnContext(ctx, "failed to close file during tar extraction")
		}
	}()

	// Copy file content with context cancellation support
	const bufferSize = 32 * 1024 // 32KB buffer
	buffer := make([]byte, bufferSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := tarReader.Read(buffer)
		if n > 0 {
			if _, writeErr := file.Write(buffer[:n]); writeErr != nil {
				return fmt.Errorf("writing to file: %w", writeErr)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading from tar: %w", err)
		}
	}

	return nil
}

func (s *server) syncReferences(ctx context.Context, source, target *gitalypb.Repository) error {
	repo := s.localRepoFactory.Build(target)

	if err := fetchInternalRemote(ctx, s.txManager, s.conns, repo, source); err != nil {
		return fmt.Errorf("fetch internal remote: %w", err)
	}

	return nil
}

func fetchInternalRemote(
	ctx context.Context,
	txManager transaction.Manager,
	conns *client.Pool,
	repo *localrepo.Repo,
	remoteRepoProto *gitalypb.Repository,
) error {
	var stderr bytes.Buffer
	if err := repo.FetchInternal(
		ctx,
		remoteRepoProto,
		[]string{git.MirrorRefSpec},
		localrepo.FetchOpts{
			Prune:  true,
			Stderr: &stderr,
			// By default, Git will fetch any tags that point into the fetched references. This check
			// requires time, and is ultimately a waste of compute because we already mirror all refs
			// anyway, including tags. By adding `--no-tags` we can thus ask Git to skip that and thus
			// accelerate the fetch.
			Tags: localrepo.FetchOptsTagsNone,
			CommandOptions: []gitcmd.CmdOpt{
				gitcmd.WithConfig(gitcmd.ConfigPair{Key: "fetch.negotiationAlgorithm", Value: "skipping"}),
				// Disable the consistency checks of objects fetched into the replicated repository.
				// These fetched objects come from preexisting internal sources, thus it would be
				// problematic for the fetch to fail consistency checks due to altered requirements.
				gitcmd.WithConfig(gitcmd.ConfigPair{Key: "fetch.fsckObjects", Value: "false"}),
			},
		},
	); err != nil {
		if errors.As(err, &localrepo.FetchFailedError{}) {
			return structerr.New("%w", err).WithMetadata("stderr", stderr.String())
		}

		return fmt.Errorf("fetch: %w", err)
	}

	remoteRepo, err := remoterepo.New(ctx, remoteRepoProto, conns)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	remoteDefaultBranch, err := remoteRepo.HeadReference(ctx)
	if err != nil {
		return structerr.NewInternal("getting remote default branch: %w", err)
	}

	defaultBranch, err := repo.HeadReference(ctx)
	if err != nil {
		return structerr.NewInternal("getting local default branch: %w", err)
	}

	if defaultBranch != remoteDefaultBranch {
		if err := repo.SetDefaultBranch(ctx, txManager, remoteDefaultBranch); err != nil {
			return structerr.NewInternal("setting default branch: %w", err)
		}
	}

	return nil
}

// syncCustomHooks replicates custom hooks from a source to a target.
func (s *server) syncCustomHooks(ctx context.Context, source, target *gitalypb.Repository) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return fmt.Errorf("creating repo client: %w", err)
	}

	stream, err := repoClient.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{
		Repository: source,
	})
	if err != nil {
		return fmt.Errorf("getting custom hooks: %w", err)
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetData(), err
	})

	if err := repoutil.SetCustomHooks(ctx, s.logger, s.locator, s.txManager, reader, target); err != nil {
		return fmt.Errorf("setting custom hooks: %w", err)
	}

	return nil
}

func (s *server) syncGitconfig(ctx context.Context, source, target *gitalypb.Repository, writeConfig func(ctx context.Context, path string, content io.Reader) error) error {
	repoClient, err := s.newRepoClient(ctx, source.GetStorageName())
	if err != nil {
		return err
	}

	repoPath, err := s.locator.GetRepoPath(ctx, target)
	if err != nil {
		return err
	}

	stream, err := repoClient.GetConfig(ctx, &gitalypb.GetConfigRequest{
		Repository: source,
	})
	if err != nil {
		return err
	}

	configPath := filepath.Join(repoPath, "config")
	return writeConfig(ctx, configPath, streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	}))
}

func (s *server) writeFile(ctx context.Context, path string, reader io.Reader) (returnedErr error) {
	parentDir := filepath.Dir(path)
	if err := os.MkdirAll(parentDir, mode.Directory); err != nil {
		return err
	}

	lockedFile, err := safe.NewLockingFileWriter(path, safe.LockingFileWriterConfig{
		FileWriterConfig: safe.FileWriterConfig{
			FileMode: mode.File,
		},
	})
	if err != nil {
		return fmt.Errorf("creating file writer: %w", err)
	}
	defer func() {
		if err := lockedFile.Close(); err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()

	if _, err := io.Copy(lockedFile, reader); err != nil {
		return err
	}

	if err := transaction.CommitLockedFile(ctx, s.txManager, lockedFile); err != nil {
		return err
	}

	return nil
}

// newRepoClient creates a new RepositoryClient that talks to the gitaly of the source repository
func (s *server) newRepoClient(ctx context.Context, storageName string) (gitalypb.RepositoryServiceClient, error) {
	gitalyServerInfo, err := storage.ExtractGitalyServer(ctx, storageName)
	if err != nil {
		return nil, err
	}

	conn, err := s.conns.Dial(ctx, gitalyServerInfo.Address, gitalyServerInfo.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}
