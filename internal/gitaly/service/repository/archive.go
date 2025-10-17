package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/smudge"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
	"google.golang.org/protobuf/proto"
)

type archiveParams struct {
	writer       io.Writer
	in           *gitalypb.GetArchiveRequest
	compressArgs []string
	format       string
	archivePath  string
	exclude      []string
}

func (s *server) GetArchive(in *gitalypb.GetArchiveRequest, stream gitalypb.RepositoryService_GetArchiveServer) error {
	ctx := stream.Context()
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	compressArgs, format := parseArchiveFormat(in.GetFormat())
	repo := s.localRepoFactory.Build(repository)

	repoRoot, err := repo.Path(ctx)
	if err != nil {
		return err
	}

	path, err := storage.ValidateRelativePath(repoRoot, string(in.GetPath()))
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	exclude := make([]string, len(in.GetExclude()))
	for i, ex := range in.GetExclude() {
		exclude[i], err = storage.ValidateRelativePath(repoRoot, string(ex))
		if err != nil {
			return structerr.NewInvalidArgument("%w", err)
		}
	}

	if err := validateGetArchiveRequest(in, format); err != nil {
		return err
	}

	if err := s.validateGetArchivePrecondition(ctx, repo, in.GetCommitId(), path, exclude); err != nil {
		return err
	}

	if in.GetElidePath() {
		// `git archive <commit ID>:<path>` expects exclusions to be relative to path
		pathSlash := path + string(os.PathSeparator)
		for i := range exclude {
			if !strings.HasPrefix(exclude[i], pathSlash) {
				return structerr.NewInvalidArgument("invalid exclude: %q is not a subdirectory of %q", exclude[i], path)
			}

			exclude[i] = exclude[i][len(pathSlash):]
		}
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetArchiveResponse{Data: p})
	})

	s.logger.WithField("request_hash", requestHash(in)).InfoContext(ctx, "request details")

	return s.handleArchive(ctx, archiveParams{
		writer:       writer,
		in:           in,
		compressArgs: compressArgs,
		format:       format,
		archivePath:  path,
		exclude:      exclude,
	})
}

func parseArchiveFormat(format gitalypb.GetArchiveRequest_Format) ([]string, string) {
	switch format {
	case gitalypb.GetArchiveRequest_TAR:
		return nil, "tar"
	case gitalypb.GetArchiveRequest_TAR_GZ:
		return []string{"gzip", "-c", "-n"}, "tar"
	case gitalypb.GetArchiveRequest_TAR_BZ2:
		return []string{"bzip2", "-c"}, "tar"
	case gitalypb.GetArchiveRequest_ZIP:
		return nil, "zip"
	}

	return nil, ""
}

func validateGetArchiveRequest(in *gitalypb.GetArchiveRequest, format string) error {
	if err := git.ValidateRevision([]byte(in.GetCommitId())); err != nil {
		return structerr.NewInvalidArgument("invalid commitId: %w", err)
	}

	if len(format) == 0 {
		return structerr.NewInvalidArgument("invalid format")
	}

	return nil
}

func (s *server) validateGetArchivePrecondition(
	ctx context.Context,
	repo gitcmd.RepositoryExecutor,
	commitID string,
	path string,
	exclude []string,
) error {
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return err
	}
	defer cancel()

	f := catfile.NewTreeEntryFinder(objectReader)
	if path != "." {
		if ok, err := findGetArchivePath(ctx, f, commitID, path); err != nil {
			return err
		} else if !ok {
			return structerr.NewFailedPrecondition("path doesn't exist")
		}
	} else {
		objectInfoReader, cancel, err := s.catfileCache.ObjectInfoReader(ctx, repo)
		if err != nil {
			return err
		}
		defer cancel()

		repoHash, err := repo.ObjectHash(ctx)
		if err != nil {
			return err
		}

		rootTree, err := objectInfoReader.Info(ctx, git.ObjectID(commitID).Revision()+"^{tree}")
		if err != nil {
			return err
		}

		// Root tree is empty, nothing to return.
		if rootTree.ObjectID() == repoHash.EmptyTreeOID {
			return structerr.NewFailedPrecondition("path doesn't exist")
		}
	}

	for i, exclude := range exclude {
		if ok, err := findGetArchivePath(ctx, f, commitID, exclude); err != nil {
			return err
		} else if !ok {
			return structerr.NewFailedPrecondition("exclude[%d] doesn't exist", i)
		}
	}

	return nil
}

func findGetArchivePath(ctx context.Context, f *catfile.TreeEntryFinder, commitID, path string) (ok bool, err error) {
	treeEntry, err := f.FindByRevisionAndPath(ctx, commitID, path)
	if err != nil {
		return false, err
	}

	if treeEntry == nil || len(treeEntry.GetOid()) == 0 {
		return false, nil
	}
	return true, nil
}

func (s *server) handleArchive(ctx context.Context, p archiveParams) error {
	var args []string
	pathspecs := make([]string, 0, len(p.exclude)+1)
	if !p.in.GetElidePath() {
		// git archive [options] <commit ID> -- <path> [exclude*]
		args = []string{p.in.GetCommitId()}
		pathspecs = append(pathspecs, p.archivePath)
	} else if p.archivePath != "." {
		// git archive [options] <commit ID>:<path> -- [exclude*]
		args = []string{p.in.GetCommitId() + ":" + p.archivePath}
	} else {
		// git archive [options] <commit ID> -- [exclude*]
		args = []string{p.in.GetCommitId()}
	}

	for _, exclude := range p.exclude {
		pathspecs = append(pathspecs, ":(exclude)"+exclude)
	}

	var env []string
	var gitConfig []gitcmd.ConfigPair

	if p.in.GetIncludeLfsBlobs() {
		smudgeCfg := smudge.Config{
			GlRepository: p.in.GetRepository().GetGlRepository(),
			Gitlab:       s.cfg.Gitlab,
			TLS:          s.cfg.TLS,
			DriverType:   smudge.DriverTypeProcess,
		}

		smudgeEnv, err := smudgeCfg.Environment()
		if err != nil {
			return fmt.Errorf("setting up smudge environment: %w", err)
		}

		smudgeGitConfig, err := smudgeCfg.GitConfiguration(s.cfg)
		if err != nil {
			return fmt.Errorf("setting up smudge gitconfig: %w", err)
		}

		env = append(
			env,
			smudgeEnv,
		)
		gitConfig = append(gitConfig, smudgeGitConfig)
	}

	repo := s.localRepoFactory.Build(p.in.GetRepository())

	cacheKey := createArchiveCacheKey(repo.GetGlProjectPath(), args, pathspecs)
	_, _, err := s.archiveCache.Fetch(ctx, cacheKey, p.writer, func(writer io.Writer) error {
		archiveCommand, err := repo.Exec(ctx, gitcmd.Command{
			Name:        "archive",
			Flags:       []gitcmd.Option{gitcmd.ValueFlag{Name: "--format", Value: p.format}, gitcmd.ValueFlag{Name: "--prefix", Value: p.in.GetPrefix() + "/"}},
			Args:        args,
			PostSepArgs: pathspecs,
		}, gitcmd.WithEnv(env...), gitcmd.WithConfig(gitConfig...), gitcmd.WithSetupStdout())
		if err != nil {
			return err
		}

		if len(p.compressArgs) > 0 {
			command, err := command.New(ctx, s.logger, p.compressArgs,
				command.WithStdin(archiveCommand), command.WithStdout(writer),
			)
			if err != nil {
				return err
			}

			if err := command.Wait(); err != nil {
				return err
			}
		} else if _, err = io.Copy(writer, archiveCommand); err != nil {
			return err
		}

		return archiveCommand.Wait()
	})

	return err
}

func requestHash(req proto.Message) string {
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return "failed to hash request"
	}

	hash := sha256.Sum256(reqBytes)
	return hex.EncodeToString(hash[:])
}

// createArchiveCacheKey creates a cache key using the GitLab project's path, the `git archive`
// command arguments and the pathspecs. The goal is to create a key that is unique not only
// across repository, but also across the content of each archive within the same repository.
func createArchiveCacheKey(gitLabProjectPath string, args []string, pathspecs []string) string {
	cacheKeyHash := sha256.New()
	cacheKeyHash.Write([]byte(gitLabProjectPath))
	cacheKeyHash.Write([]byte(strings.Join(args, ",")))
	cacheKeyHash.Write([]byte(strings.Join(pathspecs, ",")))
	return hex.EncodeToString(cacheKeyHash.Sum(nil))
}
