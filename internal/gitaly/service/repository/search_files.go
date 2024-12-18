package repository

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

const (
	surroundContext = "2"

	// searchFilesFilterMaxLength controls the maximum length of the regular
	// expression to thwart excessive resource usage when filtering
	searchFilesFilterMaxLength = 1000
)

var contentDelimiter = []byte("--\n")

func (s *server) SearchFilesByContent(req *gitalypb.SearchFilesByContentRequest, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	ctx := stream.Context()

	if err := validateSearchFilesRequest(ctx, s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(req.GetRepository())

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "grep",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--ignore-case"},
			gitcmd.Flag{Name: "-I"},
			gitcmd.Flag{Name: "--line-number"},
			gitcmd.Flag{Name: "--null"},
			gitcmd.ValueFlag{Name: "--before-context", Value: surroundContext},
			gitcmd.ValueFlag{Name: "--after-context", Value: surroundContext},
			gitcmd.Flag{Name: "--perl-regexp"},
			gitcmd.ValueFlag{Name: "-e", Value: req.GetQuery()},
		},
		Args: []string{
			string(req.GetRef()),
		},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd start failed: %w", err)
	}

	if err = sendSearchFilesResultChunked(cmd, stream); err != nil {
		return structerr.NewInternal("sending chunked response failed: %w", err)
	}

	return nil
}

func sendMatchInChunks(buf []byte, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SearchFilesByContentResponse{MatchData: bytes.Clone(p)})
	})

	if _, err := io.Copy(sw, bytes.NewReader(buf)); err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByContentResponse{EndOfMatch: true})
}

func sendSearchFilesResultChunked(cmd *command.Command, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	var buf []byte
	reader := bufio.NewReader(cmd)

	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("readbytes: %w", err)
		}

		if bytes.Equal(line, contentDelimiter) {
			if err := sendMatchInChunks(buf, stream); err != nil {
				return err
			}

			buf = nil
			continue
		}

		buf = append(buf, line...)
	}

	if len(buf) > 0 {
		return sendMatchInChunks(buf, stream)
	}

	return nil
}

func (s *server) SearchFilesByName(req *gitalypb.SearchFilesByNameRequest, stream gitalypb.RepositoryService_SearchFilesByNameServer) error {
	ctx := stream.Context()

	if err := validateSearchFilesRequest(ctx, s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	var filter *regexp.Regexp
	if req.GetFilter() != "" {
		if len(req.GetFilter()) > searchFilesFilterMaxLength {
			return structerr.NewInvalidArgument("filter exceeds maximum length")
		}
		var err error
		filter, err = regexp.Compile(req.GetFilter())
		if err != nil {
			return structerr.NewInvalidArgument("filter did not compile: %w", err)
		}
	}

	repo := s.localRepoFactory.Build(req.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name: "ls-tree",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--full-tree"},
			gitcmd.Flag{Name: "--name-status"},
			gitcmd.Flag{Name: "-r"},
			// We use -z to force NULL byte termination here to prevent git from
			// quoting and escaping unusual file names. Lstree parser would be a
			// more ideal solution. Unfortunately, it supports parsing full
			// output while we are interested in the filenames only.
			gitcmd.Flag{Name: "-z"},
		},
		Args: []string{
			string(req.GetRef()),
		},
		PostSepArgs: []string{
			req.GetQuery(),
		},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return structerr.NewInternal("cmd start failed: %w", err)
	}

	files, err := parseLsTree(objectHash, cmd, filter, int(req.GetOffset()), int(req.GetLimit()))
	if err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByNameResponse{Files: files})
}

type searchFilesRequest interface {
	GetRepository() *gitalypb.Repository
	GetRef() []byte
	GetQuery() string
}

func validateSearchFilesRequest(ctx context.Context, locator storage.Locator, req searchFilesRequest) error {
	if err := locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetQuery()) == 0 {
		return errors.New("no query given")
	}

	if len(req.GetRef()) == 0 {
		return errors.New("no ref given")
	}

	if bytes.HasPrefix(req.GetRef(), []byte("-")) {
		return errors.New("invalid ref argument")
	}

	return nil
}

func parseLsTree(objectHash git.ObjectHash, cmd *command.Command, filter *regexp.Regexp, offset int, limit int) ([][]byte, error) {
	var files [][]byte
	var index int
	parser := localrepo.NewParser(cmd, objectHash)

	for {
		path, err := parser.NextEntryPath()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if filter != nil && !filter.Match(path) {
			continue
		}

		index++
		if index > offset {
			files = append(files, path)
		}
		if limit > 0 && len(files) >= limit {
			break
		}
	}

	return files, nil
}
