package remote

import (
	"bytes"
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func (s *server) FindRemoteRepository(ctx context.Context, req *gitalypb.FindRemoteRepositoryRequest) (*gitalypb.FindRemoteRepositoryResponse, error) {
	if req.GetRemote() == "" {
		return nil, structerr.NewInvalidArgument("empty remote can't be checked.")
	}

	var output bytes.Buffer
	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		gitcmd.Command{
			Name: "ls-remote",
			Args: []string{
				req.GetRemote(),
				"HEAD",
			},
		},
		gitcmd.WithStdout(&output),
	)
	if err != nil {
		return nil, structerr.NewInternal("error executing git command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// The output of git-ls-remote is expected to be of the format:
	//
	//	58fbff2e0d3b620f591a748c158799ead87b51cd	HEAD\n
	objectID, refname, ok := bytes.Cut(output.Bytes(), []byte("\t"))
	if !ok {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// We've asked for HEAD, so the refname should match that.
	if !bytes.Equal(refname, []byte("HEAD\n")) {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// We have no way to ask the remote's object format via git-ls-remote(1), so all we can do
	// is to verify that the object hash matches something we know.
	switch len(objectID) {
	case git.ObjectHashSHA1.EncodedLen():
		return &gitalypb.FindRemoteRepositoryResponse{Exists: true}, nil
	case git.ObjectHashSHA256.EncodedLen():
		return &gitalypb.FindRemoteRepositoryResponse{Exists: true}, nil
	default:
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}
}
