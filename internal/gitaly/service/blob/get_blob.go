package blob

import (
	"context"
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) GetBlob(in *gitalypb.GetBlobRequest, stream gitalypb.BlobService_GetBlobServer) error {
	ctx := stream.Context()

	if err := validateRequest(ctx, s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(stream.Context(), repo)
	if err != nil {
		return structerr.NewInternal("create object reader: %w", err)
	}
	defer cancel()

	blob, err := objectReader.Object(ctx, git.Revision(in.GetOid()))
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			if err := stream.Send(&gitalypb.GetBlobResponse{}); err != nil {
				return structerr.NewInternal("sending empty response: %w", err)
			}
			return nil
		}
		return structerr.NewInternal("read object: %w", err)
	}

	if blob.Type != "blob" {
		if err := stream.Send(&gitalypb.GetBlobResponse{}); err != nil {
			return structerr.NewInternal("sending empty response: %w", err)
		}

		return nil
	}

	readLimit := blob.Size
	if in.GetLimit() >= 0 && in.GetLimit() < readLimit {
		readLimit = in.GetLimit()
	}
	firstMessage := &gitalypb.GetBlobResponse{
		Size: blob.Size,
		Oid:  blob.Oid.String(),
	}

	if readLimit == 0 {
		if err := stream.Send(firstMessage); err != nil {
			return structerr.NewInternal("sending empty blob: %w", err)
		}

		return nil
	}

	sw := streamio.NewWriter(func(p []byte) error {
		msg := &gitalypb.GetBlobResponse{}
		if firstMessage != nil {
			msg = firstMessage
			firstMessage = nil
		}
		msg.Data = p
		return stream.Send(msg)
	})

	_, err = io.CopyN(sw, blob, readLimit)
	if err != nil {
		return structerr.NewInternal("send: %w", err)
	}

	return nil
}

func validateRequest(ctx context.Context, locator storage.Locator, in *gitalypb.GetBlobRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}

	if len(in.GetOid()) == 0 {
		return errors.New("empty Oid")
	}
	return nil
}
