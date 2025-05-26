package commit

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func sendTreeEntry(
	stream gitalypb.CommitService_TreeEntryServer,
	objectReader catfile.ObjectContentReader,
	objectInfoReader catfile.ObjectInfoReader,
	revision, path string,
	limit, maxSize int64,
) error {
	ctx := stream.Context()

	treeEntry, err := catfile.NewTreeEntryFinder(objectReader).FindByRevisionAndPath(ctx, revision, path)
	if err != nil {
		return err
	}

	if treeEntry == nil || len(treeEntry.GetOid()) == 0 {
		return structerr.NewNotFound("tree entry not found").WithMetadata("path", path)
	}

	if treeEntry.GetType() == gitalypb.TreeEntry_COMMIT {
		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_COMMIT,
			Mode: treeEntry.GetMode(),
			Oid:  treeEntry.GetOid(),
		}
		if err := stream.Send(response); err != nil {
			return structerr.NewInternal("send: %w", err)
		}

		return nil
	}

	if treeEntry.GetType() == gitalypb.TreeEntry_TREE {
		treeInfo, err := objectInfoReader.Info(ctx, git.Revision(treeEntry.GetOid()))
		if err != nil {
			return err
		}

		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_TREE,
			Oid:  treeEntry.GetOid(),
			Size: treeInfo.Size,
			Mode: treeEntry.GetMode(),
		}

		if err := stream.Send(response); err != nil {
			return structerr.NewInternal("sending response: %w", err)
		}

		return nil
	}

	objectInfo, err := objectInfoReader.Info(ctx, git.Revision(treeEntry.GetOid()))
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	if strings.ToLower(treeEntry.GetType().String()) != objectInfo.Type {
		return structerr.NewInternal(
			"mismatched object type: tree-oid=%s object-oid=%s entry-type=%s object-type=%s",
			treeEntry.GetOid(), objectInfo.Oid, treeEntry.GetType().String(), objectInfo.Type,
		)
	}

	dataLength := objectInfo.Size

	if maxSize > 0 && dataLength > maxSize {
		return structerr.NewFailedPrecondition(
			"object size (%d) is bigger than the maximum allowed size (%d)",
			dataLength, maxSize,
		)
	}

	if limit > 0 && dataLength > limit {
		dataLength = limit
	}

	response := &gitalypb.TreeEntryResponse{
		Type: gitalypb.TreeEntryResponse_BLOB,
		Oid:  objectInfo.Oid.String(),
		Size: objectInfo.Size,
		Mode: treeEntry.GetMode(),
	}
	if dataLength == 0 {
		if err := stream.Send(response); err != nil {
			return structerr.NewInternal("sending response: %w", err)
		}

		return nil
	}

	blobObj, err := objectReader.Object(ctx, git.Revision(objectInfo.Oid))
	if err != nil {
		return err
	}
	if blobObj.Type != "blob" {
		return fmt.Errorf("blob has unexpected type %q", blobObj.Type)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		response.Data = p

		if err := stream.Send(response); err != nil {
			return structerr.NewInternal("send: %w", err)
		}

		// Use a new response so we don't send other fields (Size, ...) over and over
		response = &gitalypb.TreeEntryResponse{}

		return nil
	})

	_, err = io.CopyN(sw, blobObj, dataLength)
	return err
}

func (s *server) TreeEntry(in *gitalypb.TreeEntryRequest, stream gitalypb.CommitService_TreeEntryServer) error {
	if err := validateRequest(stream.Context(), s.locator, in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(in.GetRepository())

	requestPath := string(in.GetPath())
	// filepath.Dir("api/docs") => "api" Correct!
	// filepath.Dir("api/docs/") => "api/docs" WRONG!
	if len(requestPath) > 1 {
		requestPath = strings.TrimRight(requestPath, "/")
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(stream.Context(), repo)
	if err != nil {
		return err
	}
	defer cancel()

	objectInfoReader, cancel, err := s.catfileCache.ObjectInfoReader(stream.Context(), repo)
	if err != nil {
		return err
	}
	defer cancel()

	return sendTreeEntry(stream, objectReader, objectInfoReader, string(in.GetRevision()), requestPath, in.GetLimit(), in.GetMaxSize())
}

func validateRequest(ctx context.Context, locator storage.Locator, in *gitalypb.TreeEntryRequest) error {
	if err := locator.ValidateRepository(ctx, in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	if in.GetLimit() < 0 {
		return fmt.Errorf("negative Limit")
	}
	if in.GetMaxSize() < 0 {
		return fmt.Errorf("negative MaxSize")
	}

	return nil
}
