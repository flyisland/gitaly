package diff

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var statusLookup = map[gitalypb.ChangedPaths_Status]byte{
	gitalypb.ChangedPaths_MODIFIED:    'M',
	gitalypb.ChangedPaths_DELETED:     'D',
	gitalypb.ChangedPaths_TYPE_CHANGE: 'T',
	gitalypb.ChangedPaths_COPIED:      'C',
	gitalypb.ChangedPaths_ADDED:       'A',
	gitalypb.ChangedPaths_RENAMED:     'R',
}

func (s *server) DiffBlobs(request *gitalypb.DiffBlobsRequest, stream gitalypb.DiffService_DiffBlobsServer) error {
	ctx := stream.Context()

	if err := s.locator.ValidateRepository(ctx, request.GetRepository()); err != nil {
		return err
	}

	if len(request.GetBlobPairs()) == 0 && len(request.GetRawInfo()) == 0 {
		return structerr.NewInvalidArgument("request contains no file pairs to diff")
	}

	if len(request.GetBlobPairs()) > 0 && len(request.GetRawInfo()) > 0 {
		return structerr.NewInvalidArgument("blob pairs and raw info both used in request")
	}

	if err := validateRawInfo(request.GetRawInfo()); err != nil {
		return err
	}

	// See https://gitlab.com/gitlab-org/gitaly/-/issues/6885
	if request.GetWhitespaceChanges() != gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_UNSPECIFIED && len(request.GetBlobPairs()) > 0 {
		return structerr.NewInvalidArgument("whitespace changes cannot be ignored when blob pairs are provided")
	}

	var cmdOpts []gitcmd.Option

	switch request.GetWhitespaceChanges() {
	case gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE_ALL:
		cmdOpts = append(cmdOpts, gitcmd.Flag{Name: "--ignore-all-space"})
	case gitalypb.DiffBlobsRequest_WHITESPACE_CHANGES_IGNORE:
		cmdOpts = append(cmdOpts, gitcmd.Flag{Name: "--ignore-space-change"})
	}

	if request.GetDiffMode() == gitalypb.DiffBlobsRequest_DIFF_MODE_WORD {
		cmdOpts = append(cmdOpts, gitcmd.Flag{Name: "--word-diff=porcelain"})
	}

	if len(request.GetBlobPairs()) > 0 {
		return s.diffBlobs(ctx, request, stream, cmdOpts)
	}

	return s.diffPairs(ctx, request, stream, cmdOpts)
}

func (s *server) diffPairs(ctx context.Context,
	request *gitalypb.DiffBlobsRequest,
	stream gitalypb.DiffService_DiffBlobsServer,
	opts []gitcmd.Option,
) error {
	repo := s.localRepoFactory.Build(request.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return structerr.NewInternal("detecting object format: %w", err)
	}

	var rawInfo []diff.Raw
	var rawInput bytes.Buffer
	for _, entry := range request.GetRawInfo() {
		raw := diff.Raw{
			SrcMode: entry.GetOldMode(),
			DstMode: entry.GetNewMode(),
			SrcOID:  entry.GetOldBlobId(),
			DstOID:  entry.GetNewBlobId(),
			Status:  statusLookup[entry.GetStatus()],
			SrcPath: entry.GetPath(),
		}

		if len(entry.GetOldPath()) > 0 {
			raw.SrcPath = entry.GetOldPath()
			raw.DstPath = entry.GetPath()
		}

		rawInput.Write(raw.ToBytes())
		rawInfo = append(rawInfo, raw)
	}

	gitCmd := gitcmd.Command{
		Name: "diff-pairs",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "-z"},
			gitcmd.Flag{Name: fmt.Sprintf("--abbrev=%d", objectHash.EncodedLen())},
		},
	}
	gitCmd.Flags = append(gitCmd.Flags, opts...)

	cmd, err := repo.Exec(ctx, gitCmd, gitcmd.WithSetupStdout(), gitcmd.WithStdin(&rawInput))
	if err != nil {
		return fmt.Errorf("spawning git-diff-pairs: %w", err)
	}

	parser := diff.NewPatchParser(cmd, rawInfo, request.GetPatchBytesLimit(), objectHash)
	for parser.Parse() {
		if err := s.sendDiff(stream, parser.Diff()); err != nil {
			return structerr.NewInternal("sending diff: %w", err)
		}
	}
	if parser.Err() != nil {
		return fmt.Errorf("parsing diff: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("waiting for git-diff-pairs: %w", err)
	}

	return nil
}

func validateRawInfo(rawInfo []*gitalypb.ChangedPaths) error {
	for _, entry := range rawInfo {
		if len(entry.GetOldBlobId()) == 0 || len(entry.GetNewBlobId()) == 0 {
			return structerr.NewInvalidArgument("raw info entry missing blob IDs")
		}

		if len(entry.GetPath()) == 0 {
			return structerr.NewInvalidArgument("raw info entry missing path")
		}

		if entry.GetStatus() == gitalypb.ChangedPaths_RENAMED || entry.GetStatus() == gitalypb.ChangedPaths_COPIED {
			if len(entry.GetOldPath()) == 0 {
				return structerr.NewInvalidArgument("rename/copy raw info entry missing old path")
			}
		}
	}

	return nil
}

func (s *server) diffBlobs(ctx context.Context,
	request *gitalypb.DiffBlobsRequest,
	stream gitalypb.DiffService_DiffBlobsServer,
	cmdOpts []gitcmd.Option,
) error {
	// Unfortunately, git-diff(1) does not support generating a blob diff using a null OID as an
	// input argument. When a blob is added/deleted, there is no pre-image/post-image respectively.
	// To generate diffs for additions and deletions, the empty blob ID is used as either the left
	// of right blob pair. Unlike an empty tree object, an empty blob object is not special cased
	// and must exist in the repository to be used. Since the DiffBlobs RPC is read-only, we create
	// a quarantine directory to stage an empty blob object for use with diff generation only.
	quarantineDir, cleanup, err := quarantine.New(ctx, request.GetRepository(), s.logger, s.locator)
	if err != nil {
		return structerr.NewInternal("creating quarantine directory: %w", err)
	}
	defer cleanup()

	repo := s.localRepoFactory.Build(quarantineDir.QuarantinedRepo())

	if _, err := repo.WriteBlob(ctx, strings.NewReader(""), localrepo.WriteBlobConfig{}); err != nil {
		return structerr.NewInternal("writing empty blob: %w", err)
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return structerr.NewInternal("detecting object format: %w", err)
	}

	blobInfoPairs, err := s.blobInfoPairs(ctx, repo, objectHash, request.GetBlobPairs())
	if err != nil {
		return err
	}

	var limits diff.Limits
	if request.GetPatchBytesLimit() > 0 {
		limits.EnforceLimits = true
		limits.PatchLimitsOnly = true
		limits.MaxPatchBytes = int(request.GetPatchBytesLimit())
	}

	for _, blobInfoPair := range blobInfoPairs {
		// Each diff gets computed using an independent Git process and diff parser. Ideally a
		// single Git process could be used to process each blob pair, but unfortunately Git
		// does not yet have a means to accomplish this.
		blobDiff, err := diffBlob(ctx, repo, objectHash, blobInfoPair, limits, cmdOpts)
		if err != nil {
			return structerr.NewInternal("generating diff: %w", err)
		}

		if err := s.sendDiff(stream, blobDiff); err != nil {
			return structerr.NewInternal("sending diff: %w", err)
		}
	}

	return nil
}

func diffBlob(ctx context.Context,
	repo *localrepo.Repo,
	objectHash git.ObjectHash,
	blobInfoPair blobInfoPair,
	limits diff.Limits,
	opts []gitcmd.Option,
) (*diff.Diff, error) {
	left := blobInfoPair.leftRevision.String()
	right := blobInfoPair.rightRevision.String()

	emptyBlob, err := emptyBlobID(objectHash)
	if err != nil {
		return nil, err
	}

	// Rewrite null OIDs to an empty blob ID so diffs can be generated for additions and deletions.
	if objectHash.IsZeroOID(git.ObjectID(left)) {
		left = emptyBlob.String()
	}

	if objectHash.IsZeroOID(git.ObjectID(right)) {
		right = emptyBlob.String()
	}

	// Generating diffs between identical revisions is not supported as git-diff(1) does not produce
	// any patch or raw formatted output. Unfortunately, because NULL OIDs are rewritten to an empty
	// blob ID, it becomes possible for revisions to resolve to the same OID. For example, the newly
	// added file itself may also be empty and resolve to an empty blob. Luckily, it such scenarios,
	// the resulting diff is expected to be empty. Special case this situation by detecting matching
	// revisions and returning an empty diff early.
	if left == blobInfoPair.rightOID.String() || right == blobInfoPair.leftOID.String() {
		return &diff.Diff{
			FromID: blobInfoPair.leftOID.String(),
			ToID:   blobInfoPair.rightOID.String(),
		}, nil
	}

	gitCmd := gitcmd.Command{
		Name: "diff",
		Flags: []gitcmd.Option{
			// The diff parser requires raw output even if only a single diff is generated.
			gitcmd.Flag{Name: "--patch-with-raw"},
			gitcmd.Flag{Name: fmt.Sprintf("--abbrev=%d", objectHash.EncodedLen())},
		},
		Args: []string{left, right},
	}

	gitCmd.Flags = append(gitCmd.Flags, opts...)

	cmd, err := repo.Exec(ctx, gitCmd, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, fmt.Errorf("spawning git-diff: %w", err)
	}

	diffParser := diff.NewDiffParser(objectHash, cmd, limits)

	// Since a new parser is used for each computed diff, only a single diff should be generated.
	if !diffParser.Parse() {
		if diffParser.Err() != nil {
			return nil, diffParser.Err()
		}

		// Computing a diff using the same blob ID is not supported and results in an error. In this
		// scenario the `--raw` option would not produce any output and thus the parser thinks there
		// is no diffs to parse.
		return nil, errors.New("diff parser finished unexpectedly")
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for git-diff: %w", err)
	}

	blobDiff := diffParser.Diff()

	// If a null OID was initially requested, rewrite the empty blob ID back to a null OID.
	if objectHash.IsZeroOID(blobInfoPair.leftOID) {
		blobDiff.FromID = objectHash.ZeroOID.String()
	}

	if objectHash.IsZeroOID(blobInfoPair.rightOID) {
		blobDiff.ToID = objectHash.ZeroOID.String()
	}

	return blobDiff, nil
}

func (s *server) sendDiff(stream gitalypb.DiffService_DiffBlobsServer, diff *diff.Diff) error {
	response := &gitalypb.DiffBlobsResponse{
		LeftBlobId:          diff.FromID,
		RightBlobId:         diff.ToID,
		Binary:              diff.Binary,
		OverPatchBytesLimit: diff.TooLarge,
		PatchSize:           diff.PatchSize,
		LinesAdded:          diff.LinesAdded,
		LinesRemoved:        diff.LinesRemoved,
	}

	for {
		if len(diff.Patch) > s.MsgSizeThreshold {
			response.Patch = diff.Patch[:s.MsgSizeThreshold]
			diff.Patch = diff.Patch[s.MsgSizeThreshold:]
		} else {
			response.Patch = diff.Patch
			response.Status = gitalypb.DiffBlobsResponse_STATUS_END_OF_PATCH
			diff.Patch = nil
		}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("send: %w", err)
		}

		if len(diff.Patch) == 0 {
			break
		}

		response = &gitalypb.DiffBlobsResponse{}
	}

	return nil
}

type blobInfoPair struct {
	leftOID       git.ObjectID
	rightOID      git.ObjectID
	leftRevision  git.Revision
	rightRevision git.Revision
}

func (s *server) blobInfoPairs(
	ctx context.Context,
	repo *localrepo.Repo,
	objectHash git.ObjectHash,
	blobPairs []*gitalypb.DiffBlobsRequest_BlobPair,
) ([]blobInfoPair, error) {
	var blobInfoPairs []blobInfoPair

	reader, readerCancel, err := s.catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("retrieving object reader: %w", err)
	}
	defer readerCancel()

	for _, blobPair := range blobPairs {
		blobInfoPair := blobInfoPair{
			leftOID:       objectHash.ZeroOID,
			rightOID:      objectHash.ZeroOID,
			leftRevision:  git.Revision(blobPair.GetLeftBlob()),
			rightRevision: git.Revision(blobPair.GetRightBlob()),
		}

		// Null blob IDs do not exist in the repository.
		if !objectHash.IsZeroOID(git.ObjectID(blobPair.GetLeftBlob())) {
			leftOID, err := blobInfo(ctx, reader, objectHash, blobPair.GetLeftBlob())
			if err != nil {
				return nil, structerr.NewInvalidArgument("getting left blob info: %w", err).WithMetadata(
					"revision",
					string(blobPair.GetLeftBlob()),
				)
			}
			blobInfoPair.leftOID = leftOID
		}

		if !objectHash.IsZeroOID(git.ObjectID(blobPair.GetRightBlob())) {
			rightOID, err := blobInfo(ctx, reader, objectHash, blobPair.GetRightBlob())
			if err != nil {
				return nil, structerr.NewInvalidArgument("getting right blob info: %w", err).WithMetadata(
					"revision",
					string(blobPair.GetRightBlob()),
				)
			}
			blobInfoPair.rightOID = rightOID
		}

		if blobInfoPair.leftOID == blobInfoPair.rightOID {
			return nil, structerr.NewInvalidArgument("left and right blob revisions resolve to same OID").WithMetadataItems(
				structerr.MetadataItem{Key: "left_revision", Value: string(blobPair.GetLeftBlob())},
				structerr.MetadataItem{Key: "right_revision", Value: string(blobPair.GetRightBlob())},
			)
		}

		blobInfoPairs = append(blobInfoPairs, blobInfoPair)
	}

	return blobInfoPairs, nil
}

func blobInfo(
	ctx context.Context,
	reader catfile.ObjectInfoReader,
	objectHash git.ObjectHash,
	revision []byte,
) (git.ObjectID, error) {
	// Since only blobs are allowed, only path-scoped revisions and blob IDs are accepted.
	if bytes.Contains(revision, []byte(":")) {
		if err := git.ValidateRevision(revision, git.AllowPathScopedRevision()); err != nil {
			return "", fmt.Errorf("validating path-scoped revision: %w", err)
		}
	} else {
		if err := objectHash.ValidateHex(string(revision)); err != nil {
			return "", fmt.Errorf("validating blob ID: %w", err)
		}
	}

	info, err := reader.Info(ctx, git.Revision(revision))
	if err != nil {
		return "", fmt.Errorf("getting revision info: %w", err)
	} else if !info.IsBlob() {
		return "", errors.New("revision is not blob")
	}

	return info.Oid, nil
}

func emptyBlobID(objectHash git.ObjectHash) (git.ObjectID, error) {
	switch objectHash.Format {
	case git.ObjectHashSHA1.Format:
		return "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", nil
	case git.ObjectHashSHA256.Format:
		return "473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813", nil
	default:
		return "", fmt.Errorf("unknown object format: %q", objectHash.Format)
	}
}
