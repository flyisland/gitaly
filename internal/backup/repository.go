package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/migration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RefIterator is an interface for iterating over refs.
type RefIterator interface {
	Next() bool
	Ref() git.Reference
	Close() error
	Err() error
}

type remoteRefIterator struct {
	stream gitalypb.RefService_ListRefsClient
	buffer []*gitalypb.ListRefsResponse_Reference
	ref    git.Reference
	err    error
}

// NewRefIterator creates new ref iterator for remote repository.
func (rr *remoteRepository) NewRefIterator(ctx context.Context) (RefIterator, error) {
	refClient := rr.newRefClient()
	stream, err := refClient.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: rr.repo,
		Head:       true,
		Patterns:   [][]byte{[]byte("refs/")},
	})
	if err != nil {
		return nil, fmt.Errorf("remote repository: new ref iterator: %w", err)
	}

	return &remoteRefIterator{stream: stream}, nil
}

// refillBuffer receives the next batch of refs from the stream and adds it to the iterator's buffer.
func (i *remoteRefIterator) refillBuffer() error {
	if len(i.buffer) > 0 {
		return nil
	}

	resp, err := i.stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("stream recv: %w", err)
	}

	i.buffer = resp.GetReferences()
	return nil
}

// Next advances the iterator to the next ref.
func (i *remoteRefIterator) Next() bool {
	if len(i.buffer) == 0 {
		if err := i.refillBuffer(); err != nil {
			i.err = fmt.Errorf("refill buffer: %w", err)
			return false
		}

		if len(i.buffer) == 0 {
			return false
		}
	}

	ref := i.buffer[0]
	i.buffer = i.buffer[1:]
	i.ref = git.Reference{
		Name:   git.ReferenceName(ref.GetName()),
		Target: ref.GetTarget(),
	}

	return true
}

// Close closes the ref iterator.
func (i *remoteRefIterator) Close() error {
	// The gRPC stream doesn't have a Close method, but we might want to
	// add any cleanup logic here if needed in the future.
	return nil
}

// Ref returns the current reference of the iterator.
func (i *remoteRefIterator) Ref() git.Reference {
	return i.ref
}

// Err returns the error of the iterator.
func (i *remoteRefIterator) Err() error {
	return i.err
}

// remoteRepository implements git repository access over GRPC
type remoteRepository struct {
	repo *gitalypb.Repository
	conn *grpc.ClientConn
}

// NewRemoteRepository returns a repository accessor that operates on a remote
// repository.
func NewRemoteRepository(repo *gitalypb.Repository, conn *grpc.ClientConn) *remoteRepository {
	return &remoteRepository{
		repo: repo,
		conn: conn,
	}
}

// ListRefs fetches the full set of refs and targets for the repository
func (rr *remoteRepository) ListRefs(ctx context.Context) (RefIterator, error) {
	return rr.NewRefIterator(ctx)
}

// GetCustomHooks fetches the custom hooks archive.
func (rr *remoteRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: rr.repo})
	if err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	hooks := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	if _, err := io.Copy(out, hooks); err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns. When
// patterns is nil all refs are bundled.
func (rr *remoteRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	if patterns != nil {
		return rr.createBundlePatterns(ctx, out, patterns)
	}

	return rr.createBundle(ctx, out)
}

func (rr *remoteRepository) createBundle(ctx context.Context, out io.Writer) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.CreateBundle(ctx, &gitalypb.CreateBundleRequest{
		Repository: rr.repo,
	})
	if err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			err = localrepo.ErrEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	return nil
}

func (rr *remoteRepository) createBundlePatterns(ctx context.Context, out io.Writer, patterns io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.CreateBundleFromRefList(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: %w", err)
	}
	c := chunk.New(&createBundleFromRefListSender{
		stream: stream,
	})

	buf := bufio.NewReader(patterns)
	for {
		line, err := buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("remote repository: create bundle patterns: %w", err)
		}

		line = bytes.TrimSuffix(line, []byte("\n"))

		if err := c.Send(&gitalypb.CreateBundleFromRefListRequest{
			Repository: rr.repo,
			Patterns:   [][]byte{line},
		}); err != nil {
			return fmt.Errorf("remote repository: create bundle patterns: send: %w", err)
		}
	}
	if err := c.Flush(); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: flush: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: close: %w", err)
	}

	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			err = localrepo.ErrEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: recv: %w", err)
	}

	return nil
}

type createBundleFromRefListSender struct {
	stream gitalypb.RepositoryService_CreateBundleFromRefListClient
	chunk  gitalypb.CreateBundleFromRefListRequest
}

// Reset should create a fresh response message.
func (s *createBundleFromRefListSender) Reset() {
	s.chunk = gitalypb.CreateBundleFromRefListRequest{}
}

// Append should append the given item to the slice in the current response message
func (s *createBundleFromRefListSender) Append(msg proto.Message) {
	req := msg.(*gitalypb.CreateBundleFromRefListRequest)
	s.chunk.Repository = req.GetRepository()
	s.chunk.Patterns = append(s.chunk.Patterns, req.GetPatterns()...)
}

// Send should send the current response message
func (s *createBundleFromRefListSender) Send() error {
	err := s.stream.Send(&s.chunk)
	if err != nil {
		// On error, SendMsg aborts the stream. If the error was generated by
		// the client, the status is returned directly; otherwise, io.EOF is
		// returned and the status of the stream may be discovered using Recv.
		if errors.Is(err, io.EOF) {
			if _, recvErr := s.stream.Recv(); recvErr != nil {
				return recvErr
			}
		}
	}

	return err
}

// updateRefsSender chunks requests to the UpdateReferences RPC.
type updateRefsSender struct {
	refs   []*gitalypb.UpdateReferencesRequest_Update
	stream gitalypb.RefService_UpdateReferencesClient
}

// Reset should create a fresh response message.
func (s *updateRefsSender) Reset() {
	s.refs = s.refs[:0]
}

// Append should append the given item to the slice in the current response message
func (s *updateRefsSender) Append(msg proto.Message) {
	s.refs = append(s.refs, msg.(*gitalypb.UpdateReferencesRequest_Update))
}

// Send should send the current response message
func (s *updateRefsSender) Send() error {
	err := s.stream.Send(&gitalypb.UpdateReferencesRequest{
		Updates: s.refs,
	})
	if err != nil {
		// On error, SendMsg aborts the stream. If the error was generated by
		// the client, the status is returned directly; otherwise, io.EOF is
		// returned and the status of the stream may be discovered using Recv.
		if errors.Is(err, io.EOF) {
			if _, recvErr := s.stream.CloseAndRecv(); recvErr != nil {
				return recvErr
			}
		}
	}

	return err
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (rr *remoteRepository) Remove(ctx context.Context) error {
	repoClient := rr.newRepoClient()
	_, err := repoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: rr.repo,
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("remote repository: remove: %w", err)
	}
	return nil
}

// ResetRefs attempts to reset the list of refs in the repository to match the
// specified refs slice. Do not include the symbolic HEAD reference in the list.
func (rr *remoteRepository) ResetRefs(ctx context.Context, refs []git.Reference) error {
	if len(refs) == 0 {
		return errors.New("empty refs list")
	}

	existingRefs := []git.Reference{}
	iterator, err := rr.ListRefs(ctx)
	if err != nil {
		return fmt.Errorf("list refs: %w", err)
	}
	for iterator.Next() {
		ref := iterator.Ref()
		existingRefs = append(existingRefs, ref)
	}
	if err := iterator.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("list refs: %w", err)
	}

	objectHash, err := rr.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	refClient := rr.newRefClient()

	// Add updates to delete existing refs not in the new set
	refsToKeep := make(map[git.ReferenceName]struct{}, len(refs))
	for _, ref := range refs {
		refsToKeep[ref.Name] = struct{}{}
	}
	removeUpdates := make([]*gitalypb.UpdateReferencesRequest_Update, 0, len(existingRefs))
	for _, existingRef := range existingRefs {
		if shouldRemoveRef(refsToKeep, existingRef.Name) {
			removeUpdates = append(removeUpdates, &gitalypb.UpdateReferencesRequest_Update{
				Reference:   []byte(existingRef.Name),
				NewObjectId: []byte(objectHash.ZeroOID),
			})
		}
	}
	// Add updates to create or modify refs in the new set
	updates := make([]*gitalypb.UpdateReferencesRequest_Update, 0, len(refs))
	for _, newRef := range refs {
		if shouldUpdateRef(existingRefs, newRef) {
			updates = append(updates, &gitalypb.UpdateReferencesRequest_Update{
				Reference:   []byte(newRef.Name),
				NewObjectId: []byte(newRef.Target),
			})
		}
	}

	allUpdates := make([]*gitalypb.UpdateReferencesRequest_Update, 0, len(removeUpdates)+len(updates))
	allUpdates = append(allUpdates, removeUpdates...)
	allUpdates = append(allUpdates, updates...)
	if err := rr.sendRefUpdates(ctx, refClient, allUpdates); err != nil {
		return fmt.Errorf("update refs: %w", err)
	}

	return nil
}

// sendRefUpdates sends a batch of ref updates to the remote repository.
func (rr *remoteRepository) sendRefUpdates(ctx context.Context, refClient gitalypb.RefServiceClient, updates []*gitalypb.UpdateReferencesRequest_Update) error {
	if len(updates) == 0 {
		return nil
	}

	stream, err := refClient.UpdateReferences(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// We need to send the first update without the chunker, because the RPC expects the `Repository` field to be
	// empty in subsequent messages. We also need to send the first reference because the RPC expects at least one
	// update.
	if err := stream.Send(&gitalypb.UpdateReferencesRequest{
		Repository: rr.repo,
		Updates: []*gitalypb.UpdateReferencesRequest_Update{
			updates[0],
		},
	}); err != nil {
		return fmt.Errorf("send initial request: %w", err)
	}

	chunker := chunk.New(&updateRefsSender{stream: stream})

	for _, update := range updates[1:] {
		if err := chunker.Send(update); err != nil {
			return fmt.Errorf("send ref update: %w", err)
		}
	}

	if err := chunker.Flush(); err != nil {
		return fmt.Errorf("flush ref update chunker: %w", err)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("close stream: %w", err)
	}

	return nil
}

// SetHeadReference sets the symbolic HEAD reference of the repository.
func (rr *remoteRepository) SetHeadReference(ctx context.Context, target git.ReferenceName) error {
	repoClient := rr.newRepoClient()

	_, err := repoClient.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository: rr.repo,
		Ref:        []byte("HEAD"),
		Revision:   []byte(target),
	})
	if err != nil {
		return fmt.Errorf("write HEAD ref: %w", err)
	}

	return nil
}

// Create creates the repository.
func (rr *remoteRepository) Create(ctx context.Context, hash git.ObjectHash, defaultBranch string) error {
	repoClient := rr.newRepoClient()
	if _, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository:    rr.repo,
		ObjectFormat:  hash.ProtoFormat,
		DefaultBranch: []byte(defaultBranch),
	}); err != nil {
		return fmt.Errorf("remote repository: create: %w", err)
	}
	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (rr *remoteRepository) FetchBundle(ctx context.Context, reader io.Reader, updateHead bool) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.FetchBundle(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	request := &gitalypb.FetchBundleRequest{Repository: rr.repo, UpdateHead: updateHead}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.FetchBundleRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (rr *remoteRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.SetCustomHooks(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}

	request := &gitalypb.SetCustomHooksRequest{Repository: rr.repo}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.SetCustomHooksRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}
	return nil
}

// ObjectHash detects the object hash used by the repository.
func (rr *remoteRepository) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	repoClient := rr.newRepoClient()

	response, err := repoClient.ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
		Repository: rr.repo,
	})
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("remote repository: object hash: %w", err)
	}

	return git.ObjectHashByProto(response.GetFormat())
}

// HeadReference returns the current value of HEAD.
func (rr *remoteRepository) HeadReference(ctx context.Context) (git.ReferenceName, error) {
	refClient := rr.newRefClient()

	response, err := refClient.FindDefaultBranchName(ctx, &gitalypb.FindDefaultBranchNameRequest{
		Repository: rr.repo,
		HeadOnly:   true,
	})
	if err != nil {
		return "", fmt.Errorf("remote repository: head reference: %w", err)
	}

	return git.ReferenceName(response.GetName()), nil
}

func (rr *remoteRepository) newRepoClient() gitalypb.RepositoryServiceClient {
	return gitalypb.NewRepositoryServiceClient(rr.conn)
}

func (rr *remoteRepository) newRefClient() gitalypb.RefServiceClient {
	return gitalypb.NewRefServiceClient(rr.conn)
}

type localRepository struct {
	logger                log.Logger
	locator               storage.Locator
	gitCmdFactory         gitcmd.CommandFactory
	txManager             transaction.Manager
	repoCounter           *counter.RepositoryCounter
	catfileCache          catfile.Cache
	repo                  *localrepo.Repo
	migrationStateManager migration.StateManager
}

// NewLocalRepository returns a repository accessor that operates on a local
// repository.
func NewLocalRepository(
	logger log.Logger,
	locator storage.Locator,
	gitCmdFactory gitcmd.CommandFactory,
	txManager transaction.Manager,
	repoCounter *counter.RepositoryCounter,
	catfileCache catfile.Cache,
	repo *localrepo.Repo,
	migrationStateManager migration.StateManager,
) *localRepository {
	return &localRepository{
		logger:                logger,
		locator:               locator,
		gitCmdFactory:         gitCmdFactory,
		txManager:             txManager,
		repoCounter:           repoCounter,
		catfileCache:          catfileCache,
		repo:                  repo,
		migrationStateManager: migrationStateManager,
	}
}

type localRefIterator struct {
	scanner *bufio.Scanner
	cmd     *command.Command
	ref     git.Reference
	init    bool
	err     error
}

// Next advances the iterator to the next ref.
func (i *localRefIterator) Next() bool {
	// HEAD symref is not returned by the git for-each-ref output but explicitly set
	// to the iterator when it is created. Thus we are using init flag to return HEAD
	// first before consuming the actual git for-each-ref output.
	if i.init {
		i.init = false
		return true
	}

	if !i.scanner.Scan() {
		if err := i.scanner.Err(); err != nil {
			i.err = fmt.Errorf("scan error: %w", err)
		}
		return false
	}

	line := bytes.SplitN(i.scanner.Bytes(), []byte{0}, 3)
	if len(line) != 3 {
		i.err = errors.New("unexpected reference format")
		return false
	}

	i.ref = git.Reference{
		Name:   git.ReferenceName(line[0]),
		Target: string(line[1]),
	}

	return true
}

// Ref returns the current ref of the iterator.
func (i *localRefIterator) Ref() git.Reference {
	return i.ref
}

// Close wait for the underlying command to finish.
func (i *localRefIterator) Close() error {
	return i.cmd.Wait()
}

// Err returns the error of the iterator.
func (i *localRefIterator) Err() error {
	return i.err
}

// NewRefIterator creates new ref iterator for local repository
func (r *localRepository) NewRefIterator(ctx context.Context) (RefIterator, error) {
	cmd, err := r.repo.Exec(ctx, gitcmd.Command{
		Name:  "for-each-ref",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--format=%(refname)%00%(objectname)%00%(symref)"}},
		Args:  []string{"refs/"},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return nil, err
	}

	iterator := localRefIterator{
		scanner: bufio.NewScanner(cmd),
		cmd:     cmd,
		init:    true,
	}

	headOID, err := r.repo.ResolveRevision(ctx, git.Revision("HEAD"))
	switch {
	case errors.Is(err, git.ErrReferenceNotFound):
		iterator.init = false
	case err != nil:
		return nil, structerr.NewInternal("local repository: resolve head: %w", err)
	default:
		iterator.ref = git.NewReference("HEAD", headOID)
	}

	return &iterator, nil
}

// ListRefs returns iterator to fetch the full set of refs for the repository.
func (r *localRepository) ListRefs(ctx context.Context) (RefIterator, error) {
	return r.NewRefIterator(ctx)
}

// GetCustomHooks fetches the custom hooks archive.
func (r *localRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoPath, err := r.locator.GetRepoPath(ctx, r.repo)
	if err != nil {
		return fmt.Errorf("get repo path: %w", err)
	}

	if err := repoutil.GetCustomHooks(ctx, r.logger, repoPath, out); err != nil {
		return fmt.Errorf("local repository: get custom hooks: %w", err)
	}
	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns. When
// patterns is nil all refs are bundled.
func (r *localRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	err := r.repo.CreateBundle(ctx, out, &localrepo.CreateBundleOpts{
		Patterns: patterns,
	})
	if err != nil {
		return fmt.Errorf("local repository: create bundle: %w", err)
	}

	return nil
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (r *localRepository) Remove(ctx context.Context) error {
	err := repoutil.Remove(ctx, r.logger, r.locator, r.txManager, r.repoCounter, r.repo)
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("local repository: remove: %w", err)
	}
	return nil
}

// Create creates the repository.
func (r *localRepository) Create(ctx context.Context, hash git.ObjectHash, defaultBranch string) error {
	if err := repoutil.Create(
		ctx,
		r.logger,
		r.locator,
		r.gitCmdFactory,
		r.catfileCache,
		r.txManager,
		r.repoCounter,
		r.repo,
		func(repository *gitalypb.Repository) error { return nil },
		repoutil.WithObjectHash(hash),
		repoutil.WithBranchName(defaultBranch),
	); err != nil {
		return fmt.Errorf("local repository: create: %w", err)
	}

	if tx := storage.ExtractTransaction(ctx); tx != nil {
		repo := &gitalypb.Repository{
			StorageName:  r.repo.GetStorageName(),
			RelativePath: r.repo.GetRelativePath(),
		}

		if err := r.migrationStateManager.RecordKeyCreation(
			tx,
			tx.OriginalRepository(repo).GetRelativePath(),
		); err != nil {
			return fmt.Errorf("recording migration key: %w", err)
		}
	}

	// Recreate the local repository, since the cache of object hash and ref-format needs
	// to be invalidated.
	r.repo = localrepo.New(r.logger, r.locator, r.gitCmdFactory, r.catfileCache, r.repo)

	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (r *localRepository) FetchBundle(ctx context.Context, reader io.Reader, updateHead bool) error {
	err := r.repo.FetchBundle(ctx, r.txManager, reader, &localrepo.FetchBundleOpts{
		UpdateHead: updateHead,
	})
	if err != nil {
		return fmt.Errorf("local repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (r *localRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	if err := repoutil.SetCustomHooks(
		ctx,
		r.logger,
		r.locator,
		r.txManager,
		reader,
		r.repo,
	); err != nil {
		return fmt.Errorf("local repository: set custom hooks: %w", err)
	}
	return nil
}

// ObjectHash detects the object hash used by the repository.
func (r *localRepository) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	hash, err := r.repo.ObjectHash(ctx)
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("local repository: object hash: %w", err)
	}

	return hash, nil
}

// HeadReference returns the current value of HEAD.
func (r *localRepository) HeadReference(ctx context.Context) (git.ReferenceName, error) {
	head, err := r.repo.HeadReference(ctx)
	if err != nil {
		return "", fmt.Errorf("local repository: head reference: %w", err)
	}

	return head, nil
}

// ResetRefs attempts to reset the list of refs in the repository to match the
// specified refs slice. Do not include the symbolic HEAD reference in the list.
func (r *localRepository) ResetRefs(ctx context.Context, refs []git.Reference) (returnedErr error) {
	existingRefs, err := r.repo.GetReferences(ctx)
	if err != nil {
		return fmt.Errorf("get existing refs: %w", err)
	}

	objectHash, err := r.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	// Prepare updates to delete existing refs not in the new set
	removeUpdates := make([]git.Reference, 0, len(existingRefs))
	refsToKeep := make(map[git.ReferenceName]struct{}, len(refs))
	for _, ref := range refs {
		refsToKeep[ref.Name] = struct{}{}
	}
	for _, existingRef := range existingRefs {
		if shouldRemoveRef(refsToKeep, existingRef.Name) {
			removeUpdates = append(removeUpdates, git.NewReference(existingRef.Name, objectHash.ZeroOID))
		}
	}
	// Prepare updates to create or modify refs in the new set
	updates := make([]git.Reference, 0, len(refs))
	for _, newRef := range refs {
		if shouldUpdateRef(existingRefs, newRef) {
			updates = append(updates, newRef)
		}
	}

	allUpdates := make([]git.Reference, 0, len(removeUpdates)+len(updates))
	allUpdates = append(allUpdates, removeUpdates...)
	allUpdates = append(allUpdates, updates...)
	if err := r.updateRefs(ctx, allUpdates); err != nil {
		return fmt.Errorf("update refs: %w", err)
	}

	return nil
}

func (r *localRepository) updateRefs(ctx context.Context, updates []git.Reference) (returnedErr error) {
	u, err := updateref.New(ctx, r.repo)
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %w", err)
	}
	defer func() {
		if err := u.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("close updater: %w", err)
		}
	}()

	if err := u.Start(); err != nil {
		return fmt.Errorf("start reset refs transaction: %w", err)
	}

	for _, update := range updates {
		if err := u.Update(update.Name, git.ObjectID(update.Target), ""); err != nil {
			return fmt.Errorf("reset ref: %w", err)
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit reset refs: %w", err)
	}

	return nil
}

// SetHeadReference sets the symbolic HEAD reference of the repository.
func (r *localRepository) SetHeadReference(ctx context.Context, target git.ReferenceName) error {
	return r.repo.SetDefaultBranch(ctx, r.txManager, target)
}

// shouldRemoveRef checks if a reference should be removed from the repository.
func shouldRemoveRef(refsToKeep map[git.ReferenceName]struct{}, name git.ReferenceName) bool {
	// Updating HEAD reference is not allowed in UpdateReferences call.
	if name == "HEAD" {
		return false
	}

	if _, shouldKeep := refsToKeep[name]; shouldKeep {
		return false
	}

	return true
}

// shouldUpdateRef checks if a reference value has changed, and should be updated.
func shouldUpdateRef(existingRefs []git.Reference, newRef git.Reference) bool {
	// Updating HEAD reference is not allowed in UpdateReferences call.
	if newRef.Name == "HEAD" {
		return false
	}

	for _, existingRef := range existingRefs {
		if existingRef.Name == newRef.Name && existingRef.Target == newRef.Target {
			return false
		}
	}

	return true
}
