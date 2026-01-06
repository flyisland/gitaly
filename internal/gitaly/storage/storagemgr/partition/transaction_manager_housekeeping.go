package partition

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v18/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/wal"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/wal/reftree"
	"gitlab.com/gitlab-org/gitaly/v18/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gocloud.dev/gcerrors"
	"golang.org/x/exp/maps"
)

const (
	packFileDir = "pack"
	objectsDir  = "objects"
	configFile  = "config"
)

var (
	// errConflictRepositoryDeletion is returned when an operation conflicts with repository deletion in another
	// transaction.
	errConflictRepositoryDeletion = errors.New("detected an update conflicting with repository deletion")
	// errPackRefsConflictRefDeletion is returned when there is a committed ref deletion before pack-refs
	// task is committed. The transaction should be aborted.
	errPackRefsConflictRefDeletion = errors.New("detected a conflict with reference deletion when committing packed-refs")
	// errHousekeepingConflictConcurrent is returned when there are another concurrent housekeeping task.
	errHousekeepingConflictConcurrent = errors.New("conflict with another concurrent housekeeping task")
	// errRepackConflictPrunedObject is returned when the repacking task pruned an object that is still used by other
	// concurrent transactions.
	errRepackConflictPrunedObject = errors.New("pruned object used by other updates")
	// errRepackNotSupportedStrategy is returned when the manager runs the repacking task using unsupported strategy.
	errRepackNotSupportedStrategy = errors.New("strategy not supported")
	// errConcurrentAlternateUnlink is a repack attempts to commit against a repository that was concurrently unlinked
	// from an alternate
	errConcurrentAlternateUnlink = errors.New("concurrent alternate unlinking with repack")

	errOffloadingObjectUpload   = errors.New("upload to offloading storage")
	errOffloadingOnRepacking    = errors.New("repack for offloading")
	errOffloadingObjectDownload = errors.New("download from offloading storage")
)

// runHousekeeping models housekeeping tasks. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type runHousekeeping struct {
	packRefs          *runPackRefs
	repack            *runRepack
	writeCommitGraphs *writeCommitGraphs
	runOffloading     *runOffloading
	runRehydrating    *runRehydrating
}

// runPackRefs models refs packing housekeeping task. It packs heads and tags for efficient repository access.
type runPackRefs struct {
	// PrunedRefs contain a list of references pruned by the `git-pack-refs` command. They are used
	// for comparing to the ref list of the destination repository
	PrunedRefs map[git.ReferenceName]struct{}
	// emptyDirectories contain a list of empty directories in the transaction snapshot. It is used
	// to delete those directories during pack refs' post stage.
	emptyDirectories map[string]struct{}
	// reftablesBefore contains the data in 'tables.list' before the compaction. This is used to
	// compare with the destination repositories 'tables.list'.
	reftablesBefore []reftable.Name
	// reftablesAfter contains the data in 'tables.list' after the compaction. This is used for
	// generating the combined 'tables.list' during verification.
	reftablesAfter []reftable.Name
}

// runRepack models repack housekeeping task. We support multiple repacking strategies. At this stage, the outside
// scheduler determines which strategy to use. The transaction manager is responsible for executing it. In the future,
// we want to make housekeeping smarter by migrating housekeeping scheduling responsibility to this manager. That work
// is tracked in https://gitlab.com/gitlab-org/gitaly/-/issues/5709.
type runRepack struct {
	// config tells which strategy and baggaged options.
	config housekeepingcfg.RepackObjectsConfig
}

// writeCommitGraphs models a commit graph update.
type writeCommitGraphs struct {
	// config includes the configs for writing commit graph.
	config housekeepingcfg.WriteCommitGraphConfig
}

// runOffloading models offloading tasks. It stores configuration information needed to offload a repository.
type runOffloading struct {
	config housekeepingcfg.OffloadingConfig
}

type runRehydrating struct {
	prefix string
}

// prepareHousekeeping composes and prepares necessary steps on the staging repository before the changes are staged and
// applied. All commands run in the scope of the staging repository. Thus, we can avoid any impact on other concurrent
// transactions.
func (mgr *TransactionManager) prepareHousekeeping(ctx context.Context, transaction *Transaction) error {
	if transaction.runHousekeeping == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareHousekeeping", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	if err := mgr.preparePackRefs(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareRepacking(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareCommitGraphs(ctx, transaction); err != nil {
		return err
	}
	if err := mgr.prepareOffloading(ctx, transaction); err != nil {
		return fmt.Errorf("preparing offloading: %w", err)
	}
	if err := mgr.prepareRehydrating(ctx, transaction); err != nil {
		return fmt.Errorf("preparing rehydrating: %w", err)
	}
	return nil
}

// preparePackRefs runs pack refs on the repository after detecting
// its reference backend type.
func (mgr *TransactionManager) preparePackRefs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "preparePackRefs").End()

	if transaction.runHousekeeping.packRefs == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.preparePackRefs", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "prepare")
	defer finishTimer()

	refBackend, err := transaction.snapshotRepository.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("reference backend: %w", err)
	}

	if refBackend == git.ReferenceBackendReftables {
		if err = mgr.preparePackRefsReftable(ctx, transaction); err != nil {
			return fmt.Errorf("reftable backend: %w", err)
		}
		return nil
	}

	if err = mgr.preparePackRefsFiles(ctx, transaction); err != nil {
		return fmt.Errorf("files backend: %w", err)
	}
	return nil
}

// Git stores loose objects in the object directory under subdirectories with two hex digits in their name.
var regexpLooseObjectDir = regexp.MustCompile("^[[:xdigit:]]{2}$")

// prepareRepacking runs git-repack(1) command against the snapshot repository using desired repacking strategy. Each
// strategy has a different cost and effect corresponding to scheduling frequency.
// - IncrementalWithUnreachable: pack all loose objects into one packfile. This strategy is a no-op because all new
// objects regardless of their reachablity status are packed by default by the manager.
// - Geometric: merge all packs together with geometric repacking. This is expensive or cheap depending on which packs
// get merged. No need for a connectivity check.
// - FullWithUnreachable: merge all packs into one but keep unreachable objects. This is more expensive but we don't
// take connectivity into account. This strategy is essential for object pool. As we cannot prune objects in a pool,
// packing them into one single packfile boosts its performance.
// - FullWithCruft: Merge all packs into one and prune unreachable objects. It is the most effective, but yet costly
// strategy. We cannot run this type of task frequently on a large repository. This strategy is handled as a full
// repacking without cruft because we don't need object expiry.
// Before the command runs, we capture a snapshot of existing packfiles. After the command finishes, we re-capture the
// list and extract the list of to-be-updated packfiles. This practice is to prevent repacking task from deleting
// packfiles of other concurrent updates at the applying phase.
func (mgr *TransactionManager) prepareRepacking(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareRepacking").End()

	if transaction.runHousekeeping.repack == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareRepacking", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "prepare")
	defer finishTimer()

	var err error
	repack := transaction.runHousekeeping.repack

	// Build a working repository pointing to snapshot repository. Housekeeping task can access the repository
	// without the needs for quarantine.
	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	repoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	isFullRepack, err := housekeeping.ValidateRepacking(repack.config)
	if err != nil {
		return fmt.Errorf("validating repacking: %w", err)
	}

	if repack.config.Strategy == housekeepingcfg.RepackObjectsStrategyIncrementalWithUnreachable {
		// Once the transaction manager has been applied and at least one complete repack has occurred, there
		// should be no loose unreachable objects remaining in the repository. When the transaction manager
		// processes a change, it consolidates all unreachable objects and objects about to become reachable
		// into a new packfile, which is then placed in the repository. As a result, unreachable objects may
		// still exist but are confined to packfiles. These will eventually be cleaned up during a full repack.
		// In the interim, geometric repacking is utilized to optimize the structure of packfiles for faster
		// access. Therefore, this operation is effectively a no-op. However, we maintain it for the sake of
		// backward compatibility with the existing housekeeping scheduler.
		return errRepackNotSupportedStrategy
	}

	// Capture the list of packfiles and their baggages before repacking.
	beforeFiles, err := mgr.collectPackFiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	// midx file is different from other pack files because the file name stays
	// same after repacking but it's content changes. We need to save the stat
	// information of the midx file to compare modification times after repacking.
	midxFileName := "multi-pack-index"
	midxPath := filepath.Join(repoPath, "objects", "pack", midxFileName)
	oldMidxInode, err := wal.GetInode(midxPath)
	if err != nil {
		return fmt.Errorf("get midx inode before repacking: %w", err)
	}

	// All of the repacking operations pack/remove all loose objects. New ones are not written anymore with transactions.
	// As we're packing them away not, log their removal.
	objectsDirRelativePath := filepath.Join(transaction.relativePath, "objects")
	objectsDirEntries, err := os.ReadDir(filepath.Join(transaction.snapshot.Root(), objectsDirRelativePath))
	if err != nil {
		return fmt.Errorf("read objects dir: %w", err)
	}

	for _, entry := range objectsDirEntries {
		if entry.IsDir() && regexpLooseObjectDir.MatchString(entry.Name()) {
			if err := storage.RecordDirectoryRemoval(transaction.FS(), transaction.FS().Root(), filepath.Join(objectsDirRelativePath, entry.Name())); err != nil {
				return fmt.Errorf("record loose object dir removal: %w", err)
			}
		}
	}

	switch repack.config.Strategy {
	case housekeepingcfg.RepackObjectsStrategyGeometric:
		// Geometric repacking rearranges the list of packfiles according to a geometric progression. This process
		// does not consider object reachability. Since all unreachable objects remain within small packfiles,
		// they become included in the newly created packfiles. Geometric repacking does not prune any objects.
		if err := housekeeping.PerformGeometricRepacking(ctx, workingRepository, repack.config); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithUnreachable:
		// Git does not pack loose unreachable objects if there are no existing packs in the repository.
		// Perform an incremental repack first. This ensures all loose object are part of a pack and will be
		// included in the full pack we're about to build. This allows us to remove the loose objects from the
		// repository when applying the pack without losing any objects.
		//
		// Issue: https://gitlab.com/gitlab-org/git/-/issues/336
		if err := housekeeping.PerformIncrementalRepackingWithUnreachable(ctx, workingRepository); err != nil {
			return fmt.Errorf("perform geometric repacking: %w", err)
		}

		// This strategy merges all packfiles into a single packfile, simultaneously removing any loose objects
		// if present. Unreachable objects are then appended to the end of this unified packfile. Although the
		// `git-repack(1)` command does not offer an option to specifically pack loose unreachable objects, this
		// is not an issue because the transaction manager already ensures that unreachable objects are
		// contained within packfiles. Therefore, this strategy effectively consolidates all packfiles into a
		// single one. Adopting this strategy is crucial for alternates, as it ensures that we can manage
		// objects within an object pool without the capability to prune them.
		if err := housekeeping.PerformFullRepackingWithUnreachable(ctx, workingRepository, repack.config); err != nil {
			return err
		}
	case housekeepingcfg.RepackObjectsStrategyFullWithCruft:
		// Both of above strategies don't prune unreachable objects. They re-organize the objects between
		// packfiles. In the traditional housekeeping, the manager gets rid of unreachable objects via full
		// repacking with cruft. It pushes all unreachable objects to a cruft packfile and keeps track of each
		// object mtimes. All unreachable objects exceeding a grace period are cleaned up. The grace period is
		// to ensure the housekeeping doesn't delete a to-be-reachable object accidentally, for example when GC
		// runs while a concurrent push is being processed.
		// The transaction manager handles concurrent requests very differently from the original git way. Each
		// request runs on a snapshot repository and the results are collected in the form of packfiles. Those
		// packfiles contain resulting reachable and unreachable objects. As a result, we don't need to take
		// object expiry nor curft pack into account. This operation triggers a normal full repack without
		// cruft packing.
		// Afterward, packed unreachable objects are removed. During migration to transaction system, there
		// might be some loose unreachable objects. They will eventually be packed via either of the above tasks.
		if err := housekeeping.PerformRepack(ctx, workingRepository, repack.config,
			// Do a full repack. By using `-a` instead of `-A` we will immediately discard unreachable
			// objects instead of exploding them into loose objects.
			gitcmd.Flag{Name: "-a"},
			// Don't include objects part of alternate.
			gitcmd.Flag{Name: "-l"},
			// Delete loose objects made redundant by this repack and redundant packfiles.
			gitcmd.Flag{Name: "-d"},
		); err != nil {
			return err
		}
	}

	// Re-capture the list of packfiles and their baggages after repacking.
	afterFiles, err := mgr.collectPackFiles(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("collecting new packfiles: %w", err)
	}

	newMidxInode, err := wal.GetInode(midxPath)
	if err != nil {
		return fmt.Errorf("get midx inode after repacking: %w", err)
	}

	for file := range beforeFiles {
		// We delete the files only if it's missing from the before set.
		if _, exist := afterFiles[file]; !exist || (file == midxFileName && newMidxInode != oldMidxInode) {
			transaction.walEntry.RemoveDirectoryEntry(filepath.Join(
				objectsDirRelativePath, "pack", file,
			))
		}
	}

	for file := range afterFiles {
		// Similarly, we don't need to link existing packfiles.
		if _, exist := beforeFiles[file]; !exist || (file == midxFileName && newMidxInode != oldMidxInode) {
			fileRelativePath := filepath.Join(objectsDirRelativePath, "pack", file)

			if err := transaction.walEntry.CreateFile(
				filepath.Join(transaction.snapshot.Root(), fileRelativePath),
				fileRelativePath,
			); err != nil {
				return fmt.Errorf("record pack file creations: %q: %w", file, err)
			}
		}
	}

	if isFullRepack {
		timestampRelativePath := filepath.Join(transaction.relativePath, stats.FullRepackTimestampFilename)
		timestampAbsolutePath := filepath.Join(transaction.snapshot.Root(), timestampRelativePath)

		info, err := os.Stat(timestampAbsolutePath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat repack timestamp file: %w", err)
		}

		if err := stats.UpdateFullRepackTimestamp(filepath.Join(transaction.snapshot.Root(), transaction.relativePath), time.Now()); err != nil {
			return fmt.Errorf("updating repack timestamp: %w", err)
		}

		if info != nil {
			// The file existed and needs to be removed first.
			transaction.walEntry.RemoveDirectoryEntry(timestampRelativePath)
		}

		if err := transaction.walEntry.CreateFile(timestampAbsolutePath, timestampRelativePath); err != nil {
			return fmt.Errorf("stage repacking timestamp: %w", err)
		}
	}

	return nil
}

// prepareCommitGraphs updates the commit-graph in the snapshot repository. It then hard-links the
// graphs to the staging repository so it can be applied by the transaction manager.
func (mgr *TransactionManager) prepareCommitGraphs(ctx context.Context, transaction *Transaction) error {
	defer trace.StartRegion(ctx, "prepareCommitGraphs").End()

	if transaction.runHousekeeping.writeCommitGraphs == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareCommitGraphs", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("commit-graph", "prepare")
	defer finishTimer()

	// Check if the legacy commit-graph file exists. If so, remove it as we'd replace it with a
	// commit-graph chain.
	commitGraphRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graph")
	if info, err := os.Stat(filepath.Join(
		transaction.snapshot.Root(),
		commitGraphRelativePath,
	)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graph: %w", err)
	} else if info != nil {
		transaction.walEntry.RemoveDirectoryEntry(commitGraphRelativePath)
	}

	// Check for an existing commit-graphs directory. If so, delete it as
	// we log all commit graphs created.
	commitGraphsRelativePath := filepath.Join(transaction.relativePath, "objects", "info", "commit-graphs")
	commitGraphsAbsolutePath := filepath.Join(transaction.snapshot.Root(), commitGraphsRelativePath)
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs pre-image: %w", err)
	} else if info != nil {
		if err := storage.RecordDirectoryRemoval(transaction.FS(), transaction.FS().Root(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs removal: %w", err)
		}
	}

	if err := housekeeping.WriteCommitGraph(ctx,
		mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath)),
		transaction.runHousekeeping.writeCommitGraphs.config,
	); err != nil {
		return fmt.Errorf("re-writing commit graph: %w", err)
	}

	// If the directory exists after the operation, log all of the new state.
	if info, err := os.Stat(commitGraphsAbsolutePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat commit-graphs post-image: %w", err)
	} else if info != nil {
		if err := storage.RecordDirectoryCreation(transaction.FS(), commitGraphsRelativePath); err != nil {
			return fmt.Errorf("record commit-graphs creation: %w", err)
		}
	}

	return nil
}

// collectPackFiles collects the list of packfiles and their luggage files.
func (mgr *TransactionManager) collectPackFiles(ctx context.Context, repoPath string) (map[string]struct{}, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		return nil, fmt.Errorf("reading objects/pack dir: %w", err)
	}

	// Filter packfiles and relevant files.
	collectedFiles := make(map[string]struct{})
	for _, file := range files {
		// objects/pack directory should not include any sub-directory. We can simply ignore them.
		if file.IsDir() {
			continue
		}
		for extension := range packfileExtensions {
			if strings.HasSuffix(file.Name(), extension) {
				collectedFiles[file.Name()] = struct{}{}
			}
		}
	}

	return collectedFiles, nil
}

// verifyHousekeeping verifies if all included housekeeping tasks can be performed. Although it's feasible for multiple
// housekeeping tasks running at the same time, it's not guaranteed they are conflict-free. So, we need to ensure there
// is no other concurrent housekeeping task. Each sub-task also needs specific verification.
func (mgr *TransactionManager) verifyHousekeeping(ctx context.Context, transaction *Transaction, refBackend git.ReferenceBackend, zeroOID git.ObjectID) (*gitalypb.LogEntry_Housekeeping, error) {
	defer trace.StartRegion(ctx, "verifyHousekeeping").End()

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyHousekeeping", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "verify")
	defer finishTimer()

	// Check for any concurrent housekeeping between this transaction's snapshot LSN and the latest appended LSN.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		if entry.GetHousekeeping() != nil {
			return errHousekeepingConflictConcurrent
		}
		if entry.GetRepositoryDeletion() != nil {
			return errConflictRepositoryDeletion
		}

		// Applying a repacking operation prunes all loose objects on application. If loose objects were concurrently introduced
		// in the repository with the repacking operation, this could lead to corruption if we prune a loose object that is needed.
		// Transactions in general only introduce packs, not loose objects. The only exception to this currently is alternate
		// unlinking operations where the objects of the alternate are hard linked into the member repository. This can technically
		// still introduce loose objects into the repository and trigger this problem as the pools could still have loose objects
		// in them until the first repack.
		//
		// Check if the repository was unlinked from an alternate concurrently.
		for _, op := range entry.GetOperations() {
			switch op := op.GetOperation().(type) {
			case *gitalypb.LogEntry_Operation_RemoveDirectoryEntry_:
				if string(op.RemoveDirectoryEntry.GetPath()) == stats.AlternatesFilePath(transaction.relativePath) {
					return errConcurrentAlternateUnlink
				}
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	packRefsEntry, err := mgr.verifyPackRefs(ctx, transaction, refBackend, zeroOID)
	if err != nil {
		return nil, fmt.Errorf("verifying pack refs: %w", err)
	}

	if err := mgr.verifyRepacking(ctx, transaction); err != nil {
		return nil, fmt.Errorf("verifying repacking: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping{
		PackRefs: packRefsEntry,
	}, nil
}

// verifyPackRefs verifies if the git-pack-refs(1) can be applied without any conflicts.
// It calls the reference backend specific function to handle the core logic.
func (mgr *TransactionManager) verifyPackRefs(ctx context.Context, transaction *Transaction, refBackend git.ReferenceBackend, zeroOID git.ObjectID) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	if transaction.runHousekeeping.packRefs == nil {
		return nil, nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyPackRefs", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("pack-refs", "verify")
	defer finishTimer()

	if refBackend == git.ReferenceBackendReftables {
		packRefs, err := mgr.verifyPackRefsReftable(transaction)
		if err != nil {
			return nil, fmt.Errorf("reftable backend: %w", err)
		}
		return packRefs, nil
	}

	packRefs, err := mgr.verifyPackRefsFiles(ctx, transaction, zeroOID)
	if err != nil {
		return nil, fmt.Errorf("files backend: %w", err)
	}
	return packRefs, nil
}

// verifyRepacking checks the object repacking operations for conflicts.
//
// Object repacking without pruning is conflict-free operation. It only rearranges the objects on the disk into
// a more optimal physical format. All objects that other transactions could need are still present in pure repacking
// operations.
//
// Repacking operations that prune unreachable objects from the repository may lead to conflicts. Conflicts may occur
// if concurrent transactions depend on the unreachable objects.
//
// 1. Transactions may point references to the previously unreachable objects and make them reachable.
// 2. Transactions may write new objects that depend on the unreachable objects.
//
// In both cases a pruning operation that removes the objects must be aborted. In the first case, the pruning
// operation would remove reachable objects from the repository and the repository becomes corrupted. In the second case,
// the new objects written into the repository may not be necessarily reachable. Transactions depend on an invariant
// that all objects in the repository are valid. Therefore, we must also reject transactions that attempt to remove
// dependencies of unreachable objects even if such state isn't considered corrupted by Git.
//
// As we don't have a list of pruned objects at hand, the conflicts are identified by checking whether the recorded
// dependencies of a transaction would still exist in the repository after applying the pruning operation.
func (mgr *TransactionManager) verifyRepacking(ctx context.Context, transaction *Transaction) (returnedErr error) {
	repack := transaction.runHousekeeping.repack
	if repack == nil {
		return nil
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.verifyRepacking", nil)
	defer span.End()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "verify")
	defer finishTimer()

	// Other strategies re-organize packfiles without pruning unreachable objects. No need to run following
	// expensive verification.
	if repack.config.Strategy != housekeepingcfg.RepackObjectsStrategyFullWithCruft {
		return nil
	}

	// Setup a working repository of the destination repository and all changes of current transactions. All
	// concurrent changes must land in that repository already.
	stagingRepository, err := mgr.setupStagingRepository(ctx, transaction)
	if err != nil {
		return fmt.Errorf("setting up new snapshot for verifying repacking: %w", err)
	}

	// To verify the housekeeping transaction, we apply the operations it staged to a snapshot of the target
	// repository's current state. We then check whether the resulting state is valid.
	if err := func() error {
		dbTX := mgr.db.NewTransaction(true)
		defer dbTX.Discard()

		return applyOperations(
			ctx,
			// We're not committing the changes in to the snapshot, so no need to fsync anything.
			func(context.Context, string) error { return nil },
			transaction.stagingSnapshot.Root(),
			transaction.walEntry.Directory(),
			transaction.walEntry.Operations(),
			dbTX,
		)
	}(); err != nil {
		return fmt.Errorf("apply operations: %w", err)
	}

	// Collect object dependencies. All of them should exist in the resulting packfile or new concurrent
	// packfiles while repacking is running.
	objectDependencies := map[git.ObjectID]struct{}{}
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, txnObjectDependencies map[git.ObjectID]struct{}) error {
		for oid := range txnObjectDependencies {
			objectDependencies[oid] = struct{}{}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walking committed entries: %w", err)
	}

	if err := mgr.verifyObjectsExist(ctx, stagingRepository, objectDependencies); err != nil {
		var errInvalidObject localrepo.InvalidObjectError
		if errors.As(err, &errInvalidObject) {
			return errRepackConflictPrunedObject
		}

		return fmt.Errorf("verify objects exist: %w", err)
	}

	return nil
}

// verifyPackRefsReftable verifies if the compaction performed can be safely
// applied to the repository.
// We merge the tables.list generated by our compaction with the existing
// repositories tables.list. Because there could have been new tables after
// we performed compaction.
func (mgr *TransactionManager) verifyPackRefsReftable(transaction *Transaction) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	tables := transaction.runHousekeeping.packRefs.reftablesAfter
	if len(tables) < 1 {
		return nil, nil
	}

	// The tables.list from the target repository should be identical to that of the staging
	// repository before the compaction. However, concurrent writes might have occurred which
	// wrote new tables to the target repository. We shouldn't loose that data. So we merge
	// the compacted tables.list with the newer tables from the target repository's tables.list.
	repoPath := mgr.getAbsolutePath(transaction.relativePath)
	newTableList, err := reftable.ReadTablesList(repoPath)
	if err != nil {
		return nil, fmt.Errorf("reading tables.list: %w", err)
	}

	snapshotRepoPath := mgr.getAbsolutePath(transaction.snapshotRepository.GetRelativePath())

	// tables.list is hard-linked from the repository to the snapshot, we shouldn't
	// directly write to it as we'd modify the original. So let's remove the
	// hard-linked file.
	if err = os.Remove(filepath.Join(snapshotRepoPath, "reftable", "tables.list")); err != nil {
		return nil, fmt.Errorf("removing tables.list: %w", err)
	}

	// We need to merge the tables.list of snapshotRepo with the latest from stagingRepo.
	tablesBefore := transaction.runHousekeeping.packRefs.reftablesBefore
	finalTableList := append(tables, newTableList[len(tablesBefore):]...)

	finalTableListString := make([]string, len(finalTableList))
	for i, table := range finalTableList {
		finalTableListString[i] = table.String()
	}

	// Write the updated tables.list so we can add the required operations.
	finalTableListPath := filepath.Join(snapshotRepoPath, "reftable", "tables.list")
	if err := os.WriteFile(
		finalTableListPath,
		[]byte(strings.Join(finalTableListString, "\n")+"\n"),
		mode.File,
	); err != nil {
		return nil, fmt.Errorf("writing tables.list: %w", err)
	}

	if err := safe.NewSyncer().Sync(mgr.ctx, finalTableListPath); err != nil {
		return nil, fmt.Errorf("flush final table list: %w", err)
	}
	if err := safe.NewSyncer().SyncParent(mgr.ctx, finalTableListPath); err != nil {
		return nil, fmt.Errorf("flush final table list: %w", err)
	}

	tablesListRelativePath := filepath.Join(transaction.relativePath, "reftable", "tables.list")
	if err := transaction.FS().RecordRemoval(tablesListRelativePath); err != nil {
		return nil, fmt.Errorf("record old tables.list removal: %w", err)
	}

	// Add operation to update the tables.list.
	if err := transaction.FS().RecordFile(tablesListRelativePath); err != nil {
		return nil, fmt.Errorf("record new tables.list: %w", err)
	}

	return nil, nil
}

// verifyPackRefsFiles verifies if the pack-refs housekeeping task can be logged. Ideally, we can just apply the packed-refs
// file and prune the loose references. Unfortunately, there could be a ref modification between the time the pack-refs
// command runs and the time this transaction is logged. Thus, we need to verify if the transaction conflicts with the
// current state of the repository.
//
// There are three cases when a reference is modified:
// - Reference creation: this is the easiest case. The new reference exists as a loose reference on disk and shadows the
// one in the packed-ref.
// - Reference update: similarly, the loose reference shadows the one in packed-refs with the new OID. However, we need
// to remove it from the list of pruned references. Otherwise, the repository continues to use the old OID.
// - Reference deletion. When a reference is deleted, both loose reference and the entry in the packed-refs file are
// removed. The reflogs are also removed. In addition, we don't use reflogs in Gitaly as core.logAllRefUpdates defaults
// to false in bare repositories. It could of course be that an admin manually enabled it by modifying the config
// on-disk directly. There is no way to extract reference deletion between two states.
//
// In theory, if there is any reference deletion, it can be removed from the packed-refs file. However, it requires
// parsing and regenerating the packed-refs file. So, let's settle down with a conflict error at this point.
func (mgr *TransactionManager) verifyPackRefsFiles(ctx context.Context, transaction *Transaction, zeroOID git.ObjectID) (*gitalypb.LogEntry_Housekeeping_PackRefs, error) {
	packRefs := transaction.runHousekeeping.packRefs

	// Check for any concurrent ref deletion between this transaction's snapshot LSN to the end.
	if err := mgr.walkCommittedEntries(transaction, func(entry *gitalypb.LogEntry, objectDependencies map[git.ObjectID]struct{}) error {
		for _, refTransaction := range entry.GetReferenceTransactions() {
			for _, change := range refTransaction.GetChanges() {
				// We handle HEAD updates through the git-update-ref, but since
				// it is not part of the packed-refs file, we don't need to worry about it.
				if bytes.Equal(change.GetReferenceName(), []byte("HEAD")) {
					continue
				}

				if git.ObjectID(change.GetNewOid()) == zeroOID {
					// Oops, there is a reference deletion. Bail out.
					return errPackRefsConflictRefDeletion
				}
				// Ref update. Remove the updated ref from the list of pruned refs so that the
				// new OID in loose reference shadows the outdated OID in packed-refs.
				delete(packRefs.PrunedRefs, git.ReferenceName(change.GetReferenceName()))
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking committed entries: %w", err)
	}

	// Build a tree of the loose references and empty directories we need to prune.
	prunedPaths := reftree.New()
	for reference := range packRefs.PrunedRefs {
		if err := prunedPaths.InsertReference(reference.String()); err != nil {
			return nil, fmt.Errorf("insert reference: %w", err)
		}
	}
	for directory := range packRefs.emptyDirectories {
		if err := prunedPaths.InsertNode(directory, true, true); err != nil {
			return nil, fmt.Errorf("insert directory: %w", err)
		}
	}

	directoriesToKeep := map[string]struct{}{
		// Valid git directory needs to have a 'refs' directory, so we can't remove it.
		filepath.Join(transaction.relativePath, "refs"): {},
		// Git keeps these top-level directories. We keep them as well to reduce
		// conflicting operations on them.
		filepath.Join(transaction.relativePath, "refs", "heads"): {},
		filepath.Join(transaction.relativePath, "refs", "tags"):  {},
	}

	// Walk down the deleted references from the leaves towards the root. We'll log the deletion
	// of loose references as we walk towards the root, and the removal of directories along the
	// path that became empty as a result of removing the references. As we're operating on the real
	// repository here, we can't actually perform deletions in it. We instead keep track of the
	// files and directories we've deleted in-memory to ensure we only delete empty directories.
	deletedPaths := make(map[string]struct{}, len(packRefs.PrunedRefs)+len(packRefs.emptyDirectories))
	if err := prunedPaths.WalkPostOrder(func(path string, isDir bool) error {
		relativePath := filepath.Join(transaction.relativePath, path)
		if _, ok := directoriesToKeep[relativePath]; ok {
			return nil
		}

		if isDir {
			// If this is a directory, we need to ensure it is actually empty before removing
			// it. Check if we find any directory entries we haven't yet deleted.
			entries, err := os.ReadDir(mgr.getAbsolutePath(relativePath))
			if err != nil {
				return fmt.Errorf("read dir: %w", err)
			}

			for _, entry := range entries {
				if _, ok := deletedPaths[filepath.Join(relativePath, entry.Name())]; ok {
					// This path was already deleted. Don't consider it to exist.
					continue
				}

				// This directory was not empty because someone concurrently wrote
				// a reference into it. Keep it in place.
				directoriesToKeep[relativePath] = struct{}{}
				return nil
			}
		}

		deletedPaths[relativePath] = struct{}{}
		transaction.walEntry.RemoveDirectoryEntry(relativePath)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk post order: %w", err)
	}

	return &gitalypb.LogEntry_Housekeeping_PackRefs{}, nil
}

// prepareOffloading implements the core offloading logic within the transaction manager.
// It must be called inside a snapshot repository and performs these steps:
//
//   - Repacks the repository based on the provided filter. After repacking,
//     the repository's objects are split into two groups: one group for uploading to the
//     offloading storage and another to be kept locally.
//
//   - Uploads objects marked for offloading.
//
//   - Updates configurations (git config file, alternates file).
//
//   - Records all file changes in the WAL.
func (mgr *TransactionManager) prepareOffloading(ctx context.Context, transaction *Transaction) (returnedErr error) {
	if transaction.runHousekeeping.runOffloading == nil {
		return nil
	}
	if mgr.offloadingSink == nil {
		return fmt.Errorf("offloading sink is not configured")
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareOffloading", nil)
	defer span.End()

	// Loading configurations for offloading
	cfg := transaction.runHousekeeping.runOffloading.config

	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	// workingRepoPath is the current repository path which we are performing operations on.
	// In the context of transaction, workingRepoPath is a snapshot repository.
	workingRepoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())
	// Find the original repository's absolute path. In the context of transaction, originalRepo is the repo
	// which we are taking a snapshot of.
	originalRepo := &gitalypb.Repository{
		StorageName:  workingRepository.GetStorageName(),
		RelativePath: workingRepository.GetRelativePath(),
	}
	originalRepo = transaction.OriginalRepository(originalRepo)
	// originalRepoAbsPath := mgr.getAbsolutePath(originalRepo.GetRelativePath())

	// cfg.Prefix should be empty in production, which triggers automatic UUID generation.
	// Non-empty prefix values are only used in test environments.
	if cfg.Prefix == "" {
		offloadingID := uuid.New().String()
		// When uploading to offloading storage, use [original repo's relative path + UUID] as prefix
		cfg.Prefix = filepath.Join(originalRepo.GetRelativePath(), offloadingID)
	}

	// Capture the list of pack-files before repacking.
	oldPackFiles, err := mgr.collectPackFiles(ctx, workingRepoPath)
	if err != nil {
		return fmt.Errorf("collecting existing packfiles: %w", err)
	}

	// Creating a temporary directory where we will put the pack files (the ones to be offloaded)
	filterToBase, err := os.MkdirTemp(workingRepoPath, "gitaly-offloading-*")
	if err != nil {
		return fmt.Errorf("create directory %s: %w", filterToBase, err)
	}
	filterToDir := filepath.Join(filterToBase, objectsDir, packFileDir)
	if err := os.MkdirAll(filterToDir, mode.Directory); err != nil {
		return fmt.Errorf("create directory %s: %w", filterToDir, err)
	}

	// Repack the repository. Current offloading implementation only offloads blobs, so we hard code filter here.
	// This can be relaxed to a parameter if more offloading type is supported
	repackingFilter := "blob:none"
	if err := housekeeping.PerformRepackingForOffloading(ctx, workingRepository, repackingFilter, filterToDir); err != nil {
		return errors.Join(errOffloadingOnRepacking, err)
	}
	packFilesToUpload, err := mgr.collectPackFiles(ctx, filterToBase)
	if err != nil {
		return fmt.Errorf("collect pack files to upload: %w", err)
	}
	if len(packFilesToUpload) == 0 {
		return fmt.Errorf("no pack files to upload")
	}
	newPackFilesToStay, err := mgr.collectPackFiles(ctx, workingRepoPath)
	if err != nil {
		return fmt.Errorf("collect new pack files: %w", err)
	}
	if slices.Equal(maps.Keys(oldPackFiles), maps.Keys(newPackFilesToStay)) {
		return fmt.Errorf("same packs after offloading repacking")
	}

	uploadedPackFiles := make([]string, 0, len(packFilesToUpload))
	defer func() {
		// If returnedErr is non-nil, attempt to remove the uploaded file.
		// This is a best-effort cleanup; we can't guarantee successful deletion.
		// If there is an error, the error is returned to the caller together with the returnedErr.
		// Any undeleted files will eventually be removed by a garbage collection job.
		if returnedErr != nil {
			deletionErrors := mgr.offloadingSink.DeleteObjects(ctx, cfg.Prefix, uploadedPackFiles)
			for _, err := range deletionErrors {
				if gcerrors.Code(err) != gcerrors.NotFound {
					returnedErr = errors.Join(returnedErr, err)
				}
			}
		}
	}()

	// Prepare metadata for offloading
	metadataMap := map[string]string{
		"storage-name":  originalRepo.GetStorageName(),
		"storage-path":  mgr.storagePath,
		"relative-path": originalRepo.GetRelativePath(),
		"partition-id":  mgr.partitionID.String(),
	}

	for file := range packFilesToUpload {
		if err := mgr.offloadingSink.Upload(ctx, filepath.Join(filterToDir, file), cfg.Prefix, metadataMap); err != nil {
			return errors.Join(errOffloadingObjectUpload, err)
		}
		uploadedPackFiles = append(uploadedPackFiles, file)
	}

	// Update git config file.
	promisorRemoteURL, err := url.JoinPath(cfg.SinkBaseURL, cfg.Prefix)
	if err != nil {
		return fmt.Errorf("constructing promisor remote URL: %w", err)
	}
	if err := housekeeping.SetOffloadingGitConfig(ctx, workingRepository, promisorRemoteURL, repackingFilter, nil); err != nil {
		return fmt.Errorf("set offloading git config: %w", err)
	}

	// Record WAL entry
	for file := range oldPackFiles {
		transaction.walEntry.RemoveDirectoryEntry(filepath.Join(transaction.relativePath, objectsDir, packFileDir, file))
	}

	for file := range newPackFilesToStay {
		fileRelativePath := filepath.Join(transaction.relativePath, objectsDir, packFileDir, file)
		if err := transaction.walEntry.CreateFile(
			filepath.Join(transaction.snapshot.Root(), fileRelativePath),
			fileRelativePath,
		); err != nil {
			return fmt.Errorf("record pack-file creation: %w", err)
		}
	}

	transaction.walEntry.RemoveDirectoryEntry(filepath.Join(transaction.relativePath, configFile))
	if err := transaction.walEntry.CreateFile(
		filepath.Join(workingRepoPath, configFile),
		filepath.Join(transaction.relativePath, configFile),
	); err != nil {
		return fmt.Errorf("record config file replacement: %w", err)
	}

	return nil
}

// prepareRehydrating restores an offloaded repository to a fully local state
// within the transaction manager. It must be called from within a snapshot repository.
//
// This function performs the following steps:
//   - Downloads packfiles from offloading storage using the provided prefix.
//   - Updates Git configuration (e.g., removes the offloading remote).
//   - Records all file changes in the write-ahead log (WAL).
//
// The caller is responsible for checking whether the repository is offloaded
// and requires rehydration before invoking this function.
func (mgr *TransactionManager) prepareRehydrating(ctx context.Context, transaction *Transaction) (returnedErr error) {
	if transaction.runHousekeeping.runRehydrating == nil {
		return nil
	}
	if mgr.offloadingSink == nil {
		return fmt.Errorf("offloading sink is not configured")
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "transaction.prepareRehydrating", nil)
	defer span.End()

	workingRepository := mgr.repositoryFactory.Build(transaction.snapshot.RelativePath(transaction.relativePath))
	// workingRepoPath is the current repository path which we are performing operations on.
	// In the context of transaction, workingRepoPath is a snapshot repository.
	workingRepoPath := mgr.getAbsolutePath(workingRepository.GetRelativePath())

	prefix := transaction.runHousekeeping.runRehydrating.prefix
	packFilesToDownload, err := mgr.offloadingSink.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list pack files to download: %w", err)
	}

	// Download packfiles.
	// Note: We intentionally do not use a defer function to clean up files on the bucket.
	// Even if the download succeeds, the commit may still fail or be aborted later (e.g., due to conflicts).
	// Cleanup should only occur after the WAL has been successfully applied.
	downloadedPackFiles := make([]string, 0, len(packFilesToDownload))
	for _, file := range packFilesToDownload {
		if err := mgr.offloadingSink.Download(ctx, filepath.Join(prefix, file), filepath.Join(workingRepoPath, objectsDir, packFileDir, file)); err != nil {
			return errors.Join(errOffloadingObjectDownload, err)
		}
		downloadedPackFiles = append(downloadedPackFiles, file)
	}

	// Reset config
	if err := housekeeping.ResetOffloadingGitConfig(ctx, workingRepository, nil); err != nil {
		return fmt.Errorf("reset offloading git config: %w", err)
	}

	// Apply config file and packfiles to WAL
	for _, file := range downloadedPackFiles {
		fileRelativePath := filepath.Join(transaction.relativePath, objectsDir, packFileDir, file)
		if err := transaction.walEntry.CreateFile(
			filepath.Join(transaction.snapshot.Root(), fileRelativePath),
			fileRelativePath,
		); err != nil {
			return fmt.Errorf("record pack-file creation: %w", err)
		}
	}

	transaction.walEntry.RemoveDirectoryEntry(filepath.Join(transaction.relativePath, configFile))
	if err := transaction.walEntry.CreateFile(
		filepath.Join(workingRepoPath, configFile),
		filepath.Join(transaction.relativePath, configFile),
	); err != nil {
		return fmt.Errorf("record config file replacement: %w", err)
	}

	return returnedErr
}
