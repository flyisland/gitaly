package partition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/packfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/raftmgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	logrus "gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/offloading"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// RepositoryState describes the full asserted state of a repository.
type RepositoryState struct {
	// DefaultBranch is the expected refname that HEAD points to.
	DefaultBranch git.ReferenceName
	// CustomHooks is the expected state of the custom hooks.
	CustomHooks testhelper.DirectoryState
	// Objects are the objects that are expected to exist.
	Objects []git.ObjectID
	// Alternate is the content of 'objects/info/alternates'.
	Alternate string
	// References is the references that should exist, including ones in the packed-refs file and loose references.
	References *ReferencesState
	// Packfiles provides a more detailed object assertion in a repository. It allows us to examine the list of
	// packfiles, loose objects, invisible objects, index files, etc. Most test cases don't need to be concerned
	// with Packfile structure, though. So, we keep both, but Packfiles takes higher precedence.
	Packfiles *PackfilesState
	// FullRepackTimestamp tells if the repository has full-repack timestamp file.
	FullRepackTimestamp *FullRepackTimestamp
}

// FilesBackendState contains the reference state for the files backend. There is
// separation for loose refs and packed refs so we can assert them individually.
type FilesBackendState struct {
	// PackedReferences is the content of pack-refs file, line by line
	PackedReferences map[git.ReferenceName]git.ObjectID
	// LooseReferences is the exact list of loose references outside packed-refs.
	LooseReferences map[git.ReferenceName]git.ObjectID
}

// ReftableTable denotes a single table in the reftable backend.
type ReftableTable struct {
	// MinIndex refers to the minimum index of all reftables.
	MinIndex uint64
	// MaxIndex refers to the maximum index of all reftables.
	MaxIndex uint64
	// References is the complete list of references.
	References []git.Reference
	// Locked determines whether the table should have a lock file as well.
	Locked bool
}

// ReftableBackendState holds the reference state for the reftable backend. Since
// there are no different types of references, we contain a list of all references.
type ReftableBackendState struct {
	// Tables denotes the list of reftable tables.
	Tables []ReftableTable
}

// ReferencesState describes the asserted state of packed-refs and loose references. It's mostly used for verifying
// pack-refs housekeeping task.
type ReferencesState struct {
	// FilesBackend contains the state for the files backend.
	FilesBackend *FilesBackendState
	// ReftableBackend contains the state for the reftable backend.
	ReftableBackend *ReftableBackendState
}

// PackfilesState describes the asserted state of all packfiles and common multi-pack-index.
type PackfilesState struct {
	// LooseObjects contain the list of objects outside packfiles. Although the transaction manager cleans them up
	// eventually, loose objects should be handled during migration.
	LooseObjects []git.ObjectID
	// Packfiles is the hash map of name and corresponding packfile state assertion.
	Packfiles []*PackfileState
	// PooledObjects contain the list of pooled objects member repositories can see from the pool.
	PooledObjects []git.ObjectID
	// HasMultiPackIndex asserts whether there is a multi-pack-index inside the objects/pack directory.
	HasMultiPackIndex bool
	// CommitGraphs assert commit graphs of the repository.
	CommitGraphs *stats.CommitGraphInfo
}

// PackfileState describes the asserted state of an individual packfile, including its contained objects, index, bitmap.
type PackfileState struct {
	// Objects asserts the list of objects this packfile contains.
	Objects []git.ObjectID
	// HasBitmap asserts whether the packfile includes a corresponding bitmap index (*.bitmap).
	HasBitmap bool
	// HasReverseIndex asserts whether the packfile includes a corresponding reverse index (*.rev).
	HasReverseIndex bool
}

// FullRepackTimestamp asserts the state of the full repack timestamp file (.gitaly-full-repack-timestamp). It's not
// easy to assert the actual point of time now. Hence, we can only assert its existence.
type FullRepackTimestamp struct {
	// Exists
	Exists bool
}

// sortObjects returns a new slice with objects sorted lexically by their oid.
func sortObjects(objects []git.ObjectID) []git.ObjectID {
	sortedObjects := make([]git.ObjectID, len(objects))
	copy(sortedObjects, objects)

	sort.Slice(sortedObjects, func(i, j int) bool {
		return sortedObjects[i] < sortedObjects[j]
	})

	return sortedObjects
}

// sortPackfiles sorts the list of packfiles by their contained objects. Each packfile has a static hash. This hash is
// different between object formats and subject to change in the future. So, to make the test deterministic, we sort
// packfile states by their contained objects. This function is called before asserting packfile structures. It should
// be fine as soon as the set of objects the object distribution in packfiles are the same. Of course, we'll need to
// verify midx, which depends on packfile names, in another assertion.
func sortPackfilesState(packfilesState *PackfilesState) {
	if packfilesState == nil || len(packfilesState.Packfiles) <= 1 {
		return
	}
	// The sort key is re-compute in each sort loop. For testing purpose, which involves a handufl of objects and
	// packfiles, it's still efficient enough.
	concatObjects := func(oids []git.ObjectID) string {
		var result strings.Builder
		for _, oid := range oids {
			result.Write([]byte(oid))
		}
		return result.String()
	}
	sort.Slice(packfilesState.Packfiles, func(i, j int) bool {
		return concatObjects(packfilesState.Packfiles[i].Objects) < concatObjects(packfilesState.Packfiles[j].Objects)
	})
}

// isDirEmpty checks if a directory is empty.
func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Read at most one entry from the directory. If we get EOF, the directory is empty
	if _, err = f.Readdirnames(1); errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}

// RequireRepositoryState asserts the given repository matches the expected state.
func RequireRepositoryState(tb testing.TB, ctx context.Context, cfg config.Cfg, repo *localrepo.Repo, expected RepositoryState) {
	tb.Helper()

	repoPath, err := repo.Path(ctx)
	require.NoError(tb, err)

	refBackend, err := repo.ReferenceBackend(ctx)
	require.NoError(tb, err)

	headReference, err := repo.HeadReference(ctx)
	require.NoError(tb, err)

	gitReferences, err := repo.GetReferences(ctx)
	require.NoError(tb, err)
	actualGitReferences := map[git.ReferenceName]git.ObjectID{}
	for _, ref := range gitReferences {
		actualGitReferences[ref.Name] = git.ObjectID(ref.Target)
	}

	var actualReferencesState *ReferencesState

	if refBackend == git.ReferenceBackendFiles {
		actualReferencesState, err = collectFilesReferencesState(tb, expected, repoPath)
		require.NoError(tb, err)

		// Verify if the combination of packed-refs and loose refs match the point of view of Git.
		referencesFromFile := map[git.ReferenceName]git.ObjectID{}
		if actualReferencesState != nil {
			for name, oid := range actualReferencesState.FilesBackend.PackedReferences {
				referencesFromFile[name] = oid
			}

			for name, oid := range actualReferencesState.FilesBackend.LooseReferences {
				referencesFromFile[name] = oid
			}
		}
		require.Equalf(tb, referencesFromFile, actualGitReferences, "references perceived by Git don't match the ones in loose reference and packed-refs file")

		// Assert if there is any empty directory in the refs hierarchy excepts for heads and tags
		rootRefsDir := filepath.Join(repoPath, "refs")
		ignoredDirs := map[string]struct{}{
			rootRefsDir:                         {},
			filepath.Join(rootRefsDir, "heads"): {},
			filepath.Join(rootRefsDir, "tags"):  {},
		}
		require.NoError(tb, filepath.WalkDir(rootRefsDir, func(path string, entry fs.DirEntry, err error) error {
			if entry.IsDir() {
				if _, exist := ignoredDirs[path]; !exist {
					isEmpty, err := isDirEmpty(path)
					require.NoError(tb, err)
					require.Falsef(tb, isEmpty, "there shouldn't be any empty directory in the refs hierarchy %s", path)
				}
			}
			return nil
		}))
	} else if refBackend == git.ReferenceBackendReftables {
		actualReferencesState, err = collectReftableReferencesState(tb, expected, repoPath, headReference, actualGitReferences)
		require.NoError(tb, err)
	} else {
		tb.Errorf("unknown reference backend: %s", refBackend)
	}

	var (
		// expectedObjects are the objects that are expected to be seen by Git.
		expectedObjects = expected.Objects
		// expectedPackfiles is set in tests that assert that the object structure
		// on the disk specifically matches what is expected. This is exclusive to
		// the above expectedObjects which just asserts that certain objects exist
		// in the repository.
		actualPackfiles   *PackfilesState
		expectedPackfiles = expected.Packfiles
	)

	// If Packfiles assertion exists, we collect all objects from packfiles and loose objects. This list will be
	// compared with the actual list of objects gathered by `git-cat-file(1)`. This is to ensure on-disk structure
	// matches Git's perspective.
	if expectedPackfiles != nil {
		// Some of the pack files may contain duplicated objects.
		// When we're asserting object existence in the repository,
		// ignore the duplicated objects in the packfiles and just
		// ensure they are present.
		deduplicatedObjectIDs := map[git.ObjectID]struct{}{}
		for _, oids := range [][]git.ObjectID{
			expectedPackfiles.LooseObjects,
			expectedPackfiles.PooledObjects,
		} {
			for _, oid := range oids {
				deduplicatedObjectIDs[oid] = struct{}{}
			}
		}

		expectedPackfiles.LooseObjects = sortObjects(expectedPackfiles.LooseObjects)

		// The pooled objects are added to the general object existence assertion. If
		// the pooled objects are missing, ListObjects below won't return them, and we'll
		// ll see a failure as expected objects are missing.
		//
		// We thus do not have to separately assert which objects come from pools here.
		expectedPackfiles.PooledObjects = nil

		for _, packfile := range expected.Packfiles.Packfiles {
			packfile.Objects = sortObjects(packfile.Objects)
			for _, oid := range packfile.Objects {
				deduplicatedObjectIDs[oid] = struct{}{}
			}
		}

		expectedObjects = make([]git.ObjectID, 0, len(deduplicatedObjectIDs))
		for oid := range deduplicatedObjectIDs {
			expectedObjects = append(expectedObjects, oid)
		}

		objectHash, err := repo.ObjectHash(ctx)
		require.NoError(tb, err)

		actualPackfiles = collectPackfilesState(tb, repoPath, cfg, objectHash, expected.Packfiles)
		actualPackfiles.LooseObjects = sortObjects(actualPackfiles.LooseObjects)
		for _, packfile := range actualPackfiles.Packfiles {
			packfile.Objects = sortObjects(packfile.Objects)
		}
	}

	actualObjects := sortObjects(gittest.ListObjects(tb, cfg, repoPath))
	expectedObjects = sortObjects(expectedObjects)
	if expectedObjects == nil {
		// Normalize no objects to an empty slice. This way the equality check keeps working
		// without having to explicitly assert empty slice in the tests.
		expectedObjects = []git.ObjectID{}
	}

	alternate, err := os.ReadFile(stats.AlternatesFilePath(repoPath))
	if err != nil {
		require.ErrorIs(tb, err, fs.ErrNotExist)
	}

	sortPackfilesState(expectedPackfiles)
	sortPackfilesState(actualPackfiles)

	var actualFullRepackTimestamp *FullRepackTimestamp
	if expected.FullRepackTimestamp != nil {
		repackTimestamp, err := stats.FullRepackTimestamp(repoPath)
		require.NoError(tb, err)
		actualFullRepackTimestamp = &FullRepackTimestamp{Exists: !repackTimestamp.IsZero()}
	}

	require.Equal(tb,
		RepositoryState{
			DefaultBranch:       expected.DefaultBranch,
			Objects:             expectedObjects,
			Alternate:           expected.Alternate,
			References:          expected.References,
			Packfiles:           expectedPackfiles,
			FullRepackTimestamp: expected.FullRepackTimestamp,
		},
		RepositoryState{
			DefaultBranch:       headReference,
			Objects:             actualObjects,
			Alternate:           string(alternate),
			References:          actualReferencesState,
			Packfiles:           actualPackfiles,
			FullRepackTimestamp: actualFullRepackTimestamp,
		},
	)
	testhelper.RequireDirectoryState(tb, filepath.Join(repoPath, repoutil.CustomHooksDir), "", expected.CustomHooks)
}

func collectReftableReferencesState(
	tb testing.TB,
	expected RepositoryState,
	repoPath string,
	headReference git.ReferenceName,
	expectedReferences map[git.ReferenceName]git.ObjectID,
) (*ReferencesState, error) {
	if expected.References == nil {
		return nil, nil
	}

	state := &ReferencesState{
		ReftableBackend: &ReftableBackendState{
			Tables: []ReftableTable{},
		},
	}

	tableNames, err := reftable.ReadTablesList(repoPath)
	require.NoError(tb, err)

	expectedFiles := map[string]struct{}{"tables.list": {}}
	for _, tableName := range tableNames {
		expectedFiles[tableName] = struct{}{}
	}

	actualFiles := map[string]struct{}{}
	dirEntries, err := os.ReadDir(filepath.Join(repoPath, "reftable"))
	require.NoError(tb, err)

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), ".lock") {
			// Begin writes always a lock file for write transactions for the latest table
			// in the repository to prevent auto-compaction from touching the existing tables.
			// This would lead to conflicts as multiple transactions could auto-compact the
			// tables in their own snapshots.
			//
			// Skip the lock files as they are not present in the tables.list file, and they
			// are asserted individually for each table.
			continue
		}

		actualFiles[entry.Name()] = struct{}{}
	}
	require.Equal(tb, expectedFiles, actualFiles, "reftables directory contained unexpected files not present in tables.list")

	actualReferences := make(map[git.ReferenceName]git.ObjectID)

	for _, tableName := range tableNames {
		table := ReftableTable{}

		tablePath := filepath.Join(repoPath, "reftable", tableName)

		data, err := os.ReadFile(tablePath)
		require.NoError(tb, err)

		name, err := reftable.ParseName(tableName)
		require.NoError(tb, err)
		table.MinIndex, table.MaxIndex = name.MinUpdateIndex, name.MaxUpdateIndex

		reftable, err := reftable.NewReftable(data)
		require.NoError(tb, err)

		table.References, err = reftable.IterateRefs()
		require.NoError(tb, err)

		for _, reference := range table.References {
			actualReferences[reference.Name] = git.ObjectID(reference.Target)
		}

		// Check whether the table file had an associated lock. Begin creates
		// a lock for the latest table file for write transactions.
		table.Locked = true
		if _, err := os.Stat(tablePath + ".lock"); err != nil {
			require.ErrorIs(tb, err, os.ErrNotExist)
			table.Locked = false
		}

		state.ReftableBackend.Tables = append(state.ReftableBackend.Tables, table)
	}

	// We need to remove the tombstone refs so we can do a comparison.
	for reference, target := range actualReferences {
		if target == gittest.DefaultObjectHash.ZeroOID {
			delete(actualReferences, reference)
		}
	}

	// Append the head reference to actualGitReferences so we can compare the two maps
	expectedReferences["HEAD"] = git.ObjectID(headReference)
	require.Equal(tb, expectedReferences, actualReferences)

	return state, nil
}

func collectFilesReferencesState(tb testing.TB, expected RepositoryState, repoPath string) (*ReferencesState, error) {
	if expected.References == nil {
		return nil, nil
	}

	packRefsFile, err := os.ReadFile(filepath.Join(repoPath, "packed-refs"))
	if errors.Is(err, os.ErrNotExist) {
		// Treat missing packed-refs file as empty.
		packRefsFile = nil
	} else {
		require.NoError(tb, err)
	}

	// Parse packed-refs file
	var packedReferences map[git.ReferenceName]git.ObjectID
	if len(packRefsFile) != 0 {
		packedReferences = make(map[git.ReferenceName]git.ObjectID)
		lines := strings.Split(string(packRefsFile), "\n")
		require.Equalf(tb, strings.TrimSpace(lines[0]), "# pack-refs with: peeled fully-peeled sorted", "invalid packed-refs header")
		lines = lines[1:]
		for _, line := range lines {
			line = strings.TrimSpace(line)

			if len(line) == 0 {
				continue
			}

			// We don't care about the peeled object ID.
			if strings.HasPrefix(line, "^") {
				continue
			}

			parts := strings.Split(line, " ")
			require.Equalf(tb, 2, len(parts), "invalid packed-refs format: %q", line)
			packedReferences[git.ReferenceName(parts[1])] = git.ObjectID(parts[0])
		}
	}

	// Walk and collect loose refs.
	looseReferences := map[git.ReferenceName]git.ObjectID{}
	refsPath := filepath.Join(repoPath, "refs")
	require.NoError(tb, filepath.WalkDir(refsPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			ref, err := filepath.Rel(repoPath, path)
			if err != nil {
				return fmt.Errorf("extracting ref name: %w", err)
			}
			oid, err := os.ReadFile(path)
			require.NoError(tb, err)

			looseReferences[git.ReferenceName(ref)] = git.ObjectID(strings.TrimSpace(string(oid)))
		}
		return nil
	}))

	return &ReferencesState{
		FilesBackend: &FilesBackendState{
			PackedReferences: packedReferences,
			LooseReferences:  looseReferences,
		},
	}, nil
}

func collectPackfilesState(tb testing.TB, repoPath string, cfg config.Cfg, objectHash git.ObjectHash, expected *PackfilesState) *PackfilesState {
	state := &PackfilesState{}

	objectsDir := filepath.Join(repoPath, "objects")

	looseObjects, err := filepath.Glob(filepath.Join(objectsDir, "??/*"))
	require.NoError(tb, err)
	for _, obj := range looseObjects {
		relPath, err := filepath.Rel(objectsDir, obj)
		require.NoError(tb, err)

		parts := strings.Split(relPath, "/")
		require.Equalf(tb, 2, len(parts), "invalid loose object path %q", relPath)

		state.LooseObjects = append(state.LooseObjects, git.ObjectID(fmt.Sprintf("%s%s", parts[0], parts[1])))
	}

	packDir := filepath.Join(objectsDir, "pack")

	entries, err := os.ReadDir(filepath.Join(packDir))
	require.NoError(tb, err)

	state.Packfiles = []*PackfileState{}
	for _, file := range entries {
		indexPath := filepath.Join(objectsDir, "pack", file.Name())
		ext := filepath.Ext(file.Name())
		if ext != ".idx" {
			continue
		}
		packName := strings.TrimSuffix(file.Name(), ext)
		packfileState := &PackfileState{}

		// Run git-verify-pack(1) to ensure the packfil is well functioning.
		gittest.Exec(tb, cfg, "-C", repoPath, "verify-pack", indexPath)

		// Parse index to collect list of objects.
		idxContent, err := os.ReadFile(indexPath)
		require.NoError(tb, err)

		var stdout bytes.Buffer
		gittest.ExecOpts(tb, cfg, gittest.ExecConfig{
			Stdin:  strings.NewReader(string(idxContent)),
			Stdout: &stdout,
		}, "show-index", "--object-format="+objectHash.Format)
		require.NoError(tb, packfile.ParseIndex(&stdout, func(obj *packfile.Object) {
			packfileState.Objects = append(packfileState.Objects, git.ObjectID(obj.OID))
		}))

		// Check other index files exist.
		if _, err := os.Stat(filepath.Join(packDir, packName+".bitmap")); err == nil {
			packfileState.HasBitmap = true
		} else if !errors.Is(err, os.ErrNotExist) {
			require.NoError(tb, err)
		}

		if _, err := os.Stat(filepath.Join(packDir, packName+".rev")); err == nil {
			packfileState.HasReverseIndex = true
		} else if !errors.Is(err, os.ErrNotExist) {
			require.NoError(tb, err)
		}
		state.Packfiles = append(state.Packfiles, packfileState)
	}
	midxPath := filepath.Join(packDir, "multi-pack-index")
	if _, err := os.Stat(midxPath); err == nil {
		state.HasMultiPackIndex = true
		_, err := stats.MultiPackIndexInfoForPath(midxPath)
		require.NoError(tb, err)

		gittest.Exec(tb, cfg, "-C", repoPath, "multi-pack-index", "verify")
	} else if !errors.Is(err, os.ErrNotExist) {
		require.NoError(tb, err)
	}

	if expected.CommitGraphs != nil {
		gittest.Exec(tb, cfg, "-C", repoPath, "commit-graph", "verify")

		info, err := stats.CommitGraphInfoForRepository(repoPath)
		require.NoError(tb, err)
		state.CommitGraphs = &info
	}

	return state
}

type repositoryBuilder func(relativePath string) *localrepo.Repo

// RepositoryStates describes the state of repositories in a storage. The key is the relative path of a repository that
// is expected to exist and the value the expected state contained in the repository.
type RepositoryStates map[string]RepositoryState

// RequireRepositories finds all Git repositories in the given storage path. It asserts the set of existing repositories match the expected set
// and that all of the repositories contain the expected state.
func RequireRepositories(tb testing.TB, ctx context.Context, cfg config.Cfg, storagePath string, buildRepository repositoryBuilder, expected RepositoryStates) {
	tb.Helper()

	var actualRelativePaths []string
	require.NoError(tb, filepath.WalkDir(storagePath, func(path string, d fs.DirEntry, err error) error {
		require.NoError(tb, err)

		if !d.IsDir() {
			return nil
		}

		if err := storage.ValidateGitDirectory(path); err != nil {
			require.ErrorAs(tb, err, &storage.InvalidGitDirectoryError{})
			return nil
		}

		relativePath, err := filepath.Rel(storagePath, path)
		require.NoError(tb, err)

		actualRelativePaths = append(actualRelativePaths, relativePath)
		return nil
	}))

	var expectedRelativePaths []string
	for relativePath := range expected {
		expectedRelativePaths = append(expectedRelativePaths, relativePath)
	}

	// Sort the slices instead of using ElementsMatch so the state assertions are always done in the
	// same order as well.
	sort.Strings(actualRelativePaths)
	sort.Strings(expectedRelativePaths)

	require.Equal(tb, expectedRelativePaths, actualRelativePaths,
		"expected and actual set of repositories in the storage don't match")

	for _, relativePath := range expectedRelativePaths {
		func() {
			defer func() {
				// RequireRepositoryState works within a repository and doesn't thus print out the
				// relative path of the repository that failed. If the call failed the test,
				// print out the relative path to ease troubleshooting.
				if tb.Failed() {
					require.Failf(tb, "unexpected repository state", "relative path: %q", relativePath)
				}
			}()

			RequireRepositoryState(tb, ctx, cfg, buildRepository(relativePath), expected[relativePath])
		}()
	}
}

// DatabaseState describes the expected state of the key-value store. The keys in the map are the expected keys
// in the database and the values are the expected unmarshaled values.
type DatabaseState map[string]any

// RequireDatabase asserts the actual database state matches the expected database state. The actual values in the
// database are unmarshaled to the same type the values have in the expected database state.
func RequireDatabase(tb testing.TB, ctx context.Context, database keyvalue.Transactioner, expectedState DatabaseState) {
	tb.Helper()

	if expectedState == nil {
		expectedState = DatabaseState{}
	}

	actualState := DatabaseState{}
	unexpectedKeys := []string{}
	require.NoError(tb, database.View(func(txn keyvalue.ReadWriter) error {
		iterator := txn.NewIterator(keyvalue.IteratorOptions{})
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			key := iterator.Item().Key()
			expectedValue, ok := expectedState[string(key)]
			if !ok {
				// Print the keys out escaped as otherwise the non-printing characters are not visible in the assertion failure.
				unexpectedKeys = append(unexpectedKeys, fmt.Sprintf("%q", key))
				continue
			}

			value, err := iterator.Item().ValueCopy(nil)
			require.NoError(tb, err)

			if _, isProto := expectedValue.(proto.Message); !isProto {
				// We convert the raw bytes values to strings for easier test case building without
				// having to convert all literals to bytes.
				actualState[string(key)] = string(value)
				continue
			}

			if _, isAny := expectedValue.(*anypb.Any); isAny {
				// Verify if the database contains the key, but the content does not matter.
				actualState[string(key)] = expectedValue
				continue
			}

			// Unmarshal the actual value to the same type as the expected value.
			actualValue := proto.Clone(expectedValue.(proto.Message))
			proto.Reset(actualValue)

			require.NoErrorf(tb, proto.Unmarshal(value, actualValue), "unmarshalling value in db: %q", key)

			actualState[string(key)] = actualValue
		}

		return nil
	}))

	require.Emptyf(tb, unexpectedKeys, "database contains unexpected keys")

	// Ignore optional keys in expectedState but not in actualState
	for k, v := range expectedState {
		if _, isAny := v.(*anypb.Any); isAny {
			if _, exist := actualState[k]; !exist {
				delete(expectedState, k)
			}
		}
	}

	testhelper.ProtoEqual(tb, expectedState, actualState)
}

// OffloadingStorageState represent a state of the storage under a prefix
type OffloadingStorageState struct {
	Sink      *offloading.Sink
	Kind      OffloadingObjectStorageFormat
	FileTotal int
	Objects   []git.ObjectID
}

func RequireOffloadingStorageState(tb testing.TB, ctx context.Context, setup testTransactionSetup, prefix string, expected *OffloadingStorageState) {
	tb.Helper()
	sink := expected.Sink
	objects, err := sink.List(ctx, prefix)
	require.NoError(tb, err)
	require.Len(tb, objects, expected.FileTotal)
	if expected.FileTotal == 0 {
		// expecting empty objects in the prefix
		return
	}
	switch expected.Kind {
	case packFile:
		// For pack file-based storage, download the pack files and
		// verify object hashes match those extracted from the pack files
		tempDir := testhelper.TempDir(tb)
		indexObjCount := 0
		var localIndexFilePath string

		for _, obj := range objects {
			err := sink.Download(ctx, filepath.Join(prefix, obj), filepath.Join(tempDir, obj))
			require.NoError(tb, err)
			if filepath.Ext(obj) == ".idx" {
				indexObjCount++
				localIndexFilePath = filepath.Join(tempDir, obj)
			}
		}
		require.Equal(tb, indexObjCount, 1, "expect one and only one idx file")

		logger := testhelper.NewLogger(tb)
		index, err := packfile.ReadIndexWithGitCmdFactory(setup.CommandFactory, setup.Repo, logger, localIndexFilePath)
		require.NoError(tb, err)
		actualObjects := make([]git.ObjectID, 0, len(index.Objects))
		for _, obj := range index.Objects {
			actualObjects = append(actualObjects, git.ObjectID(obj.OID))
		}
		require.ElementsMatch(tb, actualObjects, expected.Objects)
	case looseObject:
		// For loose object-based storage, verify a one-to-one correspondence between
		// expected objects and those stored in the offloading storage
		require.Len(tb, objects, len(expected.Objects))
		actualObjects := make([]git.ObjectID, 0, len(expected.Objects))
		for _, obj := range objects {
			actualObjects = append(actualObjects, git.ObjectID(obj))
		}
		require.ElementsMatch(tb, actualObjects, expected.Objects)
	default:
		require.Fail(tb, "Unknown offloading storage file kind")
	}
}

type OffloadingStorageStates map[string]OffloadingStorageState

// Assert each prefix defined in the OffloadingStorageStates
func RequireOffloadingStorage(tb testing.TB, ctx context.Context, setup testTransactionSetup, expected OffloadingStorageStates) {
	if len(expected) == 0 {
		// we don't care comparing offloading storage, skip assertion
		return
	}
	for prefix, state := range expected {
		RequireOffloadingStorageState(tb, ctx, setup, prefix, &state)
	}
}

type testTransactionCommit struct {
	OID  git.ObjectID
	Pack []byte
}

type testTransactionTag struct {
	Name string
	OID  git.ObjectID
}

type testTransactionCommits struct {
	First       testTransactionCommit
	Second      testTransactionCommit
	Third       testTransactionCommit
	Diverging   testTransactionCommit
	Orphan      testTransactionCommit
	Unreachable testTransactionCommit
}

type testTransactionSetup struct {
	PartitionID       storage.PartitionID
	RelativePath      string
	RepositoryPath    string
	Repo              *localrepo.Repo
	Config            config.Cfg
	CommandFactory    gitcmd.CommandFactory
	RepositoryFactory localrepo.Factory
	ObjectHash        git.ObjectHash
	NonExistentOID    git.ObjectID
	Commits           testTransactionCommits
	AnnotatedTags     []testTransactionTag
	Consumer          storage.LogConsumer
	OffloadSink       *offloading.Sink
}

type testTransactionHooks struct {
	// BeforeApplyLogEntry is called before a log entry is applied to the repository.
	BeforeApplyLogEntry hookFunc
	// BeforeAppendLogEntry is called before a log entry is appended to the log.
	BeforeAppendLogEntry hookFunc
	// BeforeReadAppliedLSN is invoked before the applied LSN is read.
	BeforeReadAppliedLSN hookFunc
	// BeforeStoreAppliedLSN is invoked before the applied LSN is stored.
	BeforeStoreAppliedLSN hookFunc
	// WaitForTransactionsWhenClosing waits for a in-flight to finish before returning
	// from Run.
	WaitForTransactionsWhenClosing bool
}

// StartManager starts a TransactionManager.
type StartManager struct {
	// Hooks contains the hook functions that are configured on the TransactionManager. These allow
	// for better synchronization.
	Hooks testTransactionHooks
	// ExpectedError is the expected error to be raised from the manager's Run. Panics are converted
	// to errors and asserted to match this as well.
	ExpectedError error
	// ModifyStorage allows modifying the storage prior to the manager starting. This
	// may be necessary to test some states that can be reached from hard crashes
	// but not during the tests.
	ModifyStorage func(tb testing.TB, cfg config.Cfg, storagePath string)

	// Sink is the destination for objects during offloading operations.
	OverridingSink *offloading.Sink
}

// CloseManager closes a TransactionManager.
type CloseManager struct{}

// AssertManager asserts whether the manager has closed and Run returned. If it has, it asserts the
// error matched the expected. If the manager has exited with an error, AssertManager must be called
// or the test case fails.
type AssertManager struct {
	// ExpectedError is the error TransactionManager's Run method is expected to return.
	ExpectedError error
}

// Begin calls Begin on the TransactionManager to start a new transaction.
type Begin struct {
	// TransactionID is the identifier given to the transaction created. This is used to identify
	// the transaction in later steps.
	TransactionID int
	// RelativePath are the relative paths of the repositories this transaction is operating on.
	RelativePaths []string
	// ReadOnly indicates whether this is a read-only transaction.
	ReadOnly bool
	// Context is the context to use for the Begin call.
	Context context.Context
	// ExpectedSnapshot is the expected LSN this transaction should read the repsoitory's state at.
	ExpectedSnapshotLSN storage.LSN
	// ExpectedError is the error expected to be returned from the Begin call.
	ExpectedError error
}

// CreateRepository creates the transaction's repository..
type CreateRepository struct {
	// TransactionID is the transaction for which to create the repository.
	TransactionID int
	// DefaultBranch is the default branch to set in the repository.
	DefaultBranch git.ReferenceName
	// References are the references to create in the repository.
	References map[git.ReferenceName]git.ObjectID
	// Packs are the objects that are written into the repository.
	Packs [][]byte
	// CustomHooks are the custom hooks to write into the repository.
	CustomHooks []byte
	// Alternate links the given relative path as the repository's alternate.
	Alternate string
}

// RunPackRefs calls pack-refs housekeeping task on a transaction.
type RunPackRefs struct {
	// TransactionID is the transaction for which the pack-refs task runs.
	TransactionID int
}

// RunRepack calls repack housekeeping task on a transaction.
type RunRepack struct {
	// TransactionID is the transaction for which the repack task runs.
	TransactionID int
	// Config is the desired repacking config for the task.
	Config housekeepingcfg.RepackObjectsConfig
}

// RunOffloading calls prepare offloading task on a transaction
type RunOffloading struct {
	TransactionID int
	Config        housekeepingcfg.OffloadingConfig
}

// RunRehydrating calls prepare rehydrating task on a transaction
type RunRehydrating struct {
	TransactionID int
	Prefix        string
}

type ChangeGitConfig struct {
	TransactionID int
}

// WriteCommitGraphs calls commit-graphs writing housekeeping task on a transaction.
type WriteCommitGraphs struct {
	// TransactionID is the transaction for which the repack task runs.
	TransactionID int
	// Config is the desired commit-graphs config for the task.
	Config housekeepingcfg.WriteCommitGraphConfig
}

// CustomHooksUpdate models an update to the custom hooks.
type CustomHooksUpdate struct {
	// CustomHooksTAR contains the custom hooks as a TAR. The TAR contains a `custom_hooks`
	// directory which contains the hooks. Setting the update with nil `custom_hooks_tar` clears
	// the hooks from the repository.
	CustomHooksTAR []byte
}

// DefaultBranchUpdate provides the information to update the default branch of the repo.
type DefaultBranchUpdate struct {
	// Reference is the reference to update the default branch to.
	Reference git.ReferenceName
}

// alternateUpdate models an update to the repository's alternates file.
type alternateUpdate struct {
	// RelativePath is the relative path of the object pool to be linked to the target
	// repository as an alternate.
	RelativePath string
}

// Commit calls Commit on a transaction.
type Commit struct {
	// TransactionID identifies the transaction to commit.
	TransactionID int
	// Context is the context to use for the Commit call.
	Context context.Context
	// ExpectedError is the error that is expected to be returned when committing the transaction.
	// If ExpectedError is a function with signature func(tb testing.TB, actualErr error), it is
	// run instead of asserting the error.
	ExpectedError any

	// ReferenceUpdates are the reference updates to commit.
	ReferenceUpdates git.ReferenceUpdates
	// QuarantinedPacks are the packs to include in the quarantine directory of the transaction.
	QuarantinedPacks [][]byte
	// DefaultBranchUpdate is the default branch update to commit.
	DefaultBranchUpdate *DefaultBranchUpdate
	// CustomHooksUpdate is the custom hooks update to commit.
	CustomHooksUpdate *CustomHooksUpdate
	// CreateRepository creates the repository on commit.
	CreateRepository bool
	// DeleteRepository deletes the repository on commit.
	DeleteRepository bool
	// UpdateAlternate updates the repository's alternate when set.
	UpdateAlternate *alternateUpdate

	// UpdateGitConfig updates the repository's Git configuration with provided key-value pairs.
	// The pairs are written to the Git config file when this operation is executed.
	// Note: Currently, this operation exists solely for testing conflict detection
	// when multiple transactions attempt to modify Git configuration simultaneously.
	UpdateGitConfig map[string]string
}

// RecordInitialReferenceValues calls RecordInitialReferenceValues on a transaction.
type RecordInitialReferenceValues struct {
	// TransactionID identifies the transaction to prepare the reference updates on.
	TransactionID int
	// InitialValues are the initial values to record.
	InitialValues map[git.ReferenceName]git.Reference
}

// UpdateReferences calls UpdateReferences on a transaction.
type UpdateReferences struct {
	// TransactionID identifies the transaction to update references on.
	TransactionID int
	// ReferenceUpdates are the reference updates to make.
	ReferenceUpdates git.ReferenceUpdates
	// ExpectedError is the error that is expected to be returned when calling UpdateReferences.
	ExpectedError error
}

// SetKey calls SetKey on a transaction.
type SetKey struct {
	// TransactionID identifies the transaction to set a key in.
	TransactionID int
	// Key is the key to set.
	Key string
	// Value is the value to set the key to.
	Value string
	// ExpectedError is the expected error of the set operation.
	ExpectedError error
}

// DeleteKey calls DeleteKey on a transaction.
type DeleteKey struct {
	// TransactionID identifies the transaction to delete a key in.
	TransactionID int
	// Key is the key to delete.
	Key string
	// ExpectedError is the expected error of the delete operation.
	ExpectedError error
}

// ReadKey reads a key in a transaction.
type ReadKey struct {
	// TransactionID identifies the transaction to read a key in.
	TransactionID int
	// Key is the key to read.
	Key string
	// ExpectedValue is the expected value of the key.
	ExpectedValue string
	// ExpectedError is the expected error of the read operation.
	ExpectedError error
}

// ReadKeyPrefix reads a key prefix in a transaction.
type ReadKeyPrefix struct {
	// TransactionID identifies the transaction to read a key prfix in.
	TransactionID int
	// Prefix is the prefix to read.
	Prefix string
	// ExpectedValue is the expected value of the key.
	ExpectedValues map[string]string
}

// Rollback calls Rollback on a transaction.
type Rollback struct {
	// TransactionID identifies the transaction to rollback.
	TransactionID int
	// ExpectedError is the error that is expected to be returned when rolling back the transaction.
	ExpectedError error
}

// Prune prunes all unreferenced objects from the repository.
type Prune struct {
	// ExpectedObjects are the object expected to exist in the repository after pruning.
	ExpectedObjects []git.ObjectID
}

// ConsumerAcknowledge calls AcknowledgeConsumerPosition for all consumers.
type ConsumerAcknowledge struct {
	// LSN is the LSN acknowledged by the consumers.
	LSN storage.LSN
}

// RemoveRepository removes the repository from the disk. It must be run with the TransactionManager
// closed.
type RemoveRepository struct{}

// RepositoryAssertion asserts a given transaction's view of repositories matches the expected.
type RepositoryAssertion struct {
	// TransactionID identifies the transaction whose snapshot to assert.
	TransactionID int
	// Repositories is the expected state of the repositories the transaction sees. The
	// key is the repository's relative path and the value describes its expected state.
	Repositories RepositoryStates
}

// StateAssertions models an assertion of the entire state managed by the TransactionManager.
type StateAssertion struct {
	// Database is the expected state of the database.
	Database DatabaseState
	// NotOffsetDatabaseInRaft indicates if the LSN in the database should not be shifted.
	NotOffsetDatabaseInRaft bool
	// Directory is the expected state of the manager's state directory in the repository.
	Directory testhelper.DirectoryState
	// Repositories is the expected state of the repositories in the storage. The key is
	// the repository's relative path and the value describes its expected state.
	Repositories RepositoryStates

	// OffloadingStorage: the state the offloading bucket.
	OffloadingStorage OffloadingStorageStates
}

// AdhocAssertion allows a test to add some custom assertions apart from the built-in assertions above.
type AdhocAssertion func(*testing.T, context.Context, *TransactionManager)

type metricFamily interface {
	metricName() string
}

// histogramMetric asserts a histogram metric. We could not rely on the recorded
// values (latency/time/...). We assert the number of sample counts instead.
type histogramMetric string

func (m histogramMetric) metricName() string { return string(m) }

// AssertMetrics assert metrics collected from transaction manager. Although
// prometheus testutil provides some helpers for testing metrics, they are not
// flexible enough. It's particularly true when we want to assert histogram
// metrics.
type AssertMetrics map[metricFamily]map[string]int

// MockLogConsumer acts as a generic log consumer for testing the TransactionManager.
type MockLogConsumer struct {
	highWaterMark storage.LSN
}

func (lc *MockLogConsumer) NotifyNewEntries(storageName string, partitionID storage.PartitionID, lowWaterMark, highWaterMark storage.LSN) {
	lc.highWaterMark = highWaterMark
}

// steps defines execution steps in a test. Each test case can define multiple steps to exercise
// more complex behavior.
type steps []any

type transactionTestCase struct {
	desc          string
	skip          func(*testing.T)
	steps         steps
	customSetup   func(*testing.T, context.Context, storage.PartitionID, string) testTransactionSetup
	expectedState StateAssertion
}

func performReferenceUpdates(tb testing.TB, ctx context.Context, tx storage.Transaction, rewrittenRepo gitcmd.RepositoryExecutor, updates git.ReferenceUpdates) error {
	updater, err := updateref.New(ctx, rewrittenRepo, updateref.WithNoDeref())
	require.NoError(tb, err)

	require.NoError(tb, updater.Start())
	for reference, update := range updates {
		require.NoError(tb, updater.Update(reference, update.NewOID, update.OldOID))
	}
	require.NoError(tb, updater.Commit())

	return tx.UpdateReferences(ctx, updates)
}

func runTransactionTest(t *testing.T, ctx context.Context, tc transactionTestCase, setup testTransactionSetup) {
	logger := testhelper.NewLogger(t)
	catfileCache := catfile.NewCache(setup.Config)
	t.Cleanup(catfileCache.Stop)

	storageName := setup.Config.Storages[0].Name
	storageScopedFactory, err := setup.RepositoryFactory.ScopeByStorage(ctx, storageName)
	require.NoError(t, err)
	repo := storageScopedFactory.Build(setup.RelativePath)

	repoPath, err := repo.Path(ctx)
	require.NoError(t, err)

	dbMgr, err := databasemgr.NewDBManager(
		ctx,
		setup.Config.Storages,
		func(log logrus.Logger, path string) (keyvalue.Store, error) {
			return keyvalue.NewBadgerStore(log, filepath.Join(testhelper.TempDir(t), path))
		},
		helper.NewTimerTickerFactory(time.Minute),
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	database, err := dbMgr.GetDB(storageName)
	require.NoError(t, err)
	defer testhelper.MustClose(t, database)

	storagePath := setup.Config.Storages[0].Path
	stateDir := filepath.Join(storagePath, "state")

	stagingDir := filepath.Join(storagePath, "staging")
	require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

	newMetrics := func() Metrics {
		return NewMetrics(housekeeping.NewMetrics(setup.Config.Prometheus))
	}

	var raftFactory raftmgr.RaftReplicaFactory
	var raftEntryRecorder *raftmgr.ReplicaEntryRecorder
	clusterID := uuid.New().String()
	if testhelper.IsRaftEnabled() {
		raftEntryRecorder = raftmgr.NewReplicaEntryRecorder()
		setup.Config.Raft = config.DefaultRaftConfig(clusterID)
		// Speed up initial election overhead in the test setup
		setup.Config.Raft.ElectionTicks = 5
		setup.Config.Raft.RTTMilliseconds = 100
		setup.Config.Raft.SnapshotDir = testhelper.TempDir(t)

		raftNode, err := raftmgr.NewNode(setup.Config, logger, dbMgr, nil)
		require.NoError(t, err)

		raftFactory = raftmgr.DefaultFactoryWithNode(setup.Config.Raft, raftNode, raftmgr.WithEntryRecorder(raftEntryRecorder))
	}

	var (
		// managerRunning tracks whether the manager is running or closed.
		managerRunning bool
		// factory is the factory that produces the current TransactionManager
		factory = NewFactory(
			WithCmdFactory(setup.CommandFactory),
			WithRepoFactory(setup.RepositoryFactory),
			WithMetrics(newMetrics()),
			WithLogConsumer(setup.Consumer),
			WithRaftConfig(setup.Config.Raft),
			WithRaftFactory(raftFactory),
			WithOffloadingSink(setup.OffloadSink),
		)

		// transactionManager is the current TransactionManager instance.
		transactionManager = factory.New(logger, setup.PartitionID, database, storageName, storagePath, stateDir, stagingDir).(*TransactionManager)
		// managerErr is used for synchronizing manager closing and returning
		// the error from Run.
		managerErr chan error
		// inflightTransactions tracks the number of on going transactions calls. It is used to synchronize
		// the database hooks with transactions.
		inflightTransactions sync.WaitGroup
	)

	// closeManager closes the manager. It waits until the manager's Run method has exited.
	closeManager := func() {
		t.Helper()

		transactionManager.Close()
		managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
		require.NoError(t, err)
		require.False(t, managerRunning)
	}

	// openTransactions holds references to all of the transactions that have been
	// began in a test case.
	openTransactions := map[int]*Transaction{}

	closeManagerIfRunning := func() {
		if managerRunning {
			closeManager()
		}
	}

	// Close the manager if it is running at the end of the test.
	defer closeManagerIfRunning()
	for _, step := range tc.steps {
		switch step := step.(type) {
		case StartManager:
			require.False(t, managerRunning, "test error: manager started while it was already running")

			if step.ModifyStorage != nil {
				step.ModifyStorage(t, setup.Config, storagePath)
			}

			managerRunning = true
			managerErr = make(chan error)

			// The PartitionManager deletes and recreates the staging directory prior to starting a TransactionManager
			// to clean up any stale state leftover by crashes. Do that here as well so the tests don't fail if we don't
			// finish transactions after crash simulations.
			require.NoError(t, os.RemoveAll(stagingDir))
			require.NoError(t, os.Mkdir(stagingDir, mode.Directory))

			transactionManager = factory.New(logger, setup.PartitionID, database, storageName, storagePath, stateDir, stagingDir).(*TransactionManager)
			if step.OverridingSink != nil {
				// Typically, transactionManager.offloadingSink is properly set by Factory.New().
				// We override it here only in case when we need to simulate custom sink behaviors
				// (e.g., timeouts or interruptions) without complicating the main Factory logic.
				// This approach keeps the sink creation logic simple in production code
				// while allowing flexible testing scenarios.
				transactionManager.offloadingSink = step.OverridingSink
			}

			installHooks(transactionManager, &inflightTransactions, step.Hooks, raftEntryRecorder)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						err, ok := r.(error)
						if !ok {
							panic(r)
						}
						assert.ErrorIs(t, err, step.ExpectedError)
						managerErr <- err
					}
				}()

				managerErr <- transactionManager.Run()
			}()
		case CloseManager:
			require.True(t, managerRunning, "test error: manager closed while it was already closed")
			closeManager()
		case AssertManager:
			require.True(t, managerRunning, "test error: manager must be running for syncing")
			managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
			require.ErrorIs(t, err, step.ExpectedError)
		case Begin:
			require.NotContains(t, openTransactions, step.TransactionID, "test error: transaction id reused in begin")

			beginCtx := ctx
			if step.Context != nil {
				beginCtx = step.Context
			}

			transaction, err := transactionManager.Begin(beginCtx, storage.BeginOptions{
				Write:         !step.ReadOnly,
				RelativePaths: step.RelativePaths,
			})
			if step.ExpectedError != nil {
				require.ErrorIs(t, err, step.ExpectedError)
				continue
			}
			require.NoError(t, err)

			tx := transaction.(*Transaction)
			actualSnapshotLSN := tx.SnapshotLSN()
			if testhelper.IsRaftEnabled() {
				actualSnapshotLSN = raftEntryRecorder.WithRaftEntries(actualSnapshotLSN)
			}
			require.Equalf(t, step.ExpectedSnapshotLSN, actualSnapshotLSN, "mismatched ExpectedSnapshotLSN")
			require.NotEmpty(t, tx.Root(), "empty Root")
			require.Contains(t, tx.Root(), transactionManager.snapshotsDir())

			if step.ReadOnly {
				require.Empty(t,
					tx.quarantineDirectory,
					"read-only transaction should not have a quarantine directory",
				)
			}

			openTransactions[step.TransactionID] = tx
		case Commit:
			require.Contains(t, openTransactions, step.TransactionID, "test error: transaction committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			if transaction.relativePath != "" {
				rewrittenRepo := setup.RepositoryFactory.Build(
					transaction.RewriteRepository(&gitalypb.Repository{
						StorageName:  setup.Config.Storages[0].Name,
						RelativePath: transaction.relativePath,
					}),
				)

				if step.UpdateAlternate != nil {
					require.NoError(t, objectpool.Disconnect(
						storage.ContextWithTransaction(ctx, transaction),
						transaction.FS(),
						rewrittenRepo,
						logger,
						nil,
					))

					if step.UpdateAlternate.RelativePath != "" {
						require.NoError(t, objectpool.Link(
							storage.ContextWithTransaction(ctx, transaction),
							setup.RepositoryFactory.Build(transaction.RewriteRepository(&gitalypb.Repository{
								StorageName:  setup.Config.Storages[0].Name,
								RelativePath: step.UpdateAlternate.RelativePath,
							})),
							rewrittenRepo,
							nil,
						))
					}
				}

				if step.UpdateGitConfig != nil {
					updateGitConfig(t, ctx, rewrittenRepo, step.UpdateGitConfig, transaction)
				}

				if step.QuarantinedPacks != nil {
					for _, dir := range []string{
						transaction.stagingDirectory,
						transaction.quarantineDirectory,
					} {
						stat, err := os.Stat(dir)
						require.NoError(t, err)
						require.Equal(t, stat.Mode(), mode.Directory,
							"%q had %q mode but expected %q", dir, stat.Mode(), mode.Directory,
						)
					}

					for _, pack := range step.QuarantinedPacks {
						require.NoError(t, rewrittenRepo.UnpackObjects(ctx, bytes.NewReader(pack)))
					}
				}

				if step.ReferenceUpdates != nil {
					require.NoError(t, performReferenceUpdates(t, ctx, transaction, rewrittenRepo, step.ReferenceUpdates))
				}

				if step.DefaultBranchUpdate != nil {
					require.NoError(t, rewrittenRepo.SetDefaultBranch(storage.ContextWithTransaction(ctx, transaction), nil, step.DefaultBranchUpdate.Reference))
					require.NoError(t, transaction.UpdateReferences(ctx, map[git.ReferenceName]git.ReferenceUpdate{
						"HEAD": {NewTarget: step.DefaultBranchUpdate.Reference},
					}))
				}

				if step.CustomHooksUpdate != nil {
					require.NoError(t, repoutil.SetCustomHooks(
						storage.ContextWithTransaction(ctx, transaction),
						logger,
						config.NewLocator(setup.Config),
						nil,
						bytes.NewReader(step.CustomHooksUpdate.CustomHooksTAR),
						rewrittenRepo,
					))
				}

				if step.DeleteRepository {
					require.NoError(t, repoutil.Remove(
						storage.ContextWithTransaction(ctx, transaction),
						logger,
						config.NewLocator(setup.Config),
						nil,
						counter.NewRepositoryCounter(setup.Config.Storages),
						rewrittenRepo,
					))

					transaction.DeleteRepository()
				}
			}

			commitCtx := ctx
			if step.Context != nil {
				commitCtx = step.Context
			}

			_, commitErr := transaction.Commit(commitCtx)
			switch expectedErr := step.ExpectedError.(type) {
			case func(testing.TB, error):
				expectedErr(t, commitErr)
			case error:
				require.ErrorIs(t, commitErr, expectedErr)
			case nil:
				require.NoError(t, commitErr)
			default:
				t.Fatalf("unexpected error type: %T", expectedErr)
			}
		case RecordInitialReferenceValues:
			require.Contains(t, openTransactions, step.TransactionID, "test error: record initial reference value on transaction before beginning it")

			transaction := openTransactions[step.TransactionID]
			require.NoError(t, transaction.RecordInitialReferenceValues(ctx, step.InitialValues))
		case UpdateReferences:
			require.Contains(t, openTransactions, step.TransactionID, "test error: reference updates aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]

			rewrittenRepo := setup.RepositoryFactory.Build(
				transaction.RewriteRepository(&gitalypb.Repository{
					StorageName:  setup.Config.Storages[0].Name,
					RelativePath: transaction.relativePath,
				}),
			)

			require.Equal(t, step.ExpectedError, performReferenceUpdates(t, ctx, transaction, rewrittenRepo, step.ReferenceUpdates))
		case ReadKey:
			require.Contains(t, openTransactions, step.TransactionID, "test error: read key called on transaction before beginning it")

			transaction := openTransactions[step.TransactionID]
			item, err := transaction.KV().Get([]byte(step.Key))
			if step.ExpectedError != nil {
				require.Equal(t, step.ExpectedError, err)
				break
			}

			require.NoError(t, err)
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, step.ExpectedValue, string(value))
		case ReadKeyPrefix:
			func() {
				require.Contains(t, openTransactions, step.TransactionID, "test error: read key prefix called on transaction before beginning it")

				transaction := openTransactions[step.TransactionID]

				iterator := transaction.KV().NewIterator(keyvalue.IteratorOptions{Prefix: []byte(step.Prefix)})
				defer iterator.Close()

				actualValues := map[string]string{}
				for iterator.Rewind(); iterator.Valid(); iterator.Next() {
					value, err := iterator.Item().ValueCopy(nil)
					require.NoError(t, err)

					actualValues[string(iterator.Item().Key())] = string(value)
				}

				expectedValues := step.ExpectedValues
				if step.ExpectedValues == nil {
					expectedValues = map[string]string{}
				}

				require.Equal(t, expectedValues, actualValues)
			}()
		case SetKey:
			require.Contains(t, openTransactions, step.TransactionID, "test error: set key called on transaction before beginning it")

			transaction := openTransactions[step.TransactionID]
			require.Equal(t, step.ExpectedError, transaction.KV().Set([]byte(step.Key), []byte(step.Value)))
		case DeleteKey:
			require.Contains(t, openTransactions, step.TransactionID, "test error: delete key called on transaction before beginning it")

			transaction := openTransactions[step.TransactionID]
			require.Equal(t, step.ExpectedError, transaction.KV().Delete([]byte(step.Key)))
		case Rollback:
			require.Contains(t, openTransactions, step.TransactionID, "test error: transaction rollbacked before beginning it")
			require.Equal(t, step.ExpectedError, openTransactions[step.TransactionID].Rollback(ctx))
		case Prune:
			// Repack all objects into a single pack and remove all other packs to remove all
			// unreachable objects from the packs.
			gittest.Exec(t, setup.Config, "-C", repoPath, "repack", "-ad")
			// Prune all unreachable loose objects in the repository.
			gittest.Exec(t, setup.Config, "-C", repoPath, "prune")

			require.ElementsMatch(t, step.ExpectedObjects, gittest.ListObjects(t, setup.Config, repoPath))
		case RemoveRepository:
			require.NoError(t, os.RemoveAll(repoPath))
		case CreateRepository:
			require.Contains(t, openTransactions, step.TransactionID, "test error: repository created in transaction before beginning it")

			transaction := openTransactions[step.TransactionID]

			rewrittenRepository := transaction.RewriteRepository(&gitalypb.Repository{
				StorageName:  setup.Config.Storages[0].Name,
				RelativePath: transaction.relativePath,
			})

			locator := config.NewLocator(setup.Config)

			require.NoError(t, repoutil.Create(
				storage.ContextWithTransaction(ctx, transaction),
				logger,
				locator,
				setup.CommandFactory,
				catfileCache,
				nil,
				counter.NewRepositoryCounter(setup.Config.Storages),
				rewrittenRepository,
				func(repoProto *gitalypb.Repository) error {
					repo := setup.RepositoryFactory.Build(repoProto)

					if step.DefaultBranch != "" {
						require.NoError(t, repo.SetDefaultBranch(ctx, nil, step.DefaultBranch))
					}

					for _, pack := range step.Packs {
						require.NoError(t, repo.UnpackObjects(ctx, bytes.NewReader(pack)))
					}

					for name, oid := range step.References {
						require.NoError(t, repo.UpdateRef(ctx, name, oid, setup.ObjectHash.ZeroOID))
					}

					if step.CustomHooks != nil {
						require.NoError(t,
							repoutil.SetCustomHooks(ctx, logger, config.NewLocator(setup.Config), nil, bytes.NewReader(step.CustomHooks), repo),
						)
					}

					if step.Alternate != "" {
						repoPath, err := repo.Path(ctx)
						require.NoError(t, err)

						alternatesPath := stats.AlternatesFilePath(repoPath)

						alternatesContent, err := filepath.Rel(
							filepath.Join(transaction.FS().Root(), transaction.relativePath, "objects"),
							filepath.Join(transaction.FS().Root(), step.Alternate, "objects"),
						)
						require.NoError(t, err)

						require.NoError(t, os.WriteFile(alternatesPath, []byte(alternatesContent), fs.ModePerm))
					}

					return nil
				},
				repoutil.WithObjectHash(setup.ObjectHash),
			))
		case RunPackRefs:
			require.Contains(t, openTransactions, step.TransactionID, "test error: pack-refs housekeeping task aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			transaction.PackRefs()
		case RunRepack:
			require.Contains(t, openTransactions, step.TransactionID, "test error: repack housekeeping task aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			transaction.Repack(step.Config)
		case WriteCommitGraphs:
			require.Contains(t, openTransactions, step.TransactionID, "test error: repack housekeeping task aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			transaction.WriteCommitGraphs(step.Config)
		case ConsumerAcknowledge:
			lsn := step.LSN
			if testhelper.IsRaftEnabled() {
				lsn = raftEntryRecorder.WithoutRaftEntries(lsn)
			}
			require.NoError(t, transactionManager.logManager.AcknowledgePosition(log.ConsumerPosition, lsn))
		case RepositoryAssertion:
			require.Contains(t, openTransactions, step.TransactionID, "test error: transaction's snapshot asserted before beginning it")
			transaction := openTransactions[step.TransactionID]

			RequireRepositories(t, ctx, setup.Config,
				// Assert the contents of the transaction's snapshot.
				filepath.Join(setup.Config.Storages[0].Path, transaction.snapshot.Prefix()),
				// Rewrite all of the repositories to point to their snapshots.
				func(relativePath string) *localrepo.Repo {
					return setup.RepositoryFactory.Build(
						transaction.RewriteRepository(&gitalypb.Repository{
							StorageName:  setup.Config.Storages[0].Name,
							RelativePath: relativePath,
						}),
					)
				}, step.Repositories)
		case AdhocAssertion:
			step(t, ctx, transactionManager)
		case AssertMetrics:
			reg := prometheus.NewPedanticRegistry()
			registerErr := reg.Register(transactionManager.metrics.housekeeping)
			require.NoError(t, registerErr)
			promMetrics, err := reg.Gather()
			require.NoError(t, err)

			actualMetricAssertion := AssertMetrics{}
			for family := range step {
				for _, actualFamily := range promMetrics {
					if actualFamily.GetName() != family.metricName() {
						continue
					}

					switch family.(type) {
					case histogramMetric:
						require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, actualFamily.GetType())

						familyName := histogramMetric(actualFamily.GetName())
						actualMetricAssertion[familyName] = map[string]int{}
						for _, actualMetric := range actualFamily.GetMetric() {
							var labels []string
							for _, actualLabel := range actualMetric.GetLabel() {
								var label strings.Builder
								label.WriteString(actualLabel.GetName())
								label.WriteString("=")
								label.WriteString(actualLabel.GetValue())
								labels = append(labels, label.String())
							}
							actualMetricAssertion[familyName][strings.Join(labels, ",")] = int(actualMetric.GetHistogram().GetSampleCount())
						}
					default:
						panic(fmt.Sprintf("metric assertion has not supported %s metric type", family))
					}
				}
			}

			require.Equal(t, step, actualMetricAssertion)
		case RunOffloading:
			require.Contains(t, openTransactions, step.TransactionID, "test error: offloading task aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			transaction.SetOffloadingConfig(step.Config)
		case RunRehydrating:
			require.Contains(t, openTransactions, step.TransactionID, "test error: rehydrating task aborted on committed before beginning it")

			transaction := openTransactions[step.TransactionID]
			transaction.SetRehydratingConfig(step.Prefix)
		default:
			t.Fatalf("unhandled step type: %T", step)
		}
	}

	if managerRunning {
		managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
		require.NoError(t, err)
	}

	// If singular Raft cluster is enabled, the Raft handler starts to insert raft log entries. We need to offset
	// the expected LSN to account for LSN shifting. As the order of insertion is not deterministic, the LSN
	// shifting depends on a Raft event recorder.
	//
	// Without Raft:
	// Entry 1 (update A) -> Entry 2 (update B)
	//
	// With Raft:
	// Entry 1 (ConfChange) -> Entry 2 (Empty verification entry) -> Entry 3 (update A) -> Entry 4 (update B)
	if testhelper.IsRaftEnabled() && raftEntryRecorder.Len() > 0 {
		if !tc.expectedState.NotOffsetDatabaseInRaft {
			appliedLSN, exist := tc.expectedState.Database[string(keyAppliedLSN)]
			if exist {
				// If expected applied LSN is present, offset expected LSN.
				appliedLSN := appliedLSN.(*gitalypb.LSN)
				tc.expectedState.Database[string(keyAppliedLSN)] = raftEntryRecorder.WithoutRaftEntries(storage.LSN(appliedLSN.GetValue())).ToProto()
			} else {
				// Otherwise, the test expects no applied log entry in cases such as invalid transactions.
				// Regardless, raft log entries are applied successfully.
				if tc.expectedState.Database == nil {
					tc.expectedState.Database = DatabaseState{}
				}
				tc.expectedState.Database[string(keyAppliedLSN)] = raftEntryRecorder.Latest().ToProto()
			}
		}
		// Those are raft-specific keys. They should not be a concern of TransactionManager tests.
		for _, key := range raftmgr.RaftDBKeys {
			tc.expectedState.Database[string(key)] = &anypb.Any{}
		}
		peerKey := fmt.Sprintf("p/1/raft/%s/%d", storageName, setup.PartitionID)
		if _, ok := tc.expectedState.Database[peerKey]; !ok {
			tc.expectedState.Database[peerKey] = &anypb.Any{}
		}
	}
	RequireDatabase(t, ctx, database, tc.expectedState.Database)

	expectedRepositories := tc.expectedState.Repositories
	if expectedRepositories == nil {
		expectedRepositories = RepositoryStates{
			setup.RelativePath: {},
		}
	}

	for relativePath, state := range expectedRepositories {
		if state.Objects == nil {
			state.Objects = []git.ObjectID{
				setup.ObjectHash.EmptyTreeOID,
				setup.Commits.First.OID,
				setup.Commits.Second.OID,
				setup.Commits.Third.OID,
				setup.Commits.Diverging.OID,
			}
			for _, tag := range setup.AnnotatedTags {
				state.Objects = append(state.Objects, tag.OID)
			}
		}

		if state.DefaultBranch == "" {
			state.DefaultBranch = git.DefaultRef
		}

		expectedRepositories[relativePath] = state
	}

	// Close the manager if it is still running to finish all of the background tasks. They may affect the state being
	// asserted.
	closeManagerIfRunning()
	// Close snapshots before asserting the repositories in the storage. Otherwise the repositories in the shared snapshots
	// will be considered to be repositories in the storage.
	require.NoError(t, transactionManager.CloseSnapshots())
	// The temporary state of transaction is cleaned in the background by the transaction manager.
	// Wait for these tasks to complete before asserting the storage directory's state as they would
	// still be visible there otherwise.
	RequireRepositories(t, ctx, setup.Config, setup.Config.Storages[0].Path, storageScopedFactory.Build, expectedRepositories)

	RequireOffloadingStorage(t, ctx, setup, tc.expectedState.OffloadingStorage)

	expectedDirectory := tc.expectedState.Directory
	if expectedDirectory == nil {
		// Set the base state as the default so we don't have to repeat it in every test case but it
		// gets asserted.
		expectedDirectory = testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		}
	}

	expectedDirectory = modifyDirectoryStateForRaft(t, expectedDirectory, transactionManager, raftEntryRecorder)

	testhelper.RequireDirectoryState(t, stateDir, "", dirStateWithOrWithoutRaft(expectedDirectory, storageName, setup.PartitionID.String()))

	expectedStagingDirState := testhelper.DirectoryState{
		"/": {Mode: mode.Directory},
	}

	// Snapshots directory may not exist if the manager failed to initialize. Check if it exists, and if so,
	// ensure that it is empty.
	if _, err := os.Stat(transactionManager.snapshotsDir()); err == nil {
		expectedStagingDirState["/snapshots"] = testhelper.DirectoryEntry{Mode: mode.Directory}
	} else {
		require.ErrorIs(t, err, fs.ErrNotExist)
	}

	testhelper.RequireDirectoryState(t, transactionManager.stagingDirectory, "", expectedStagingDirState)
}

// modifyDirectoryStateForRaft modifies log entry directory. It does three major things:
// - Shift expected log entry paths to the corresponding location. The LSN of normal entries are affected by Raft's
// internal entries.
// - Insert RAFT metadata files.
// - Insert Raft's internal log entries if they are not pruned away.
func modifyDirectoryStateForRaft(t *testing.T, expectedDirectory testhelper.DirectoryState, tm *TransactionManager, recorder *raftmgr.ReplicaEntryRecorder) testhelper.DirectoryState {
	if !testhelper.IsRaftEnabled() {
		return expectedDirectory
	}

	newExpectedDirectory := testhelper.DirectoryState{}
	lsnRegex := regexp.MustCompile(`/wal/(\d+)\/?.*`)
	for path, state := range expectedDirectory {
		matches := lsnRegex.FindStringSubmatch(path)
		if len(matches) == 0 {
			// If no LSN found, retain the original entry
			newExpectedDirectory[path] = state
			continue
		}
		// Extract and offset the LSN
		lsnStr := matches[1]
		lsn, err := strconv.ParseUint(lsnStr, 10, 64)
		require.NoError(t, err)

		offsetLSN := recorder.WithoutRaftEntries(storage.LSN(lsn))
		offsetPath := strings.Replace(path, fmt.Sprintf("/%s", lsnStr), fmt.Sprintf("/%s", offsetLSN.String()), 1)
		newExpectedDirectory[offsetPath] = state
		newExpectedDirectory[fmt.Sprintf("/wal/%s/RAFT", offsetLSN)] = testhelper.DirectoryEntry{
			Mode:    mode.File,
			Content: "anything",
			ParseContent: func(tb testing.TB, path string, content []byte) any {
				// The test should not be concerned with the actual content of RAFT file.
				return "anything"
			},
		}
	}

	// Insert Raft-specific log entries into the new directory structure
	raftEntries := recorder.FromRaft()
	for lsn, raftEntry := range raftEntries {
		if lsn >= tm.GetLogReader().LowWaterMark() {
			newExpectedDirectory[fmt.Sprintf("/wal/%s", lsn)] = testhelper.DirectoryEntry{Mode: mode.Directory}
			newExpectedDirectory[fmt.Sprintf("/wal/%s/MANIFEST", lsn)] = manifestDirectoryEntry(raftEntry)
			newExpectedDirectory[fmt.Sprintf("/wal/%s/RAFT", lsn)] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: "anything",
				ParseContent: func(tb testing.TB, path string, content []byte) any {
					// The test should not be concerned with the actual content of RAFT file.
					return "anything"
				},
			}
		}
	}

	// Update expected directory
	return newExpectedDirectory
}

func checkManagerError(t *testing.T, ctx context.Context, managerErrChannel chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	testTransaction := &Transaction{
		relativePath: "sentinel-non-existent",
		result:       make(resultChannel, 1),
		finish:       func(bool) error { return nil },
	}

	var (
		// managerErr is the error returned from the TransactionManager's Run method.
		managerErr error
		// closeChannel determines whether the channel was still open. If so, we need to close it
		// so further calls of checkManagerError do not block as they won't manage to receive an err
		// as it was already received and won't be able to send as the manager is no longer running.
		closeChannel bool
	)

	select {
	case managerErr, closeChannel = <-managerErrChannel:
	case mgr.admissionQueue <- testTransaction:
		// If the error channel doesn't receive, we don't know whether it is because the manager is still running
		// or we are still waiting for it to return. We test whether the manager is running or not here by queueing a
		// a transaction that will error. If the manager processes it, we know it is still running.
		//
		// If the manager was closed, it might manage to admit the testTransaction but not process it. To determine
		// whether that was the case, we also keep waiting on the managerErr channel.
		select {
		case result := <-testTransaction.result:
			require.Error(t, result.error, "test transaction is expected to error out")

			// Begin a transaction to wait until the manager has applied all log entries currently
			// committed. This ensures the disk state assertions run with all log entries fully applied
			// to the repository.
			tx, err := mgr.Begin(ctx, storage.BeginOptions{
				RelativePaths: []string{"non-existent"},
			})
			require.NoError(t, err)
			require.NoError(t, tx.Rollback(ctx))

			return true, nil
		case managerErr, closeChannel = <-managerErrChannel:
		}
	}

	if closeChannel {
		close(managerErrChannel)
	}

	return false, managerErr
}

// updateGitConfig mocks an operation that modifies Git configuration within a transaction
func updateGitConfig(t *testing.T, ctx context.Context, repo *localrepo.Repo, configKV map[string]string, tx *Transaction) {
	for k, v := range configKV {
		require.NoError(t, repo.SetConfig(ctx, k, v, nil))
	}

	repoPath, err := repo.Path(ctx)
	require.NoError(t, err)
	configPath := filepath.Join(repoPath, "config")

	require.NotNil(t, tx)
	relPath, err := filepath.Rel(tx.FS().Root(), configPath)
	require.NoError(t, err)
	require.NoError(t, tx.FS().RecordFile(relPath))
}

// convertToReplicaState transforms a DirectoryState with "/wal" paths
// to the new partitioned format with "/partitions/XX/YY/storageName_id/wal".
func convertToReplicaState(state testhelper.DirectoryState, storageName, partitionID string) testhelper.DirectoryState {
	// Calculate the hash-based path

	// Empty dir state - no conversion
	if len(state) == 0 {
		return testhelper.DirectoryState{
			"/":           {Mode: mode.Directory},
			"/partitions": {Mode: mode.Directory},
		}
	}

	raftPartitionPath := storage.GetRaftPartitionName(storageName, partitionID)
	hasher := sha256.New()
	hasher.Write([]byte(raftPartitionPath))
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Create partition directory structure
	prefix := fmt.Sprintf("/partitions/%s/%s/%s", hash[:2], hash[2:4], raftPartitionPath)
	result := testhelper.DirectoryState{
		"/":                       {Mode: mode.Directory},
		"/partitions":             {Mode: mode.Directory},
		"/partitions/" + hash[:2]: {Mode: mode.Directory},
		"/partitions/" + hash[:2] + "/" + hash[2:4]: {Mode: mode.Directory},
		prefix:          {Mode: mode.Directory},
		prefix + "/wal": {Mode: mode.Directory},
	}
	// Copy and transform all entries
	for path, entry := range state {
		if strings.HasPrefix(path, "/wal") {
			// Replace "/wal" with the partition prefix
			newPath := prefix + path // 4 is len("/wal")
			result[newPath] = entry
		}
	}

	return result
}

// dirStateWithOrWithoutRaft returns the appropriate DatabaseState
// based on whether Raft is enabled
func dirStateWithOrWithoutRaft(state testhelper.DirectoryState, storageName, partitionID string) testhelper.DirectoryState {
	if testhelper.IsRaftEnabled() {
		return convertToReplicaState(state, storageName, partitionID)
	}
	return state
}

func getTestDBManager(t *testing.T, ctx context.Context, cfg config.Cfg, logger logrus.Logger) (*databasemgr.DBManager, keyvalue.Store) {
	t.Helper()

	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	db, err := dbMgr.GetDB(cfg.Storages[0].Name)
	require.NoError(t, err)

	return dbMgr, db
}
