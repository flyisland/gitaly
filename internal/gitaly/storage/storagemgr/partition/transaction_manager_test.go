package partition

import (
	"archive/tar"
	"bytes"
	"container/list"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict/refdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// errSimulatedCrash is used in the tests to simulate a crash at a certain point during
// TransactionManager.Run execution.
var errSimulatedCrash = errors.New("simulated crash")

func manifestDirectoryEntry(expected *gitalypb.LogEntry) testhelper.DirectoryEntry {
	return testhelper.DirectoryEntry{
		Mode:    mode.File,
		Content: expected,
		ParseContent: func(tb testing.TB, path string, content []byte) any {
			var logEntry gitalypb.LogEntry
			require.NoError(tb, proto.Unmarshal(content, &logEntry))
			return &logEntry
		},
	}
}

// buildReftableDirectory builds the testhelper.DirectoryState for reftables.
// In the file system backend, we know for certain the files which will be included
// and also their content. But in the reftable backend, we have a binary format
// which we cannot verify and this is not deterministic. So let's do a best effort
// basis here. We verify that the number of tables are correct and that the tables.list
// constituents of reftables. We do not verify the content of the reftable themselves
// here.
func buildReftableDirectory(data map[int][]git.ReferenceUpdates) testhelper.DirectoryState {
	state := testhelper.DirectoryState{
		"/":    {Mode: mode.Directory},
		"/wal": {Mode: mode.Directory},
	}

	for id, updates := range data {
		prefix := fmt.Sprintf("/wal/000000000000%d", id)
		numTables := uint(len(updates))

		state[prefix] = testhelper.DirectoryEntry{Mode: mode.Directory}
		state[prefix+"/MANIFEST"] = testhelper.DirectoryEntry{
			Mode:    mode.File,
			Content: true,
			ParseContent: func(tb testing.TB, path string, content []byte) any {
				var logEntry gitalypb.LogEntry
				require.NoError(tb, proto.Unmarshal(content, &logEntry))

				// If there are no reftables being created, we exit early.
				if numTables == 0 {
					return true
				}

				// If there are reftables being created, we need to account for
				// N tables and +2 for the tables.list being updated.
				require.Equal(tb, numTables+2, uint(len(logEntry.GetOperations())))

				// The reftables should only be created.
				for i := uint(0); i < numTables; i++ {
					create := logEntry.GetOperations()[i].GetCreateHardLink()
					require.NotNil(tb, create)
					require.True(tb, git.ReftableTableNameRegex.Match(create.GetDestinationPath()))
				}

				// The tables.list should be deleted and create (updated).
				delete := logEntry.GetOperations()[numTables].GetRemoveDirectoryEntry()
				require.NotNil(tb, delete)
				require.True(tb, strings.Contains(string(delete.GetPath()), "tables.list"))

				create := logEntry.GetOperations()[numTables].GetRemoveDirectoryEntry()
				require.NotNil(tb, create)
				require.True(tb, strings.Contains(string(delete.GetPath()), "tables.list"))

				return true
			},
		}

		// Parse the reftable and check its references.
		for i := uint(1); i <= numTables; i++ {
			state[fmt.Sprintf("%s/%d", prefix, i)] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: updates[i-1],
				ParseContent: func(tb testing.TB, path string, content []byte) any {
					table, err := git.NewReftable(content)
					require.NoError(tb, err)

					references, err := table.IterateRefs()
					require.NoError(tb, err)

					refUpdates := make(git.ReferenceUpdates)

					for _, reference := range references {
						update := git.ReferenceUpdate{}

						if reference.IsSymbolic {
							update.NewTarget = git.ReferenceName(reference.Target)
						} else {
							update.NewOID = git.ObjectID(reference.Target)
						}

						refUpdates[reference.Name] = update
					}

					return refUpdates
				},
			}
		}

		// For tables.list, we can verify that all the lines within that file are
		// reftables indeed.
		if numTables > 0 {
			state[fmt.Sprintf("%s/%d", prefix, numTables+1)] = testhelper.DirectoryEntry{
				Mode:    mode.File,
				Content: true,
				ParseContent: func(tb testing.TB, path string, content []byte) any {
					for _, file := range strings.Split(string(content), "\n") {
						if len(file) == 0 {
							break
						}

						require.True(tb, git.ReftableTableNameRegex.Match([]byte(file)))
					}
					return true
				},
			}
		}
	}

	return state
}

func validCustomHooks(tb testing.TB) []byte {
	tb.Helper()

	var hooks bytes.Buffer
	writer := tar.NewWriter(&hooks)
	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/",
		Mode: int64(mode.Directory),
	}))

	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/pre-receive",
		Size: int64(len("hook content")),
		Mode: int64(fs.ModePerm),
	}))
	_, err := writer.Write([]byte("hook content"))
	require.NoError(tb, err)

	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/private-dir/",
		Mode: int64(mode.Directory),
	}))

	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/private-dir/private-file",
		Size: int64(len("private content")),
		Mode: int64(mode.File),
	}))
	_, err = writer.Write([]byte("private content"))
	require.NoError(tb, err)

	require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "ignored_file"}))
	require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "ignored_dir/ignored_file"}))

	require.NoError(tb, writer.Close())
	return hooks.Bytes()
}

func refChangeLogEntry(setup testTransactionSetup, ref string, oid git.ObjectID) *gitalypb.LogEntry {
	entry := &gitalypb.LogEntry{
		RelativePath: setup.RelativePath,
		ReferenceTransactions: []*gitalypb.LogEntry_ReferenceTransaction{
			{
				Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
					{
						ReferenceName: []byte(ref),
						NewOid:        []byte(oid),
					},
				},
			},
		},
		Operations: []*gitalypb.LogEntry_Operation{
			{
				Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
					CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
						SourcePath:      []byte("1"),
						DestinationPath: []byte(filepath.Join(setup.RelativePath, ref)),
					},
				},
			},
		},
	}

	if testhelper.IsReftableEnabled() {
		entry.Operations = append(entry.Operations, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry_{
				RemoveDirectoryEntry: &gitalypb.LogEntry_Operation_RemoveDirectoryEntry{
					Path: []byte(filepath.Join(setup.RelativePath, "reftable/tables.list")),
				},
			},
		}, &gitalypb.LogEntry_Operation{
			Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
				CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
					SourcePath:      []byte("2"),
					DestinationPath: []byte(filepath.Join(setup.RelativePath, "reftable/tables.list")),
				},
			},
		})
	}

	return entry
}

func setupTest(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) testTransactionSetup {
	t.Helper()

	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		RelativePath:           relativePath,
	})

	firstCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
	secondCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommitOID))
	thirdCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(secondCommitOID))
	divergingCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommitOID), gittest.WithMessage("diverging commit"))

	cmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	logger := testhelper.NewLogger(t)
	locator := config.NewLocator(cfg)
	localRepo := localrepo.New(
		logger,
		locator,
		cmdFactory,
		catfileCache,
		repo,
	)

	objectHash, err := localRepo.ObjectHash(ctx)
	require.NoError(t, err)

	hasher := objectHash.Hash()
	_, err = hasher.Write([]byte("content does not matter"))
	require.NoError(t, err)
	nonExistentOID, err := objectHash.FromHex(hex.EncodeToString(hasher.Sum(nil)))
	require.NoError(t, err)

	packCommit := func(oid git.ObjectID) []byte {
		t.Helper()

		var pack bytes.Buffer
		require.NoError(t,
			localRepo.PackObjects(ctx, strings.NewReader(oid.String()), &pack),
		)

		return pack.Bytes()
	}

	return testTransactionSetup{
		PartitionID:       testPartitionID,
		RelativePath:      relativePath,
		RepositoryPath:    repoPath,
		Repo:              localRepo,
		Config:            cfg,
		ObjectHash:        objectHash,
		CommandFactory:    cmdFactory,
		RepositoryFactory: localrepo.NewFactory(logger, locator, cmdFactory, catfileCache),
		NonExistentOID:    nonExistentOID,
		Commits: testTransactionCommits{
			First: testTransactionCommit{
				OID:  firstCommitOID,
				Pack: packCommit(firstCommitOID),
			},
			Second: testTransactionCommit{
				OID:  secondCommitOID,
				Pack: packCommit(secondCommitOID),
			},
			Third: testTransactionCommit{
				OID:  thirdCommitOID,
				Pack: packCommit(thirdCommitOID),
			},
			Diverging: testTransactionCommit{
				OID:  divergingCommitOID,
				Pack: packCommit(divergingCommitOID),
			},
		},
	}
}

func TestTransactionManager(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// testPartitionID is the partition ID used in the tests for the TransactionManager.
	const testPartitionID storage.PartitionID = 1

	// A clean repository is setup for each test. We build a setup ahead of the tests here once to
	// get deterministic commit IDs, relative path and object hash we can use to build the declarative
	// test cases.
	relativePath := gittest.NewRepositoryName(t)
	setup := setupTest(t, ctx, testPartitionID, relativePath)

	subTests := map[string][]transactionTestCase{
		"Common":                           generateCommonTests(t, ctx, setup),
		"CommittedEntries":                 generateCommittedEntriesTests(t, setup),
		"ModifyReferences":                 generateModifyReferencesTests(t, setup),
		"CreateRepository":                 generateCreateRepositoryTests(t, setup),
		"DeleteRepository":                 generateDeleteRepositoryTests(t, setup),
		"DefaultBranch":                    generateDefaultBranchTests(t, setup),
		"Alternate":                        generateAlternateTests(t, setup),
		"CustomHooks":                      generateCustomHooksTests(t, setup),
		"Housekeeping/PackRefs":            generateHousekeepingPackRefsTests(t, ctx, testPartitionID, relativePath),
		"Housekeeping/RepackingStrategy":   generateHousekeepingRepackingStrategyTests(t, ctx, testPartitionID, relativePath),
		"Housekeeping/RepackingConcurrent": generateHousekeepingRepackingConcurrentTests(t, ctx, setup),
		"Housekeeping/CommitGraphs":        generateHousekeepingCommitGraphsTests(t, ctx, setup),
		"Consumer":                         generateConsumerTests(t, setup),
		"KeyValue":                         generateKeyValueTests(setup),
	}

	for desc, tests := range subTests {
		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s/%s", desc, tc.desc), func(t *testing.T) {
				t.Parallel()

				if tc.skip != nil {
					tc.skip(t)
				}

				// Setup the repository with the exact same state as what was used to build the test cases.
				var setup testTransactionSetup
				if tc.customSetup != nil {
					setup = tc.customSetup(t, ctx, testPartitionID, relativePath)
				} else {
					setup = setupTest(t, ctx, testPartitionID, relativePath)
				}

				runTransactionTest(t, ctx, tc, setup)
			})
		}
	}
}

func generateCommonTests(t *testing.T, ctx context.Context, setup testTransactionSetup) []transactionTestCase {
	return []transactionTestCase{
		{
			desc: "begin returns if context is canceled before initialization",
			steps: steps{
				Begin{
					RelativePaths: []string{setup.RelativePath},
					Context: func() context.Context {
						ctx, cancel := context.WithCancel(ctx)
						cancel()
						return ctx
					}(),
					ExpectedError: context.Canceled,
				},
			},
			expectedState: StateAssertion{
				// Manager is not started up so no state is initialized.
				Directory: testhelper.DirectoryState{},
			},
		},
		{
			desc: "commit returns if transaction processing stops before admission",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				CloseManager{},
				Commit{
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
			},
		},
		func() transactionTestCase {
			ctx, cancel := context.WithCancel(ctx)
			return transactionTestCase{
				desc: "commit returns if context is canceled after admission",
				steps: steps{
					StartManager{
						Hooks: testTransactionHooks{
							BeforeAppendLogEntry: func(hookCtx hookContext) {
								// Cancel the context used in Commit
								cancel()
							},
						},
					},
					Begin{
						RelativePaths: []string{setup.RelativePath},
					},
					Commit{
						Context: ctx,
						ReferenceUpdates: git.ReferenceUpdates{
							"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						},
						ExpectedError: context.Canceled,
					},
				},
				expectedState: StateAssertion{
					Database: DatabaseState{
						string(keyAppliedLSN): storage.LSN(1).ToProto(),
					},
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/main": setup.Commits.First.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 1,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
														IsSymbolic: true,
													},
												},
											},
											{
												MinIndex: 2,
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:   "refs/heads/main",
														Target: setup.Commits.First.OID.String(),
													},
												},
											},
										},
									},
								},
							),
						},
					},
				},
			}
		}(),
		{
			desc: "commit returns if transaction processing stops before transaction acceptance",
			steps: steps{
				StartManager{
					Hooks: testTransactionHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.closeManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an storage.ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenClosing: true,
					},
				},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				CloseManager{},
				Commit{
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
			},
		},
		{
			desc: "commit returns if transaction processing stops after transaction acceptance",
			steps: steps{
				StartManager{
					Hooks: testTransactionHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
			},
			expectedState: StateAssertion{
				Directory: gittest.FilesOrReftables(testhelper.DirectoryState{
					"/":                  {Mode: mode.Directory},
					"/wal":               {Mode: mode.Directory},
					"/wal/0000000000001": {Mode: mode.Directory},
					"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(&gitalypb.LogEntry{
						RelativePath: setup.RelativePath,
						ReferenceTransactions: []*gitalypb.LogEntry_ReferenceTransaction{
							{
								Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
									{
										ReferenceName: []byte("refs/heads/main"),
										NewOid:        []byte(setup.Commits.First.OID),
									},
								},
							},
						},
						Operations: []*gitalypb.LogEntry_Operation{
							{
								Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
									CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
										SourcePath:      []byte("1"),
										DestinationPath: []byte(filepath.Join(setup.RelativePath, "refs/heads/main")),
									},
								},
							},
						},
					}),
					"/wal/0000000000001/1": {Mode: mode.File, Content: []byte(setup.Commits.First.OID + "\n")},
				}, buildReftableDirectory(map[int][]git.ReferenceUpdates{
					1: {{"refs/heads/main": git.ReferenceUpdate{NewOID: setup.Commits.First.OID}}},
				})),
			},
		},
		{
			desc: "read snapshots include committed data",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{setup.RelativePath},
				},
				RepositoryAssertion{
					TransactionID: 1,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				// Transaction 2 is isolated from the changes made by transaction 1. It does not see the
				// committed changes.
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				// Transaction 3 is should see the new changes as it began after transaction 1 was committed.
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/main": setup.Commits.First.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 1,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
														IsSymbolic: true,
													},
												},
											},
											{
												MinIndex: 2,
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:   "refs/heads/main",
														Target: setup.Commits.First.OID.String(),
													},
												},
											},
										},
									},
								},
							),
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: mode.Directory},
								"/pre-receive": {
									Mode:    mode.Executable,
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: mode.Directory},
								"/private-dir/private-file": {Mode: mode.File, Content: []byte("private content")},
							},
						},
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					ExpectedError: refdb.UnexpectedOldValueError{
						TargetReference: "refs/heads/main",
						ExpectedValue:   setup.ObjectHash.ZeroOID.String(),
						ActualValue:     setup.Commits.First.OID.String(),
					},
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Rollback{
					TransactionID: 3,
				},
				Begin{
					TransactionID:       5,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 4,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
				Begin{
					TransactionID:       6,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				Rollback{
					TransactionID: 5,
				},
				Rollback{
					TransactionID: 6,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: mode.Directory},
						},
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.Third.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.Third.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc: "pack file includes referenced commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file reapplying works",
			steps: steps{
				Prune{},
				StartManager{
					Hooks: testTransactionHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					ExpectedError:    storage.ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "repository contains packfile's unreachable dependency",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID:    1,
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					IncludeObjects:   []git.ObjectID{setup.Commits.First.OID},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.Second.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
			},
		},
		{
			desc: "transaction points references to a new and existing commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID:    1,
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					IncludeObjects:   []git.ObjectID{setup.Commits.First.OID},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/existing": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/new":      {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/existing": setup.Commits.First.OID,
										"refs/heads/new":      setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/existing",
													Target: setup.Commits.First.OID.String(),
												},
												{
													Name:   "refs/heads/new",
													Target: setup.Commits.Second.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file missing intermediate commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Third.Pack},
					ExpectedError:    localrepo.InvalidObjectError(setup.Commits.Second.OID),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file only",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID:    1,
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "transaction includes an unreachable object with dependencies",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Third.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
			},
		},
		{
			desc: "transaction includes multiple unreachable objects",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Diverging.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID, setup.Commits.Diverging.OID},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Diverging.OID,
						},
					},
				},
			},
		},
		{
			desc: "transaction includes a missing object",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID},
					ExpectedError:  localrepo.InvalidObjectError(setup.Commits.Second.OID),
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "pack file with deletions",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file fails to be committed with dependency concurrently deleted",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
				AssertManager{},
				Prune{},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/dependant": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
					ExpectedError:    localrepo.InvalidObjectError(setup.Commits.First.OID),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "files of interrupted log commits are cleaned up after a crash",
			steps: steps{
				StartManager{
					// The manager cleans up pack files if a committing fails. Since we can't
					// hard kill the manager and it will still run the deferred clean up functions,
					// we have to test the behavior by manually creating a stale pack here.
					//
					// The Manager starts up and we expect the pack file to be gone at the end of the test.
					ModifyStorage: func(_ testing.TB, _ config.Cfg, storagePath string) {
						packFilePath := packFilePath(log.EntryPath(filepath.Join(storagePath, setup.RelativePath), 1))
						require.NoError(t, os.MkdirAll(filepath.Dir(packFilePath), mode.Directory))
						require.NoError(t, os.WriteFile(
							packFilePath,
							[]byte("invalid pack"),
							mode.Directory,
						))
					},
				},
			},
		},
		{
			desc: "non-existent repository is correctly handled",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				RepositoryAssertion{
					TransactionID: 1,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 1,
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "failing initialization prevents transaction beginning",
			steps: steps{
				StartManager{
					Hooks: testTransactionHooks{
						BeforeReadAppliedLSN: func(hookContext) {
							// Raise a panic when the manager is about to read the applied log
							// index when initializing. In reality this would crash the server but
							// in tests it serves as a way to abort the initialization in correct
							// location.
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					RelativePaths: []string{setup.RelativePath},
					ExpectedError: errInitializationFailed,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
			},
		},
		{
			desc: "transaction rollbacked after already being rollbacked",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Rollback{},
				Rollback{
					ExpectedError: storage.ErrTransactionAlreadyRollbacked,
				},
			},
		},
		{
			desc: "transaction rollbacked after already being committed",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{},
				Rollback{
					ExpectedError: storage.ErrTransactionAlreadyCommitted,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
			},
		},
		{
			desc: "transaction committed after already being committed",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{},
				Commit{
					ExpectedError: storage.ErrTransactionAlreadyCommitted,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
			},
		},
		{
			desc: "transaction committed after already being rollbacked",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Rollback{},
				Commit{
					ExpectedError: storage.ErrTransactionAlreadyRollbacked,
				},
			},
		},
		{
			desc: "read-only transaction doesn't commit a log entry",
			steps: steps{
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
					ReadOnly:      true,
				},
				Commit{},
			},
		},
		{
			desc: "transactions are snapshot isolated from concurrent updates",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/new-head",
					},
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				// This transaction was started before the commit, so it should see the original state.
				RepositoryAssertion{
					TransactionID: 1,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/main",
						},
					},
				},
				// This transaction was started after the commit, so it should see the new state.
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/new-head",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/main": setup.Commits.First.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 1,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
														IsSymbolic: true,
													},
												},
											},
											{
												MinIndex: 2,
												MaxIndex: 3,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/new-head",
														IsSymbolic: true,
													},
													{
														Name:   "refs/heads/main",
														Target: setup.Commits.First.OID.String(),
													},
												},
											},
										},
									},
								},
							),
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: mode.Directory},
								"/pre-receive": {
									Mode:    mode.Executable,
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: mode.Directory},
								"/private-dir/private-file": {Mode: mode.File, Content: []byte("private content")},
							},
						},
					},
				},
				Rollback{
					TransactionID: 1,
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/new-head",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/new-head",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: mode.Directory},
							"/pre-receive": {
								Mode:    mode.Executable,
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: mode.Directory},
							"/private-dir/private-file": {Mode: mode.File, Content: []byte("private content")},
						},
					},
				},
			},
		},
	}
}

type expectedCommittedEntry struct {
	lsn             storage.LSN
	snapshotReaders int
	entry           *gitalypb.LogEntry
}

func generateCommittedEntriesTests(t *testing.T, setup testTransactionSetup) []transactionTestCase {
	assertCommittedEntries := func(t *testing.T, manager *TransactionManager, expected []*expectedCommittedEntry, actualList *list.List) {
		require.Equal(t, len(expected), actualList.Len())

		i := 0
		for elm := actualList.Front(); elm != nil; elm = elm.Next() {
			actual := elm.Value.(*committedEntry)
			require.Equal(t, expected[i].lsn, actual.lsn)
			require.Equal(t, expected[i].snapshotReaders, actual.snapshotReaders)

			if expected[i].entry != nil {
				expectedEntry := expected[i].entry

				if testhelper.IsReftableEnabled() {
					for idx, op := range expectedEntry.GetOperations() {
						if chl := op.GetCreateHardLink(); chl != nil {
							actualCHL := actual.entry.GetOperations()[idx].GetCreateHardLink()
							require.NotNil(t, actualCHL)

							if filepath.Base(string(actualCHL.GetDestinationPath())) == "tables.list" {
								continue
							}

							// We can't predict the table names, but we can verify
							// the regex.
							require.True(t, git.ReftableTableNameRegex.Match(actualCHL.GetDestinationPath()))
							chl.DestinationPath = actualCHL.GetDestinationPath()
						}
					}
				}

				testhelper.ProtoEqual(t, expectedEntry, actual.entry)
			}
			i++
		}
	}

	return []transactionTestCase{
		{
			desc: "manager has just initialized",
			steps: steps{
				StartManager{},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
			},
		},
		{
			desc: "a transaction has one reader",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             0,
							snapshotReaders: 1,
						},
					}, tm.committedEntries)
				}),
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-1": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             1,
							snapshotReaders: 1,
						},
					}, tm.committedEntries)
				}),
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main":     setup.Commits.First.OID,
										"refs/heads/branch-1": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/branch-1",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc: "a transaction has multiple readers",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             1,
							snapshotReaders: 2,
						},
					}, tm.committedEntries)
				}),
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-1": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             1,
							snapshotReaders: 1,
						},
						{
							lsn:   2,
							entry: refChangeLogEntry(setup, "refs/heads/branch-1", setup.Commits.First.OID),
						},
					}, tm.committedEntries)
				}),
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             1,
							snapshotReaders: 1,
						},
						{
							lsn:             2,
							snapshotReaders: 1,
							entry:           refChangeLogEntry(setup, "refs/heads/branch-1", setup.Commits.First.OID),
						},
					}, tm.committedEntries)
				}),
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{
						{
							lsn:             2,
							entry:           refChangeLogEntry(setup, "refs/heads/branch-1", setup.Commits.First.OID),
							snapshotReaders: 1,
						},
						{
							lsn:   3,
							entry: refChangeLogEntry(setup, "refs/heads/branch-2", setup.Commits.First.OID),
						},
					}, tm.committedEntries)
				}),
				Rollback{
					TransactionID: 4,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main":     setup.Commits.First.OID,
										"refs/heads/branch-1": setup.Commits.First.OID,
										"refs/heads/branch-2": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/branch-1",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 4,
											MaxIndex: 4,
											References: []git.Reference{
												{
													Name:   "refs/heads/branch-2",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc: "committed read-only transaction are not kept",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
					ReadOnly:      true,
				},
				Commit{
					TransactionID: 1,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
				Begin{
					TransactionID: 2,
					RelativePaths: []string{setup.RelativePath},
					ReadOnly:      true,
				},
				Commit{
					TransactionID: 2,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					assertCommittedEntries(t, tm, []*expectedCommittedEntry{}, tm.committedEntries)
				}),
			},
			expectedState: StateAssertion{
				Database: DatabaseState{},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
					},
				},
			},
		},
		{
			desc: "transaction manager cleans up left-over committed entries when appliedLSN == appendedLSN",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-1": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				CloseManager{},
				Commit{
					TransactionID: 4,
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					RequireDatabase(t, ctx, tm.db, DatabaseState{
						string(keyAppliedLSN): storage.LSN(3).ToProto(),
					})
					// Transaction 2 and 3 are left-over.
					testhelper.RequireDirectoryState(t, tm.logManager.StateDirectory(), "",
						testhelper.DirectoryState{
							"/":    {Mode: mode.Directory},
							"/wal": {Mode: mode.Directory},
						},
					)
				}),
				StartManager{},
				AssertManager{},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					// When the manager finishes initialization, the left-over log entries are
					// cleaned up.
					RequireDatabase(t, ctx, tm.db, DatabaseState{
						string(keyAppliedLSN): storage.LSN(3).ToProto(),
					})
					require.Equal(t, tm.appliedLSN, storage.LSN(3))
					require.Equal(t, tm.logManager.AppendedLSN(), storage.LSN(3))
				}),
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main":     setup.Commits.First.OID,
										"refs/heads/branch-1": setup.Commits.First.OID,
										"refs/heads/branch-2": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/branch-1",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 4,
											MaxIndex: 4,
											References: []git.Reference{
												{
													Name:   "refs/heads/branch-2",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc: "transaction manager cleans up left-over committed entries when appliedLSN < appendedLSN",
			skip: func(t *testing.T) {
				testhelper.SkipWithReftable(t, "test requires manual log addition")
			},
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-1": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch-2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				CloseManager{},
				Commit{
					TransactionID: 4,
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					// Insert an out-of-band log-entry directly into the database for easier test
					// setup. It's a bit tricky to simulate committed log entries and un-processed
					// appended log entries at the same time.
					logEntryPath := filepath.Join(t.TempDir(), "log_entry")
					require.NoError(t, os.Mkdir(logEntryPath, mode.Directory))
					require.NoError(t, os.WriteFile(filepath.Join(logEntryPath, "1"), []byte(setup.Commits.First.OID+"\n"), mode.File))
					expectedManifest := manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/branch-3", setup.Commits.First.OID))
					manifestBytes, err := proto.Marshal(expectedManifest.Content.(proto.Message))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(manifestPath(logEntryPath), manifestBytes, mode.File))
					require.NoError(t, tm.appendLogEntry(ctx, map[git.ObjectID]struct{}{setup.Commits.First.OID: {}}, nil, logEntryPath))

					RequireDatabase(t, ctx, tm.db, DatabaseState{
						string(keyAppliedLSN): storage.LSN(3).ToProto(),
					})
					// Transaction 2 and 3 are left-over.
					testhelper.RequireDirectoryState(t, tm.logManager.StateDirectory(), "", testhelper.DirectoryState{
						"/":                           {Mode: mode.Directory},
						"/wal":                        {Mode: mode.Directory},
						"/wal/0000000000004":          {Mode: mode.Directory},
						"/wal/0000000000004/MANIFEST": expectedManifest,
						"/wal/0000000000004/1":        {Mode: mode.File, Content: []byte(setup.Commits.First.OID + "\n")},
					})
				}),
				StartManager{},
				AssertManager{},
				AdhocAssertion(func(t *testing.T, ctx context.Context, tm *TransactionManager) {
					// When the manager finishes initialization, the left-over log entries are
					// cleaned up.
					RequireDatabase(t, ctx, tm.db, DatabaseState{
						string(keyAppliedLSN): storage.LSN(4).ToProto(),
					})
					require.Equal(t, tm.appliedLSN, storage.LSN(4))
					require.Equal(t, tm.logManager.AppendedLSN(), storage.LSN(4))
				}),
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(4).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: &ReferencesState{
							FilesBackend: &FilesBackendState{
								LooseReferences: map[git.ReferenceName]git.ObjectID{
									"refs/heads/main":     setup.Commits.First.OID,
									"refs/heads/branch-1": setup.Commits.First.OID,
									"refs/heads/branch-2": setup.Commits.First.OID,
									"refs/heads/branch-3": setup.Commits.First.OID,
								},
							},
						},
					},
				},
			},
		},
	}
}

// BenchmarkTransactionManager benchmarks the transaction throughput of the TransactionManager at various levels
// of concurrency and transaction sizes.
func BenchmarkTransactionManager(b *testing.B) {
	for _, tc := range []struct {
		// numberOfRepositories sets the number of repositories that are updating the references. Each repository has
		// its own TransactionManager. Setting this to 1 allows for testing throughput of a single repository while
		// setting this higher allows for testing parallel throughput of multiple repositories. This mostly serves
		// to determine the impact of the shared resources such as the database.
		numberOfRepositories int
		// concurrentUpdaters sets the number of goroutines that are calling committing transactions for a repository.
		// Each of the updaters work on their own references so they don't block each other. Setting this to 1 allows
		// for testing sequential update throughput of a repository. Setting this higher allows for testing reference
		// update throughput when multiple references are being updated concurrently.
		concurrentUpdaters int
		// transactionSize sets the number of references that are updated in each transaction.
		transactionSize int
	}{
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   1,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   10,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 10,
			concurrentUpdaters:   1,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   1,
			transactionSize:      10,
		},
		{
			numberOfRepositories: 10,
			concurrentUpdaters:   1,
			transactionSize:      10,
		},
	} {
		desc := fmt.Sprintf("%d repositories/%d updaters/%d transaction size",
			tc.numberOfRepositories,
			tc.concurrentUpdaters,
			tc.transactionSize,
		)
		b.Run(desc, func(b *testing.B) {
			ctx := testhelper.Context(b)

			cfg := testcfg.Build(b)
			logger := testhelper.NewLogger(b)

			cmdFactory := gittest.NewCommandFactory(b, cfg)
			cache := catfile.NewCache(cfg)
			defer cache.Stop()

			database, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(b), b.TempDir())
			require.NoError(b, err)
			defer testhelper.MustClose(b, database)

			var (
				// managerWG records the running TransactionManager.Run goroutines.
				managerWG sync.WaitGroup
				managers  []*TransactionManager
			)

			repositoryFactory, err := localrepo.NewFactory(
				logger, config.NewLocator(cfg), cmdFactory, cache,
			).ScopeByStorage(ctx, cfg.Storages[0].Name)
			require.NoError(b, err)

			// transactionWG tracks the number of on going transaction.
			var transactionWG sync.WaitGroup
			transactionChan := make(chan struct{})

			// Set up the repositories and start their TransactionManagers.
			for i := 0; i < tc.numberOfRepositories; i++ {
				repo, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				storageName := cfg.Storages[0].Name
				storagePath := cfg.Storages[0].Path

				stateDir := filepath.Join(storagePath, "state", strconv.Itoa(i))
				require.NoError(b, os.MkdirAll(stateDir, mode.Directory))

				stagingDir := filepath.Join(storagePath, "staging", strconv.Itoa(i))
				require.NoError(b, os.MkdirAll(stagingDir, mode.Directory))

				m := NewMetrics(housekeeping.NewMetrics(cfg.Prometheus)).Scope(storageName)

				// Valid partition IDs are >=1.
				testPartitionID := storage.PartitionID(i + 1)
				manager := NewTransactionManager(testPartitionID, logger, database, storageName, storagePath, stateDir, stagingDir, cmdFactory, repositoryFactory, m, nil)

				managers = append(managers, manager)

				managerWG.Add(1)
				go func() {
					defer managerWG.Done()
					assert.NoError(b, manager.Run())
				}()

				objectHash, err := repositoryFactory.Build(repo.GetRelativePath()).ObjectHash(ctx)
				require.NoError(b, err)

				for updaterID := 0; updaterID < tc.concurrentUpdaters; updaterID++ {
					// Build the reference updates that this updater will go back and forth with.
					initialReferenceUpdates := make(git.ReferenceUpdates, tc.transactionSize)
					updateA := make(git.ReferenceUpdates, tc.transactionSize)
					updateB := make(git.ReferenceUpdates, tc.transactionSize)

					// Set up a commit pair for each reference that the updater changes updates back
					// and forth. The commit IDs are unique for each reference in a repository..
					for branchID := 0; branchID < tc.transactionSize; branchID++ {
						commit1 := gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents(), gittest.WithMessage(fmt.Sprintf("updater-%d-reference-%d", updaterID, branchID)))
						commit2 := gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents(commit1))

						ref := git.ReferenceName(fmt.Sprintf("refs/heads/updater-%d-branch-%d", updaterID, branchID))
						initialReferenceUpdates[ref] = git.ReferenceUpdate{
							OldOID: objectHash.ZeroOID,
							NewOID: commit1,
						}

						updateA[ref] = git.ReferenceUpdate{
							OldOID: commit1,
							NewOID: commit2,
						}

						updateB[ref] = git.ReferenceUpdate{
							OldOID: commit2,
							NewOID: commit1,
						}
					}

					// Setup the starting state so the references start at the expected old tip.
					transaction, err := manager.Begin(ctx, storage.BeginOptions{
						Write:         true,
						RelativePaths: []string{repo.GetRelativePath()},
					})
					require.NoError(b, err)
					require.NoError(b, performReferenceUpdates(b, ctx,
						transaction,
						localrepo.New(logger, config.NewLocator(cfg), cmdFactory, cache, transaction.RewriteRepository(repo)),
						initialReferenceUpdates,
					))
					require.NoError(b, transaction.UpdateReferences(ctx, initialReferenceUpdates))
					require.NoError(b, transaction.Commit(ctx))

					transactionWG.Add(1)
					go func() {
						defer transactionWG.Done()

						for range transactionChan {
							transaction, err := manager.Begin(ctx, storage.BeginOptions{
								Write:         true,
								RelativePaths: []string{repo.GetRelativePath()},
							})
							require.NoError(b, err)
							require.NoError(b, performReferenceUpdates(b, ctx,
								transaction,
								localrepo.New(logger, config.NewLocator(cfg), cmdFactory, cache, transaction.RewriteRepository(repo)),
								updateA,
							))
							require.NoError(b, transaction.UpdateReferences(ctx, updateA))
							assert.NoError(b, transaction.Commit(ctx))
							updateA, updateB = updateB, updateA
						}
					}()
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			began := time.Now()
			for n := 0; n < b.N; n++ {
				transactionChan <- struct{}{}
			}
			close(transactionChan)

			transactionWG.Wait()
			b.StopTimer()

			b.ReportMetric(float64(b.N)/time.Since(began).Seconds(), "tx/s")

			for _, manager := range managers {
				manager.Close()
			}

			managerWG.Wait()
		})
	}
}
