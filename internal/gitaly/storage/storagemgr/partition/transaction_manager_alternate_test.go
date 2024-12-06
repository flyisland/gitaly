package partition

import (
	"bytes"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict/fshistory"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func generateAlternateTests(t *testing.T, setup testTransactionSetup) []transactionTestCase {
	return []transactionTestCase{
		{
			desc: "repository is linked to alternate on creation",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository is created with dependencies in alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
					Packs:         [][]byte{setup.Commits.Second.Pack},
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/new-branch": setup.Commits.Second.OID,
					},
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
						Packfiles: &PackfilesState{
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.ObjectHash.EmptyTreeOID,
										setup.Commits.First.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
					},
					"member": {
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/new-branch": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/new-branch",
													Target: setup.Commits.Second.OID.String(),
												},
											},
										},
									},
								},
							},
						),
						Packfiles: &PackfilesState{
							PooledObjects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.Commits.Second.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository is linked to an alternate after creation",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{RelativePath: "pool"},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository is disconnected from alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				CloseManager{},
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						// Transactions write objects always as packs into the repository. To test
						// scenarios where repositories may have existing loose objects, manually
						// unpack the objects to the repository.
						gittest.ExecOpts(tb, cfg,
							gittest.ExecConfig{Stdin: bytes.NewReader(setup.Commits.Second.Pack)},
							"-C", filepath.Join(storagePath, "pool"), "unpack-objects",
						)
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
						Packfiles: &PackfilesState{
							LooseObjects: []git.ObjectID{
								setup.Commits.Second.OID,
							},
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.ObjectHash.EmptyTreeOID,
										setup.Commits.First.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
					},
					"member": {
						// The objects should have been copied over to the repository when it was
						// disconnected from the alternate.
						Packfiles: &PackfilesState{
							LooseObjects: []git.ObjectID{
								setup.Commits.Second.OID,
							},
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.ObjectHash.EmptyTreeOID,
										setup.Commits.First.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "repository is disconnected from alternate concurrently with housekeeping",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				CloseManager{},
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						// Transactions write objects always as packs into the repository. To test
						// scenarios where repositories may have existing loose objects, manually
						// unpack the objects to the repository.
						gittest.ExecOpts(tb, cfg,
							gittest.ExecConfig{Stdin: bytes.NewReader(setup.Commits.Second.Pack)},
							"-C", filepath.Join(storagePath, "pool"), "unpack-objects",
						)
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       5,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 5,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
				RunRepack{
					TransactionID: 3,
					Config: housekeepingcfg.RepackObjectsConfig{
						Strategy: housekeepingcfg.RepackObjectsStrategyFullWithUnreachable,
					},
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{},
				},
				Commit{
					TransactionID: 3,
					ExpectedError: errConcurrentAlternateUnlink,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(4).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
						Packfiles: &PackfilesState{
							LooseObjects: []git.ObjectID{
								setup.Commits.Second.OID,
							},
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.ObjectHash.EmptyTreeOID,
										setup.Commits.First.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
					},
					"member": {
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
						// The objects should have been copied over to the repository when it was
						// disconnected from the alternate.
						Packfiles: &PackfilesState{
							LooseObjects: []git.ObjectID{
								setup.Commits.Second.OID,
							},
							Packfiles: []*PackfileState{
								{
									Objects: []git.ObjectID{
										setup.ObjectHash.EmptyTreeOID,
										setup.Commits.First.OID,
									},
									HasReverseIndex: true,
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "repository can't be linked concurrently multiple times",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{RelativePath: "pool"},
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{RelativePath: "pool"},
					ExpectedError: fshistory.NewReadWriteConflictError(
						filepath.Join("member", "objects", "info", "alternates"), 2, 3,
					),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository without an alternate can't be disconnected",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"repository"},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"repository"},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:   2,
					UpdateAlternate: &alternateUpdate{},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                               storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("repository")): string(""),
				},
				Repositories: RepositoryStates{
					"repository": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "repository can't be disconnected concurrently multiple times",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{},
					ExpectedError: fshistory.NewReadWriteConflictError(
						filepath.Join("member", "objects", "info", "alternates"), 2, 3,
					),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "reapplying alternate linking works",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				CloseManager{},
				StartManager{
					Hooks: testTransactionHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"member": {
							DefaultBranch: "refs/heads/main",
						},
						"pool": {
							DefaultBranch: "refs/heads/main",
						},
					},
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{RelativePath: "pool"},
					ExpectedError:   storage.ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "reapplying alternate disconnection works",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				CloseManager{},
				StartManager{
					Hooks: testTransactionHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
							DefaultBranch: "refs/heads/main",
						},
						"member": {
							DefaultBranch: "refs/heads/main",
							Alternate:     "../../pool/objects",
						},
					},
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
					ExpectedError:   storage.ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "point reference to an object in an alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
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
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "point reference to new object with dependencies in an alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
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
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository's alternate is automatically snapshotted",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"member"},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
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
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
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
						},
						"member": {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "multiple repositories can be included in transaction's snapshot",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"repository-1"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack},
					CustomHooks: validCustomHooks(t),
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"repository-2"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Third.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Third.Pack,
					},
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{"repository-3", "repository-2"},
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 3,
					// Set repository-2 as repository-3's alternate to assert the
					// snasphotted repositories' alternates are also included.
					Alternate: "repository-2",
				},
				Commit{
					TransactionID: 3,
				},
				Begin{
					TransactionID: 4,
					// Create a repository that is not snapshotted to assert it's not included
					// in the snapshot.
					RelativePaths:       []string{"repository-4"},
					ExpectedSnapshotLSN: 3,
				},
				CreateRepository{
					TransactionID: 4,
				},
				Commit{
					TransactionID: 4,
				},
				Begin{
					TransactionID:       5,
					RelativePaths:       []string{"repository-1", "repository-3"},
					ExpectedSnapshotLSN: 4,
				},
				RepositoryAssertion{
					TransactionID: 5,
					Repositories: RepositoryStates{
						"repository-1": {
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
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
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
						"repository-2": {
							DefaultBranch: "refs/heads/branch",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/branch": setup.Commits.Third.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 3,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/branch",
														IsSymbolic: true,
													},
													{
														Name:   "refs/heads/branch",
														Target: setup.Commits.Third.OID.String(),
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
							},
						},
						"repository-3": {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
							},
							Alternate: "../../repository-2/objects",
						},
					},
				},
				Rollback{
					TransactionID: 5,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(4).ToProto(),
					"kv/" + string(storage.RepositoryKey("repository-1")): string(""),
					"kv/" + string(storage.RepositoryKey("repository-2")): string(""),
					"kv/" + string(storage.RepositoryKey("repository-3")): string(""),
					"kv/" + string(storage.RepositoryKey("repository-4")): string(""),
				},
				Repositories: RepositoryStates{
					"repository-1": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					"repository-2": {
						DefaultBranch: "refs/heads/branch",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/branch": setup.Commits.Third.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/branch",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/branch",
													Target: setup.Commits.Third.OID.String(),
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
						},
					},
					"repository-3": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
						Alternate: "../../repository-2/objects",
					},
					"repository-4": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "additional repository is included in the snapshot explicitly and implicitly",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
					},
					Alternate: "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID: 3,
					// The pool is included explicitly here, and also implicitly through
					// the alternate link of member.
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
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
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
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
						},
						"member": {
							DefaultBranch: "refs/heads/branch",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/branch": setup.Commits.Second.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 3,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/branch",
														IsSymbolic: true,
													},
													{
														Name:   "refs/heads/branch",
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
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
						DefaultBranch: "refs/heads/branch",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/branch": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/branch",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/branch",
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
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "target repository is included in the snapshot explicitly and implicitly",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"pool"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{"member", "pool"},
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
					},
					Alternate: "pool",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID: 3,
					// The pool is targeted, and also implicitly included through
					// the alternate link of member.
					RelativePaths:       []string{"pool", "member"},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
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
												MaxIndex: 2,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/main",
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
						},
						"member": {
							DefaultBranch: "refs/heads/branch",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/branch": setup.Commits.Second.OID,
										},
									},
								}, &ReferencesState{
									ReftableBackend: &ReftableBackendState{
										Tables: []ReftableTable{
											{
												MinIndex: 1,
												MaxIndex: 3,
												References: []git.Reference{
													{
														Name:       "HEAD",
														Target:     "refs/heads/branch",
														IsSymbolic: true,
													},
													{
														Name:   "refs/heads/branch",
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
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN):                           storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("pool")):   string(""),
					"kv/" + string(storage.RepositoryKey("member")): string(""),
				},
				Repositories: RepositoryStates{
					"pool": {
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
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
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
					},
					"member": {
						DefaultBranch: "refs/heads/branch",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/branch": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/branch",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/branch",
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
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "non-git directories are not snapshotted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					// Try to snapshot the parent directory, which is no a valid Git directory.
					RelativePaths: []string{filepath.Dir(setup.RelativePath)},
					ExpectedError: storage.InvalidGitDirectoryError{MissingEntry: "objects"},
				},
			},
		},
	}
}
