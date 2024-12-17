package partition

import (
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/conflict/fshistory"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func generateCreateRepositoryTests(t *testing.T, setup testTransactionSetup) []transactionTestCase {
	return []transactionTestCase{
		{
			desc: "create repository when it doesn't exist",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				CreateRepository{},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "create repository when it already exists",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{setup.RelativePath},
				},
				CreateRepository{
					TransactionID: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
					ExpectedError: ErrRepositoryAlreadyExists,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "create repository with full state",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				CreateRepository{
					DefaultBranch: "refs/heads/branch",
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main":   setup.Commits.First.OID,
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack, setup.Commits.Second.Pack},
					CustomHooks: validCustomHooks(t),
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/branch",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main":   setup.Commits.First.OID,
										"refs/heads/branch": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 4,
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
		{
			desc: "transactions are snapshot isolated from concurrent creations",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
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
					RelativePaths:       []string{setup.RelativePath},
					ReadOnly:            true,
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    3,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 4,
					DefaultBranch: "refs/heads/other",
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/other": setup.Commits.Second.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack, setup.Commits.Second.Pack},
				},
				Commit{
					TransactionID: 4,
				},
				// Transaction 2 has been open through out the repository deletion and creation. It should
				// still see the original state of the repository before the deletion.
				RepositoryAssertion{
					TransactionID: 2,
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
								"/": {Mode: snapshot.ModeReadOnlyDirectory},
								"/pre-receive": {
									Mode:    mode.Executable,
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: snapshot.ModeReadOnlyDirectory},
								"/private-dir/private-file": {Mode: mode.File, Content: []byte("private content")},
							},
						},
					},
				},
				Begin{
					TransactionID:       5,
					RelativePaths:       []string{setup.RelativePath},
					ReadOnly:            true,
					ExpectedSnapshotLSN: 3,
				},
				RepositoryAssertion{
					TransactionID: 5,
					Repositories: RepositoryStates{
						setup.RelativePath: {
							DefaultBranch: "refs/heads/other",
							References: gittest.FilesOrReftables(
								&ReferencesState{
									FilesBackend: &FilesBackendState{
										LooseReferences: map[git.ReferenceName]git.ObjectID{
											"refs/heads/other": setup.Commits.Second.OID,
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
														Target:     "refs/heads/other",
														IsSymbolic: true,
													},
													{
														Name:   "refs/heads/other",
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
				Rollback{
					TransactionID: 2,
				},
				Rollback{
					TransactionID: 5,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/other",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/other": setup.Commits.Second.OID,
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
													Target:     "refs/heads/other",
													IsSymbolic: true,
												},
												{
													Name:   "refs/heads/other",
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
			desc: "logged repository creation is respected",
			steps: steps{
				RemoveRepository{},
				StartManager{
					Hooks: testTransactionHooks{
						BeforeApplyLogEntry: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reapplying repository creation works",
			steps: steps{
				RemoveRepository{},
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
				CreateRepository{
					TransactionID: 1,
					DefaultBranch: "refs/heads/branch",
					Packs:         [][]byte{setup.Commits.First.Pack},
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					CustomHooks: validCustomHooks(t),
				},
				Commit{
					TransactionID: 1,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/branch",
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
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/branch",
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
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: mode.Directory},
							"/pre-receive": {
								Mode:    mode.Executable,
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: mode.Directory},
							"/private-dir/private-file": {Mode: mode.File, Content: []byte("private content")},
						},
						Objects: []git.ObjectID{
							setup.Commits.First.OID,
							setup.ObjectHash.EmptyTreeOID,
						},
					},
				},
			},
		},
		{
			desc: "commit without creating a repository",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "two repositories created in different transactions",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"repository-1"},
				},
				Begin{
					TransactionID: 2,
					RelativePaths: []string{"repository-2"},
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack},
					CustomHooks: validCustomHooks(t),
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
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("repository-1")): string(""),
					"kv/" + string(storage.RepositoryKey("repository-2")): string(""),
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
				},
			},
		},
		{
			desc: "begin transaction on with all repositories",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{"repository-1"},
				},
				CreateRepository{
					TransactionID: 1,
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
				},
				Commit{
					TransactionID: 2,
				},
				// Start a transaction on all repositories.
				Begin{
					TransactionID:       3,
					RelativePaths:       nil,
					ReadOnly:            true,
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"repository-1": {
							DefaultBranch: "refs/heads/main",
						},
						"repository-2": {
							DefaultBranch: "refs/heads/main",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
					"kv/" + string(storage.RepositoryKey("repository-1")): string(""),
					"kv/" + string(storage.RepositoryKey("repository-2")): string(""),
				},
				Repositories: RepositoryStates{
					// The setup repository does not have its relative path in the partition KV.
					setup.RelativePath: {},
					"repository-1": {
						Objects: []git.ObjectID{},
					},
					"repository-2": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "starting a writing transaction with all repositories is an error",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					ReadOnly:      false,
					RelativePaths: nil,
					ExpectedError: errWritableAllRepository,
				},
			},
		},
	}
}

func generateDeleteRepositoryTests(t *testing.T, setup testTransactionSetup) []transactionTestCase {
	return []transactionTestCase{
		{
			desc: "repository is successfully deleted",
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
				RunPackRefs{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:    3,
					DeleteRepository: true,
					ExpectedError:    nil,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "repository deletion fails if repository is deleted",
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
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
					ExpectedError:    storage.ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "custom hooks update fails if repository is deleted",
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
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{CustomHooksTAR: validCustomHooks(t)},
					ExpectedError:     storage.ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reference updates fail if repository is deleted",
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
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: storage.ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "default branch update fails if repository is deleted",
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
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/new-default",
					},
					ExpectedError: storage.ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "logged repository deletions are considered after restart",
			steps: steps{
				StartManager{
					Hooks: testTransactionHooks{
						BeforeApplyLogEntry: func(hookContext) {
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
					TransactionID:    1,
					DeleteRepository: true,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reapplying repository deletion works",
			steps: steps{
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
					TransactionID:    1,
					DeleteRepository: true,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "deletion fails with concurrent write to an existing file in repository",
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
				Commit{
					TransactionID: 1,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch",
					},
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
					ExpectedError: gittest.FilesOrReftables[error](
						fshistory.NewReadWriteConflictError(
							// The deletion fails on the new file only because the deletions are currently
							filepath.Join(setup.RelativePath, "HEAD"), 0, 1,
						),
						fshistory.DirectoryNotEmptyError{
							// Conflicts on the `tables.list` file are ignored with reftables as reference
							// write conflicts with it. We see the conflict here as reftable directory not
							// being empty due to the new table written into it.
							Path: filepath.Join(setup.RelativePath, "reftable"),
						},
					),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/branch",
					},
				},
			},
		},
		{
			desc: "deletion fails with concurrent write introducing files in repository",
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
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/branch": {NewOID: setup.Commits.First.OID},
					},
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
					ExpectedError: gittest.FilesOrReftables(
						fshistory.DirectoryNotEmptyError{
							// The deletion fails on `refs/heads` directory as it is no longer empty
							// due to the concurrent branch write.
							Path: filepath.Join(setup.RelativePath, "refs", "heads"),
						},
						fshistory.DirectoryNotEmptyError{
							// Conflicts on the `tables.list` file are ignored with reftables as reference
							// write conflicts with it. We see the conflict here as reftable directory not
							// being empty due to the new table written into it.
							Path: filepath.Join(setup.RelativePath, "reftable"),
						},
					),
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/branch": setup.Commits.First.OID,
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
													Name:   "refs/heads/branch",
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
			desc: "deletion waits until other transactions are done",
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
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				// The concurrent transaction should be able to read the
				// repository despite the committed deletion.
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
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "transactions are snapshot isolated from concurrent deletions",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
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
					TransactionID:    2,
					DeleteRepository: true,
				},
				// This transaction was started before the deletion, so it should see the old state regardless
				// of the repository being deleted.
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
					TransactionID: 3,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 4,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 4,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "create repository again after deletion",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
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
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:    3,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 3,
				},
				CreateRepository{
					TransactionID: 4,
				},
				Commit{
					TransactionID: 4,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(4).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "writes concurrent with repository deletion fail",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
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

				// Start a write. During the write, the repository is deleted
				// and recreated. We expect this write to fail.
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},

				// Delete the repository concurrently.
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    4,
					DeleteRepository: true,
				},

				// Recreate the repository concurrently.
				Begin{
					TransactionID:       5,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 5,
				},
				Commit{
					TransactionID: 5,
				},

				// Commit the write that ran during which the repository was concurrently recreated. This
				// should lead to a conflict.
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					ExpectedError: conflict.ErrRepositoryConcurrentlyDeleted,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
					"kv/" + string(storage.RepositoryKey(setup.RelativePath)): string(""),
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
	}
}
