package refdb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

func TestTree(t *testing.T) {
	zeroOID := gittest.DefaultObjectHash.ZeroOID

	newReference := func(target git.ObjectID) *node {
		return &node{
			target:   target.String(),
			children: children{},
		}
	}

	newSymbolicReference := func(target git.ReferenceName) *node {
		return &node{
			target:   target.String(),
			children: children{},
		}
	}

	t.Run("discarded uncommitted changes are discarded", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx1 := history.Begin()
		require.NoError(t, tx1.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		tx1.Commit(1)

		// We leave this transaction uncommitted and assert the history
		// is unchanged.
		tx2 := history.Begin()
		require.NoError(t, tx2.ApplyUpdates(git.ReferenceUpdates{
			"HEAD":               {NewTarget: "refs/heads/main"},
			"refs/heads/main":    {OldOID: "oid-1", NewOID: "oid-2"},
			"refs/heads/deleted": {NewOID: zeroOID},
		}))

		require.Equal(t, &node{
			childReferences: 2,
			children: children{
				"HEAD": newSymbolicReference("refs/heads/main"),
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main":    newReference("oid-2"),
								"deleted": newReference(zeroOID),
							},
						},
					},
				},
			},
		}, tx2.root)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main": newReference("oid-1"),
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("default branch updated", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"HEAD": {NewTarget: "refs/heads/new-default"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"HEAD": newReference("refs/heads/new-default"),
			},
		}, history.root)
	})

	t.Run("non-conflicting default branch updates", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"HEAD": {NewTarget: "refs/heads/new-default-1"},
		}))

		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"HEAD": {OldTarget: "refs/heads/new-default-1", NewTarget: "refs/heads/new-default-2"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"HEAD": newReference("refs/heads/new-default-2"),
			},
		}, history.root)
	})

	t.Run("conflicting default branch updates", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"HEAD": {NewTarget: "refs/heads/new-default-1"},
		}))

		require.Equal(t,
			NewUnexpectedOldValueError("HEAD", "refs/heads/main", "refs/heads/new-default-1"),
			tx.ApplyUpdates(git.ReferenceUpdates{
				"HEAD": {OldTarget: "refs/heads/main", NewTarget: "refs/heads/new-default-2"},
			}))
	})

	t.Run("parent references exist", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))

		require.Equal(t,
			NewParentReferenceExistsError("refs/heads/main", "refs/heads/main/child"),
			tx.ApplyUpdates(git.ReferenceUpdates{
				"refs/heads/main/child": {NewOID: "oid-1"},
			}))
	})

	t.Run("child references exist", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main/child": {NewOID: "oid-1"},
		}))

		require.Equal(t,
			NewChildReferencesExistError("refs/heads/main"),
			tx.ApplyUpdates(git.ReferenceUpdates{
				"refs/heads/main": {NewOID: "oid-1"},
			}))
	})

	t.Run("resolved directory-file conflict in history", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {OldOID: "oid-1", NewOID: zeroOID},
		}))
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main/child": {NewOID: "oid-2"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main": {
									childReferences: 1,
									target:          zeroOID.String(),
									children: children{
										"child": newReference("oid-2"),
									},
								},
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("deletions are kept regardless of parent writes", func(t *testing.T) {
		history := NewHistory(zeroOID)

		// Child reference is first deleted.
		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main/child": {NewOID: zeroOID},
		}))
		tx.Commit(1)

		// And then a parent reference is created.
		tx = history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		tx.Commit(2)

		// We expect the history to contain a record of the deletion regardless
		// of `refs/heads/main` existing above the child reference.
		require.Equal(t,
			&History{
				zeroOID: zeroOID,
				lsnByReference: map[git.ReferenceName]storage.LSN{
					"refs/heads/main/child": 1,
					"refs/heads/main":       2,
				},
				referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{
					1: {"refs/heads/main/child": {}},
					2: {"refs/heads/main": {}},
				},
				root: &node{
					childReferences: 1,
					children: children{
						"refs": {
							childReferences: 1,
							children: children{
								"heads": {
									childReferences: 1,
									children: children{
										"main": {
											target: "oid-1",
											children: children{
												"child": newReference(zeroOID),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			history,
		)

		// Delete the parent reference again.
		tx = history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {OldOID: "oid-1", NewOID: zeroOID},
		}))
		tx.Commit(3)

		// A subsequent transaction should still consider the recorded
		// deletion. The node recording the deletion `refs/heads/main/child`
		// should not be removed even if we created a reference above it at
		// `refs/heads/main` and subsequently deleted `refs/heads/main` again.
		tx = history.Begin()
		require.Equal(t,
			NewUnexpectedOldValueError("refs/heads/main/child", "oid-1", zeroOID.String()),
			tx.ApplyUpdates(git.ReferenceUpdates{
				"refs/heads/main/child": {OldOID: "oid-1", NewOID: "oid-2"},
			}))

		require.Equal(t,
			&History{
				zeroOID: zeroOID,
				lsnByReference: map[git.ReferenceName]storage.LSN{
					"refs/heads/main/child": 1,
					"refs/heads/main":       3,
				},
				referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{
					1: {"refs/heads/main/child": {}},
					3: {"refs/heads/main": {}},
				},
				root: &node{
					children: children{
						"refs": {
							children: children{
								"heads": {
									children: children{
										"main": {
											target: zeroOID.String(),
											children: children{
												"child": newReference(zeroOID),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("reference update", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main": newReference("oid-1"),
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("non-conflicting reference updates", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {OldOID: "oid-1", NewOID: "oid-2"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main": newReference("oid-2"),
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("conflicting reference update", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))

		require.Equal(t,
			NewUnexpectedOldValueError("refs/heads/main", "oid-2", "oid-1"),
			tx.ApplyUpdates(git.ReferenceUpdates{
				"refs/heads/main": {OldOID: "oid-2", NewOID: "oid-3"},
			}))
	})

	t.Run("reference deletion", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: zeroOID},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			children: children{
				"refs": {
					children: children{
						"heads": {
							children: children{
								"main": newReference(zeroOID),
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("reference update with a deletion in history", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: zeroOID},
		}))
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {OldOID: zeroOID, NewOID: "oid-1"},
		}))
		tx.Commit(1)

		require.Equal(t, &node{
			childReferences: 1,
			children: children{
				"refs": {
					childReferences: 1,
					children: children{
						"heads": {
							childReferences: 1,
							children: children{
								"main": newReference("oid-1"),
							},
						},
					},
				},
			},
		}, history.root)
	})

	t.Run("evict writes of a LSN", func(t *testing.T) {
		history := NewHistory(zeroOID)

		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/tx-1-created":                            {NewOID: "oid-1"},
			"refs/heads/tx-1-deleted":                            {NewOID: zeroOID},
			"refs/heads/tx-2-updated":                            {NewOID: "oid-1"},
			"refs/heads/tx-1-created-tx-2-deleted":               {NewOID: "oid-1"},
			"refs/to-be-empty/tx-1-created":                      {NewOID: "oid-1"},
			"refs/to-be-empty/tx-1-deleted":                      {NewOID: zeroOID},
			"refs/has-child-refs/tx-1-deleted":                   {NewOID: zeroOID},
			"refs/has-deleted-child-refs/tx-1-deleted":           {NewOID: zeroOID},
			"refs/to-have-parent-refs/tx-2-created/tx-1-deleted": {NewOID: zeroOID},
		}))
		tx.Commit(1)

		tx = history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/tx-2-created":                               {NewOID: "oid-1"},
			"refs/heads/tx-2-deleted":                               {NewOID: zeroOID},
			"refs/heads/tx-2-updated":                               {OldOID: "oid-1", NewOID: "oid-2"},
			"refs/heads/tx-1-created-tx-2-deleted":                  {OldOID: "oid-1", NewOID: zeroOID},
			"refs/has-child-refs/tx-1-deleted/tx-2-created":         {NewOID: "oid-1"},
			"refs/has-deleted-child-refs/tx-1-deleted/tx-2-deleted": {NewOID: zeroOID},
			"refs/to-have-parent-refs/tx-2-created":                 {NewOID: "oid-1"},
		}))
		tx.Commit(2)

		require.Equal(t,
			&History{
				zeroOID: zeroOID,
				referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{
					1: {
						"refs/heads/tx-1-created":                            {},
						"refs/heads/tx-1-deleted":                            {},
						"refs/to-be-empty/tx-1-created":                      {},
						"refs/to-be-empty/tx-1-deleted":                      {},
						"refs/has-child-refs/tx-1-deleted":                   {},
						"refs/has-deleted-child-refs/tx-1-deleted":           {},
						"refs/to-have-parent-refs/tx-2-created/tx-1-deleted": {},
					},
					2: {
						"refs/heads/tx-2-created":                               {},
						"refs/heads/tx-2-deleted":                               {},
						"refs/heads/tx-2-updated":                               {},
						"refs/heads/tx-1-created-tx-2-deleted":                  {},
						"refs/has-child-refs/tx-1-deleted/tx-2-created":         {},
						"refs/has-deleted-child-refs/tx-1-deleted/tx-2-deleted": {},
						"refs/to-have-parent-refs/tx-2-created":                 {},
					},
				},
				lsnByReference: map[git.ReferenceName]storage.LSN{
					"refs/heads/tx-1-created":                               1,
					"refs/heads/tx-1-deleted":                               1,
					"refs/to-be-empty/tx-1-created":                         1,
					"refs/to-be-empty/tx-1-deleted":                         1,
					"refs/has-child-refs/tx-1-deleted":                      1,
					"refs/has-deleted-child-refs/tx-1-deleted":              1,
					"refs/to-have-parent-refs/tx-2-created/tx-1-deleted":    1,
					"refs/heads/tx-2-created":                               2,
					"refs/heads/tx-2-deleted":                               2,
					"refs/heads/tx-2-updated":                               2,
					"refs/heads/tx-1-created-tx-2-deleted":                  2,
					"refs/has-child-refs/tx-1-deleted/tx-2-created":         2,
					"refs/has-deleted-child-refs/tx-1-deleted/tx-2-deleted": 2,
					"refs/to-have-parent-refs/tx-2-created":                 2,
				},
				root: &node{
					childReferences: 6,
					children: children{
						"refs": {
							childReferences: 6,
							children: children{
								"heads": {
									childReferences: 3,
									children: children{
										"tx-1-created":              newReference("oid-1"),
										"tx-1-deleted":              newReference(zeroOID),
										"tx-2-created":              newReference("oid-1"),
										"tx-2-updated":              newReference("oid-2"),
										"tx-2-deleted":              newReference(zeroOID),
										"tx-1-created-tx-2-deleted": newReference(zeroOID),
									},
								},
								"to-be-empty": {
									childReferences: 1,
									children: children{
										"tx-1-created": newReference("oid-1"),
										"tx-1-deleted": newReference(zeroOID),
									},
								},
								"has-child-refs": {
									childReferences: 1,
									children: children{
										"tx-1-deleted": {
											target:          zeroOID.String(),
											childReferences: 1,
											children: children{
												"tx-2-created": newReference("oid-1"),
											},
										},
									},
								},
								"has-deleted-child-refs": {
									children: children{
										"tx-1-deleted": {
											target: zeroOID.String(),
											children: children{
												"tx-2-deleted": newReference(zeroOID),
											},
										},
									},
								},
								"to-have-parent-refs": {
									childReferences: 1,
									children: children{
										"tx-2-created": {
											target: "oid-1",
											children: children{
												"tx-1-deleted": newReference(zeroOID),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			history,
		)

		history.Evict(1)

		require.Equal(t,
			&History{
				zeroOID: zeroOID,
				referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{
					2: {
						"refs/heads/tx-2-created":                               {},
						"refs/heads/tx-2-deleted":                               {},
						"refs/heads/tx-2-updated":                               {},
						"refs/heads/tx-1-created-tx-2-deleted":                  {},
						"refs/has-child-refs/tx-1-deleted/tx-2-created":         {},
						"refs/has-deleted-child-refs/tx-1-deleted/tx-2-deleted": {},
						"refs/to-have-parent-refs/tx-2-created":                 {},
					},
				},
				lsnByReference: map[git.ReferenceName]storage.LSN{
					"refs/heads/tx-2-created":                               2,
					"refs/heads/tx-2-deleted":                               2,
					"refs/heads/tx-2-updated":                               2,
					"refs/heads/tx-1-created-tx-2-deleted":                  2,
					"refs/has-child-refs/tx-1-deleted/tx-2-created":         2,
					"refs/has-deleted-child-refs/tx-1-deleted/tx-2-deleted": 2,
					"refs/to-have-parent-refs/tx-2-created":                 2,
				},
				root: &node{
					childReferences: 4,
					children: children{
						"refs": {
							childReferences: 4,
							children: children{
								"heads": {
									childReferences: 2,
									children: children{
										"tx-2-created":              newReference("oid-1"),
										"tx-2-deleted":              newReference(zeroOID),
										"tx-2-updated":              newReference("oid-2"),
										"tx-1-created-tx-2-deleted": newReference(zeroOID),
									},
								},
								"has-child-refs": {
									childReferences: 1,
									children: children{
										"tx-1-deleted": {
											childReferences: 1,
											children: children{
												"tx-2-created": newReference("oid-1"),
											},
										},
									},
								},
								"has-deleted-child-refs": {
									children: children{
										"tx-1-deleted": {
											children: children{
												"tx-2-deleted": newReference(zeroOID),
											},
										},
									},
								},
								"to-have-parent-refs": {
									childReferences: 1,
									children: children{
										"tx-2-created": {
											target:   "oid-1",
											children: children{},
										},
									},
								},
							},
						},
					},
				},
			},
			history,
		)

		history.Evict(2)

		require.Equal(t,
			&History{
				zeroOID:         zeroOID,
				referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{},
				lsnByReference:  map[git.ReferenceName]storage.LSN{},
				root:            &node{children: children{}},
			},
			history,
		)
	})

	t.Run("empty transactions are dropped", func(t *testing.T) {
		history := NewHistory(zeroOID)

		history.Begin().Commit(1)

		require.Equal(t, NewHistory(zeroOID), history)
	})

	t.Run("evict LSN with no references", func(t *testing.T) {
		history := NewHistory(zeroOID)

		// Create a reference and then overwrite it from another transaction.
		tx := history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {NewOID: "oid-1"},
		}))
		tx.Commit(1)

		tx = history.Begin()
		require.NoError(t, tx.ApplyUpdates(git.ReferenceUpdates{
			"refs/heads/main": {OldOID: "oid-1", NewOID: "oid-2"},
		}))
		tx.Commit(2)

		// Records related to the first LSN have already been dropped as all of its
		// reference writes have been superseded.
		expectedHistory := &History{
			zeroOID: zeroOID,
			lsnByReference: map[git.ReferenceName]storage.LSN{
				"refs/heads/main": 2,
			},
			referencesByLSN: map[storage.LSN]map[git.ReferenceName]struct{}{
				2: {"refs/heads/main": {}},
			},
			root: &node{
				childReferences: 1,
				children: children{
					"refs": {
						childReferences: 1,
						children: children{
							"heads": {
								childReferences: 1,
								children: children{
									"main": newReference("oid-2"),
								},
							},
						},
					},
				},
			},
		}
		require.Equal(t, expectedHistory, history)

		// Evicting it now should be a no-op.
		history.Evict(1)
		require.Equal(t, expectedHistory, history)
	})
}
