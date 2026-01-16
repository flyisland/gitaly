package commit

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetTreeEntries(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type TestData struct {
		cfg    config.Cfg
		client *grpc.ClientConn
	}

	type setupData struct {
		request             *gitalypb.GetTreeEntriesRequest
		expectedTreeEntries []*gitalypb.TreeEntry
		expectedCursor      *gitalypb.PaginationCursor
		expectedErr         error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, data TestData) setupData
	}{
		{
			desc: "path with curly braces exists",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blob := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test1"))
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "issue-46261", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test1.txt", Mode: "100644", OID: blob},
						})},
						{Path: "{{curly}}", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test2.txt", Mode: "100644", Content: "test2"},
						})},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("issue-46261/folder"),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       blob.String(),
							Path:      []byte("issue-46261/folder/test1.txt"),
							Type:      0,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("issue-46261/folder/test1.txt"),
						},
					},
				}
			},
		},
		{
			desc: "path with curly braces exists and is requested",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blob := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test2"))
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "issue-46261", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test1.txt", Mode: "100644", Content: "test1"},
						})},
						{Path: "{{curly}}", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test2.txt", Mode: "100644", OID: blob},
						})},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("issue-46261/{{curly}}"),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       blob.String(),
							Path:      []byte("issue-46261/{{curly}}/test2.txt"),
							Type:      0,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("issue-46261/{{curly}}/test2.txt"),
						},
					},
				}
			},
		},
		{
			desc: "repository does not exist",
			setup: func(t *testing.T, data TestData) setupData {
				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
						Revision:   []byte(gittest.DefaultObjectHash.EmptyTreeOID),
						Path:       []byte("folder"),
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("fake"),
					)),
				}
			},
		},
		{
			desc: "repository is nil",
			setup: func(t *testing.T, data TestData) setupData {
				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: nil,
						Revision:   []byte(gittest.DefaultObjectHash.EmptyTreeOID),
						Path:       []byte("folder"),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "revision is empty",
			setup: func(t *testing.T, data TestData) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, data.cfg)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   nil,
						Path:       []byte("folder"),
					},
					expectedErr: structerr.NewInvalidArgument("empty revision"),
				}
			},
		},
		{
			desc: "path is empty",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
					},
					expectedErr: structerr.NewInvalidArgument("empty path").WithDetail(&gitalypb.GetTreeEntriesError{
						Error: &gitalypb.GetTreeEntriesError_Path{
							Path: &gitalypb.PathError{
								ErrorType: gitalypb.PathError_ERROR_TYPE_EMPTY_PATH,
							},
						},
					}),
				}
			},
		},
		{
			desc: "revision is invalid",
			setup: func(t *testing.T, data TestData) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, data.cfg)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte("--output=/meow"),
						Path:       []byte("folder"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "non existent token",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("folder"),
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: "non-existent",
						},
					},
					expectedErr: status.Error(codes.Internal, "could not find starting OID: non-existent"),
				}
			},
		},
		{
			desc: "path points to a file",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{
						Mode:    "100644",
						Path:    "README.md",
						Content: "something with spaces in between",
					}),
				)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("README.md"),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("path not treeish").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "README.md"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path points to a file plus recursive",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{
						Mode:    "100644",
						Path:    "README.md",
						Content: "something with spaces in between",
					}),
				)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("README.md"),
						Recursive:  true,
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("path not treeish").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "README.md"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path resolves outside the repo",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath,
					gittest.WithTreeEntries(gittest.TreeEntry{
						Mode:    "100644",
						Path:    "README.md",
						Content: "something with spaces in between",
					}),
				)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("./.."),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "./.."},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path contains relative path syntax ..",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("./folder/.."),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "./folder/.."},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path contains relative path syntax ./",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					}),
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("./folder/test.txt"),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "./folder/test.txt"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path with .. in request raises no errors",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				treeID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "test.txt", Mode: "100644", OID: blobID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					OID:  treeID,
					Mode: "040000",
					Path: "a..b",
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte("a..b"),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       blobID.String(),
							Path:      []byte("a..b/test.txt"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("a..b/test.txt"),
						},
					},
				}
			},
		},
		{
			desc: "path is .",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				treeID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "test.txt", Mode: "100644", Content: "test"},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					OID:  treeID,
					Mode: "040000",
					Path: "folder",
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						// when path is ".", we resolve it to ""
						Path: []byte("."),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       treeID.String(),
							Path:      []byte("folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("folder"),
						},
					},
				}
			},
		},
		{
			desc: "absolute path is used",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				treeID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "test.txt", Mode: "100644", Content: "test"},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					OID:  treeID,
					Mode: "040000",
					Path: "folder",
				}))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID.String()),
						Path:       []byte(repoPath + "folder"),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: repoPath + "folder"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "deeply nested flat path",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				nestingLevel := 12
				require.Greater(t, nestingLevel, defaultFlatTreeRecursion, "sanity check: construct folder deeper than default recursion value")

				// We create a tree structure that is one deeper than the flat-tree recursion limit.
				var treeIDs []git.ObjectID
				for i := nestingLevel; i >= 0; i-- {
					var treeEntry gittest.TreeEntry
					if len(treeIDs) == 0 {
						treeEntry = gittest.TreeEntry{Path: ".gitkeep", Mode: "100644", Content: "something"}
					} else {
						// We use a numbered directory name to make it easier to see when things get
						// truncated.
						treeEntry = gittest.TreeEntry{Path: strconv.Itoa(i), Mode: "040000", OID: treeIDs[len(treeIDs)-1]}
					}

					treeID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{treeEntry})
					treeIDs = append(treeIDs, treeID)
				}
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTree(treeIDs[len(treeIDs)-1]))

				return setupData{
					// We make a non-recursive request which tries to fetch tree entrie for the tree structure
					// we have created above. This should return a single entry, which is the directory we're
					// requesting.
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("0"),
						Recursive:  false,
					},
					// We know that there is a directory "1/2/3/4/5/6/7/8/9/10/11/12", but here we only get
					// "1/2/3/4/5/6/7/8/9/10/11" as flat path. This proves that FlatPath recursion is bounded,
					// which is the point of this test.
					expectedTreeEntries: []*gitalypb.TreeEntry{{
						Oid:       treeIDs[nestingLevel-2].String(),
						Path:      []byte("0/1"),
						FlatPath:  []byte("0/1/2/3/4/5/6/7/8/9/10"),
						Type:      gitalypb.TreeEntry_TREE,
						Mode:      0o40000,
						CommitOid: commitID.String(),
					}},
				}
			},
		},
		{
			desc: "with root path but only files in repo",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				fileOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("file"))
				file2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("file2"))

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: file2OID, Mode: "100644", Path: "bar"},
					gittest.TreeEntry{OID: fileOID, Mode: "100644", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       file2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("bar"),
						},
						{
							Oid:       fileOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("foo"),
						},
					},
				}
			},
		},
		{
			desc: "with root path and disabled flat path but only files in repo",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				fileOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("file"))
				file2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("file2"))

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: file2OID, Mode: "100644", Path: "bar"},
					gittest.TreeEntry{OID: fileOID, Mode: "100644", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository:    repo,
						Revision:      []byte(commitID),
						Path:          []byte("."),
						SkipFlatPaths: true,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       file2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       fileOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "with root path and repo with folders",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					})},
				})

				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder2", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "folder3", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test2.txt", Mode: "100644", Content: "test2"},
						})},
					})},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("bar/folder2/folder3"),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("foo/folder"),
						},
					},
				}
			},
		},
		{
			desc: "with specific folder",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "test.txt", Mode: "100644", Content: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder2", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "folder3", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test2.txt", Mode: "100644", Content: "test2"},
						})},
					})},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("foo"),
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("foo/folder"),
						},
					},
				}
			},
		},
		{
			desc: "with specific folder and disabled flatpath",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "test.txt", Mode: "100644", Content: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder2", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "folder3", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{Path: "test2.txt", Mode: "100644", Content: "test2"},
						})},
					})},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository:    repo,
						Revision:      []byte(commitID),
						Path:          []byte("foo"),
						SkipFlatPaths: true,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "with recursive",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolder2OID.String(),
							Path:      []byte("bar/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subSubFolderOID.String(),
							Path:      []byte("bar/folder/folder2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "with non-existent path",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					})},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("does-not-exist"),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "does-not-exist"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "with non-existent path plus recursive",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					})},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("does-not-exist"),
						Recursive:  true,
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewNotFound("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte(commitID),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: "does-not-exist"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "with non-existent revision",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					})},
				})

				gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte("does-not-exist"),
						Path:       []byte("."),
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte("does-not-exist"),
								},
							},
						}),
						structerr.MetadataItem{
							Key:   "path",
							Value: "",
						},
						structerr.MetadataItem{Key: "revision", Value: "does-not-exist"},
					),
				}
			},
		},
		{
			desc: "with non-existent revision plus recursive",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{Path: "test.txt", Mode: "100644", Content: "test"},
					})},
				})

				gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte("does-not-exist"),
						Path:       []byte("."),
						Recursive:  true,
					},
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewNotFound("invalid revision or path").WithDetail(&gitalypb.GetTreeEntriesError{
							Error: &gitalypb.GetTreeEntriesError_ResolveTree{
								ResolveTree: &gitalypb.ResolveRevisionError{
									Revision: []byte("does-not-exist"),
								},
							},
						}),
						structerr.MetadataItem{Key: "path", Value: ""},
						structerr.MetadataItem{Key: "revision", Value: "does-not-exist"},
					),
				}
			},
		},
		{
			desc: "sorted by trees first",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolder2OID.String(),
							Path:      []byte("bar/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subSubFolderOID.String(),
							Path:      []byte("bar/folder/folder2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "pagination - read a tree with subdirectories",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)
				subSubDir2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "test3",
						Content: "test3-content",
					},
					{
						Mode:    "100644",
						Path:    "test4",
						Content: "test4-content",
					},
				})

				subSubDir3OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "test5",
						Content: "test5-content",
					},
				})

				SubDirBlobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test6-content"))

				rootTreeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{
						Path: "rootDir",
						Mode: "040000",
						OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
							{
								Path: "subDir",
								Mode: "040000",
								OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
									{
										Path: "subSubDir",
										Mode: "040000",
										OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
											{
												Mode:    "100644",
												Path:    "test",
												Content: "test",
											},
											{
												Mode:    "100644",
												Path:    "test2",
												Content: "test2-content",
											},
										}),
									},
									{
										Path: "subSubDir2",
										Mode: "040000",
										OID:  subSubDir2OID,
									},

									{
										Path: "subSubDir3",
										Mode: "040000",
										OID:  subSubDir3OID,
									},

									{
										Path: "test6-content",
										Mode: "100644",
										OID:  SubDirBlobOID,
									},
								}),
							},
							{
								Path: "subDir2",
								Mode: "040000",
								OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
									{
										Mode:    "100644",
										Path:    "test5",
										Content: "test5-content",
									},
								}),
							},
							{
								Path: "subDir3",
								Mode: "040000",
								OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
									{
										Mode:    "100644",
										Path:    "test6",
										Content: "test6-content",
									},
								}),
							},
						}),
					},
					{
						Mode:    "100644",
						Path:    "file",
						Content: "file-content",
					},
				})
				gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTree(rootTreeOID), gittest.WithBranch("main"))

				// First request to get initial page token
				firstReq := &gitalypb.GetTreeEntriesRequest{
					Repository: repo,
					Revision:   []byte("main"),
					Path:       []byte("rootDir/subDir"),
					Recursive:  false,
					PaginationParams: &gitalypb.PaginationParameter{
						Limit: 1,
					},
					Sort: gitalypb.GetTreeEntriesRequest_TREES_FIRST,
				}
				stream, err := gitalypb.NewCommitServiceClient(data.client).GetTreeEntries(ctx, firstReq)
				require.NoError(t, err)

				var firstResp *gitalypb.GetTreeEntriesResponse
				firstResp, err = stream.Recv()
				require.NoError(t, err)
				require.NotEmpty(t, firstResp.GetPaginationCursor().GetNextCursor())
				initialPageToken := firstResp.GetPaginationCursor().GetNextCursor()

				// Verify first entry
				require.Len(t, firstResp.GetEntries(), 1)
				require.Equal(t, []byte("rootDir/subDir/subSubDir"), firstResp.GetEntries()[0].GetPath())

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte("main"),
						Path:       []byte("rootDir/subDir"),
						Recursive:  false,
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: initialPageToken,
							Limit:     3,
						},
						Sort: gitalypb.GetTreeEntriesRequest_TREES_FIRST,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       string(subSubDir2OID),
							Path:      []byte("rootDir/subDir/subSubDir2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o040000,
							FlatPath:  []byte("rootDir/subDir/subSubDir2"),
							CommitOid: string("main"),
						},
						{
							Oid:       string(subSubDir3OID),
							Path:      []byte("rootDir/subDir/subSubDir3"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o040000,
							FlatPath:  []byte("rootDir/subDir/subSubDir3"),
							CommitOid: string("main"),
						},

						{
							Oid:       string(SubDirBlobOID),
							Path:      []byte("rootDir/subDir/test6-content"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							FlatPath:  []byte("rootDir/subDir/test6-content"),
							CommitOid: string("main"),
						},
					},
				}
			},
		},
		{
			desc: "pagination continues on same tree after concurrent commit",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)
				file3OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("file-3-content"))
				dir2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Mode: "100644", Path: "file3", OID: file3OID},
				})
				// Initial commit
				originalCommitOID := gittest.WriteCommit(t, data.cfg, repoPath,
					gittest.WithTree(gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{
							Path: "dir1",
							Mode: "040000",
							OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
								{Mode: "100644", Path: "file2", Content: "file2-content"},
							}),
						},
						{
							Path: "dir2",
							Mode: "040000",
							OID:  dir2OID,
						},
					})),
					gittest.WithBranch("main"),
				)

				// We need hooks set up to commit the reference update in WriteRef with transactions.
				testcfg.BuildGitalyHooks(t, data.cfg)
				// Second commit we'll update the main branch to between the paginated requests.
				concurrentCommitOID := gittest.WriteCommit(t, data.cfg, repoPath,
					gittest.WithTree(gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
						{
							Path: "new_dir",
							Mode: "040000",
							OID: gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
								{
									Mode:    "100644",
									Path:    "new_file2",
									Content: "new_file2-content",
								},
							}),
						},
					})),
				)

				// First request to get initial page token
				firstReq := &gitalypb.GetTreeEntriesRequest{
					Repository: repo,
					Revision:   []byte("main"),
					Path:       []byte("."),
					Recursive:  true,
					PaginationParams: &gitalypb.PaginationParameter{
						Limit: 2,
					},
				}
				stream, err := gitalypb.NewCommitServiceClient(data.client).GetTreeEntries(ctx, firstReq)
				require.NoError(t, err)

				var firstResp *gitalypb.GetTreeEntriesResponse
				firstResp, err = stream.Recv()
				require.NoError(t, err)
				require.NotEmpty(t, firstResp.GetPaginationCursor().GetNextCursor())
				initialPageToken := firstResp.GetPaginationCursor().GetNextCursor()

				// Verify first two entries
				require.Len(t, firstResp.GetEntries(), 2)
				require.Equal(t, []byte("dir1"), firstResp.GetEntries()[0].GetPath())
				require.Equal(t, []byte("dir1/file2"), firstResp.GetEntries()[1].GetPath())

				resp, err := gitalypb.NewRepositoryServiceClient(data.client).WriteRef(ctx, &gitalypb.WriteRefRequest{
					Repository:  repo,
					Ref:         []byte("refs/heads/main"),
					Revision:    []byte(concurrentCommitOID),
					OldRevision: []byte(originalCommitOID),
				})
				require.NoError(t, err)
				testhelper.ProtoEqual(t, resp, &gitalypb.WriteRefResponse{})

				expectedTreeEntries := []*gitalypb.TreeEntry{
					{
						Oid:  dir2OID.String(),
						Path: []byte("dir2"),
						Type: gitalypb.TreeEntry_TREE,
						Mode: 0o40000,
						// CommitOid field is currently being evaluated as revision could be a branch name or a commitID,
						// for more info refer to https://gitlab.com/gitlab-org/gitaly/-/issues/6205
						CommitOid: "main",
					},
					{
						Oid:       file3OID.String(),
						Path:      []byte("dir2/file3"),
						Type:      gitalypb.TreeEntry_BLOB,
						Mode:      0o100644,
						CommitOid: "main",
					},
				}

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte("main"),
						Path:       []byte("."),
						Recursive:  true,
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: initialPageToken,
							Limit:     2,
						},
					},
					expectedTreeEntries: expectedTreeEntries,
				}
			},
		},
		{
			desc: "sorted by trees first and paginated",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				rootTreeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: folder2OID, Mode: "040000", Path: "bar"},
					{OID: folderOID, Mode: "040000", Path: "foo"},
				})
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTree(rootTreeOID))

				expectedTreeEntries := []*gitalypb.TreeEntry{
					{
						Oid:       folder2OID.String(),
						Path:      []byte("bar"),
						Type:      gitalypb.TreeEntry_TREE,
						Mode:      0o40000,
						CommitOid: commitID.String(),
					},
					{
						Oid:       subFolder2OID.String(),
						Path:      []byte("bar/folder"),
						Type:      gitalypb.TreeEntry_TREE,
						Mode:      0o40000,
						CommitOid: commitID.String(),
					},
					{
						Oid:       subSubFolderOID.String(),
						Path:      []byte("bar/folder/folder2"),
						Type:      gitalypb.TreeEntry_TREE,
						Mode:      0o40000,
						CommitOid: commitID.String(),
					},
				}

				cursor, err := encodePageToken(expectedTreeEntries[2], rootTreeOID)
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 3,
						},
					},
					expectedTreeEntries: expectedTreeEntries,
					expectedCursor: &gitalypb.PaginationCursor{
						NextCursor: cursor,
					},
				}
			},
		},
		{
			desc: "sorted by trees first and paginated with token",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
						PaginationParams: &gitalypb.PaginationParameter{
							PageToken: folderOID.String(),
							Limit:     3,
						},
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "sorted by trees first with high pagination limit",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 100,
						},
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolder2OID.String(),
							Path:      []byte("bar/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subSubFolderOID.String(),
							Path:      []byte("bar/folder/folder2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "sorted by trees first with 0 pagination limit",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 0,
						},
					},
				}
			},
		},
		{
			desc: "sorted by trees first with -1 pagination limit",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_TREES_FIRST,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: -1,
						},
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolder2OID.String(),
							Path:      []byte("bar/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subSubFolderOID.String(),
							Path:      []byte("bar/folder/folder2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "path to submodule",
			setup: func(t *testing.T, data TestData) setupData {
				_, submoduleRepoPath := gittest.CreateRepository(t, ctx, data.cfg)
				submodule := gittest.WriteCommit(t, data.cfg, submoduleRepoPath)

				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "submodule", Mode: "160000", OID: submodule},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("submodule"),
					},
					// When the path is to a submodule, the repository resolves the revision to the
					// commit ID for the submodule. This OID does not exist in the repository.
					// This results in the provided path being considered invalid and an error is
					// returned.
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewNotFound("revision doesn't exist").WithDetail(
							&gitalypb.GetTreeEntriesError{
								Error: &gitalypb.GetTreeEntriesError_ResolveTree{
									ResolveTree: &gitalypb.ResolveRevisionError{
										Revision: []byte(commitID),
									},
								},
							},
						),
						structerr.MetadataItem{Key: "path", Value: "submodule"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "path inside submodule",
			setup: func(t *testing.T, data TestData) setupData {
				_, submoduleRepoPath := gittest.CreateRepository(t, ctx, data.cfg)
				submodule := gittest.WriteCommit(t, data.cfg, submoduleRepoPath)

				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "submodule", Mode: "160000", OID: submodule},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("submodule/foo"),
					},
					// When the path is in a submodule, the repository is unable to resolve the
					// revision and consequently is considered invalid.
					expectedErr: testhelper.WithInterceptedMetadataItems(
						structerr.NewInvalidArgument("invalid revision or path").WithDetail(
							&gitalypb.GetTreeEntriesError{
								Error: &gitalypb.GetTreeEntriesError_ResolveTree{
									ResolveTree: &gitalypb.ResolveRevisionError{
										Revision: []byte(commitID),
									},
								},
							},
						),
						structerr.MetadataItem{Key: "path", Value: "submodule/foo"},
						structerr.MetadataItem{Key: "revision", Value: commitID},
					),
				}
			},
		},
		{
			desc: "sorted by filesystem lexicographically",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobAOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("a"))
				blobBOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("b"))

				treeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "nested", Mode: "100644", Content: "nested"},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: blobAOID, Mode: "100644", Path: "aaa"},
					gittest.TreeEntry{OID: treeOID, Mode: "040000", Path: "bbb"},
					gittest.TreeEntry{OID: blobBOID, Mode: "100644", Path: "ccc"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  false,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
					},
					// FILESYSTEM sort: entries sorted lexicographically by path
					// aaa < bbb < ccc (regardless of type)
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       blobAOID.String(),
							Path:      []byte("aaa"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("aaa"),
						},
						{
							Oid:       treeOID.String(),
							Path:      []byte("bbb"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("bbb"),
						},
						{
							Oid:       blobBOID.String(),
							Path:      []byte("ccc"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("ccc"),
						},
					},
				}
			},
		},
		{
			desc: "sorted by filesystem with dots and underscores",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				fooOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("foo"))
				fooAOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("foo.a"))
				fooATestOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("foo.a.test"))
				fooBOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("foo.b"))

				fooBTestOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "nested", Mode: "100644", Content: "nested"},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: fooOID, Mode: "100644", Path: "Foo"},
					gittest.TreeEntry{OID: fooAOID, Mode: "100644", Path: "Foo.A"},
					gittest.TreeEntry{OID: fooATestOID, Mode: "100644", Path: "Foo.A.Test"},
					gittest.TreeEntry{OID: fooBOID, Mode: "100644", Path: "Foo.B"},
					gittest.TreeEntry{OID: fooBTestOID, Mode: "040000", Path: "Foo.B.Test"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  false,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
					},
					// Lexicographic order: Foo < Foo.A < Foo.A.Test < Foo.B < Foo.B.Test
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       fooOID.String(),
							Path:      []byte("Foo"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Foo"),
						},
						{
							Oid:       fooAOID.String(),
							Path:      []byte("Foo.A"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Foo.A"),
						},
						{
							Oid:       fooATestOID.String(),
							Path:      []byte("Foo.A.Test"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Foo.A.Test"),
						},
						{
							Oid:       fooBOID.String(),
							Path:      []byte("Foo.B"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Foo.B"),
						},
						{
							Oid:       fooBTestOID.String(),
							Path:      []byte("Foo.B.Test"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Foo.B.Test"),
						},
					},
				}
			},
		},
		{
			desc: "sorted by filesystem recursive",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test"))
				subFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blobOID, Mode: "100644", Path: "test"},
				})
				folderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolderOID},
				})

				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("test2"))
				subSubFolderOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob2OID, Mode: "100644", Path: "test"},
				})
				subFolder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: subSubFolderOID, Mode: "040000", Path: "folder2"},
				})
				folder2OID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{Path: "folder", Mode: "040000", OID: subFolder2OID},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: folder2OID, Mode: "040000", Path: "bar"},
					gittest.TreeEntry{OID: folderOID, Mode: "040000", Path: "foo"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       folder2OID.String(),
							Path:      []byte("bar"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolder2OID.String(),
							Path:      []byte("bar/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subSubFolderOID.String(),
							Path:      []byte("bar/folder/folder2"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blob2OID.String(),
							Path:      []byte("bar/folder/folder2/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       folderOID.String(),
							Path:      []byte("foo"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       subFolderOID.String(),
							Path:      []byte("foo/folder"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       blobOID.String(),
							Path:      []byte("foo/folder/test"),
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "sorted by filesystem with pagination",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blob1OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("1"))
				blob2OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("2"))
				blob3OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("3"))
				blob4OID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("4"))

				rootTreeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: blob1OID, Mode: "100644", Path: "aaa"},
					{OID: blob2OID, Mode: "100644", Path: "bbb"},
					{OID: blob3OID, Mode: "100644", Path: "ccc"},
					{OID: blob4OID, Mode: "100644", Path: "ddd"},
				})
				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTree(rootTreeOID))

				expectedFirstPage := []*gitalypb.TreeEntry{
					{
						Oid:       blob1OID.String(),
						Path:      []byte("aaa"),
						Type:      gitalypb.TreeEntry_BLOB,
						Mode:      0o100644,
						CommitOid: commitID.String(),
						FlatPath:  []byte("aaa"),
					},
					{
						Oid:       blob2OID.String(),
						Path:      []byte("bbb"),
						Type:      gitalypb.TreeEntry_BLOB,
						Mode:      0o100644,
						CommitOid: commitID.String(),
						FlatPath:  []byte("bbb"),
					},
				}

				cursor, err := encodePageToken(expectedFirstPage[1], rootTreeOID)
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  false,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 2,
						},
					},
					expectedTreeEntries: expectedFirstPage,
					expectedCursor: &gitalypb.PaginationCursor{
						NextCursor: cursor,
					},
				}
			},
		},
		{
			desc: "sorted by filesystem with nested directory paths",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				// Test sorting when shorter path is a prefix of longer path in directories.
				// In byte order: "Foo.A/Bar" < "Foo.A.Test/Bar" because '/' (47) < '.' (46) is false,
				// and '.' (46) < '/' (47), so "Foo.A.Test" < "Foo.A/" when compared byte-by-byte
				// up to the differing character.
				// Actually: "Foo.A.Test/Bar" vs "Foo.A/Bar" - at index 5, '.' (46) < '/' (47)
				// So "Foo.A.Test/Bar" < "Foo.A/Bar"
				barInFooAOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("bar in Foo.A"))
				barInFooATestOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("bar in Foo.A.Test"))

				fooATreeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: barInFooAOID, Mode: "100644", Path: "Bar"},
				})
				fooATestTreeOID := gittest.WriteTree(t, data.cfg, repoPath, []gittest.TreeEntry{
					{OID: barInFooATestOID, Mode: "100644", Path: "Bar"},
				})

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: fooATreeOID, Mode: "040000", Path: "Foo.A"},
					gittest.TreeEntry{OID: fooATestTreeOID, Mode: "040000", Path: "Foo.A.Test"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  true,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
					},
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       fooATreeOID.String(),
							Path:      []byte("Foo.A"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       fooATestTreeOID.String(),
							Path:      []byte("Foo.A.Test"),
							Type:      gitalypb.TreeEntry_TREE,
							Mode:      0o40000,
							CommitOid: commitID.String(),
						},
						{
							Oid:       barInFooATestOID.String(),
							Path:      []byte("Foo.A.Test/Bar"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
						{
							Oid:       barInFooAOID.String(),
							Path:      []byte("Foo.A/Bar"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
						},
					},
				}
			},
		},
		{
			desc: "sorted by filesystem case sensitivity",
			setup: func(t *testing.T, data TestData) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, data.cfg)

				blobAOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("A"))
				blobZOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("Z"))
				blobaOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("a"))
				blobzOID := gittest.WriteBlob(t, data.cfg, repoPath, []byte("z"))

				commitID := gittest.WriteCommit(t, data.cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{OID: blobAOID, Mode: "100644", Path: "Apple"},
					gittest.TreeEntry{OID: blobZOID, Mode: "100644", Path: "Zebra"},
					gittest.TreeEntry{OID: blobaOID, Mode: "100644", Path: "apple"},
					gittest.TreeEntry{OID: blobzOID, Mode: "100644", Path: "zebra"},
				))

				return setupData{
					request: &gitalypb.GetTreeEntriesRequest{
						Repository: repo,
						Revision:   []byte(commitID),
						Path:       []byte("."),
						Recursive:  false,
						Sort:       gitalypb.GetTreeEntriesRequest_FILESYSTEM,
					},
					// ASCII byte order: Apple < Zebra < apple < zebra
					expectedTreeEntries: []*gitalypb.TreeEntry{
						{
							Oid:       blobAOID.String(),
							Path:      []byte("Apple"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Apple"),
						},
						{
							Oid:       blobZOID.String(),
							Path:      []byte("Zebra"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("Zebra"),
						},
						{
							Oid:       blobaOID.String(),
							Path:      []byte("apple"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("apple"),
						},
						{
							Oid:       blobzOID.String(),
							Path:      []byte("zebra"),
							Type:      gitalypb.TreeEntry_BLOB,
							Mode:      0o100644,
							CommitOid: commitID.String(),
							FlatPath:  []byte("zebra"),
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)

			cfg.SocketPath = startTestServices(t, cfg)
			cc := dial(t, cfg.SocketPath)

			data := tc.setup(t, TestData{
				cfg:    cfg,
				client: cc,
			})

			c, err := gitalypb.NewCommitServiceClient(cc).GetTreeEntries(ctx, data.request)
			require.NoError(t, err)

			fetchedEntries, cursor := getTreeEntriesFromTreeEntryClient(t, c, data.expectedErr)
			testhelper.ProtoEqual(t, data.expectedTreeEntries, fetchedEntries)
			if data.expectedCursor != nil || cursor.GetNextCursor() != "" {
				testhelper.ProtoEqual(t, data.expectedCursor, cursor)
			}
		})
	}
}

func BenchmarkGetTreeEntries(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg, client := setupCommitService(b, ctx)

	repo, repoPath := gittest.CreateRepository(b, ctx, cfg)
	commitID := populateRepoWithTreesBlobs(b, repoPath, cfg, 20)

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.GetTreeEntriesRequest
		expectedEntries int
	}{
		{
			desc: "recursive from root",
			request: &gitalypb.GetTreeEntriesRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("."),
				Recursive:  true,
			},
			expectedEntries: 40419,
		},
		{
			desc: "non-recursive from root",
			request: &gitalypb.GetTreeEntriesRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("."),
				Recursive:  false,
			},
			expectedEntries: 21,
		},
		{
			desc: "recursive from subdirectory",
			request: &gitalypb.GetTreeEntriesRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("folder1/folder2/folder3"),
				Recursive:  true,
			},
			expectedEntries: 34356,
		},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stream, err := client.GetTreeEntries(ctx, tc.request)
				require.NoError(b, err)

				entriesReceived, err := testhelper.ReceiveAndFold(stream.Recv, func(result int, response *gitalypb.GetTreeEntriesResponse) int {
					return result + len(response.GetEntries())
				})
				require.NoError(b, err)
				require.Equal(b, tc.expectedEntries, entriesReceived)
			}
		})
	}
}

func getTreeEntriesFromTreeEntryClient(t *testing.T, client gitalypb.CommitService_GetTreeEntriesClient, expectedError error) ([]*gitalypb.TreeEntry, *gitalypb.PaginationCursor) {
	t.Helper()

	var entries []*gitalypb.TreeEntry
	var cursor *gitalypb.PaginationCursor
	firstEntryReceived := false

	for {
		resp, err := client.Recv()

		if expectedError == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			entries = append(entries, resp.GetEntries()...)

			if !firstEntryReceived {
				cursor = resp.GetPaginationCursor()
				firstEntryReceived = true
			} else {
				require.Equal(t, nil, resp.GetPaginationCursor())
			}
		} else {
			testhelper.RequireGrpcError(t, expectedError, err, protocmp.SortRepeatedFields(&spb.Status{}, "details"))
			break
		}
	}
	return entries, cursor
}

func populateRepoWithTreesBlobs(tb testing.TB, repoPath string, cfg config.Cfg, depth int) git.ObjectID {
	var treeOID git.ObjectID
	treeCount, blobCount := 20, 100

	writeTree := func(path string) gittest.TreeEntry {
		entries := []gittest.TreeEntry{}

		for i := 0; i < blobCount; i++ {
			entries = append(entries, gittest.TreeEntry{
				OID: gittest.WriteBlob(tb, cfg, repoPath, []byte(fmt.Sprintf("%d", i))), Mode: "100644", Path: fmt.Sprintf("%d", i),
			})
		}

		return gittest.TreeEntry{
			OID:  gittest.WriteTree(tb, cfg, repoPath, entries),
			Mode: "040000",
			Path: path,
		}
	}

	for i := depth; i > 0; i-- {
		entries := []gittest.TreeEntry{}

		for j := 0; j < treeCount; j++ {
			entries = append(entries, writeTree(fmt.Sprintf("%d", j)))
		}

		if treeOID != "" {
			entries = append(entries, gittest.TreeEntry{
				OID:  treeOID,
				Mode: "040000",
				Path: fmt.Sprintf("folder%d", i),
			})
		}

		treeOID = gittest.WriteTree(tb, cfg, repoPath, entries)
	}

	return gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTree(treeOID))
}
