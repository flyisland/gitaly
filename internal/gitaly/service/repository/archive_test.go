package repository

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/smudge"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v18/streamio"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
)

func TestGetArchive(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	gitlabURL, cleanup := gitlab.NewTestServer(t, gitlab.TestServerOptions{
		SecretToken: "gitlab-secret-token",
		LfsBody:     "replaced LFS pointer contents",
	})
	t.Cleanup(cleanup)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Gitlab: config.Gitlab{
			URL:        gitlabURL,
			SecretFile: gitlab.WriteShellSecretFile(t, testhelper.TempDir(t), "gitlab-secret-token"),
		},
	}))
	testcfg.BuildGitalyLFSSmudge(t, cfg)

	client, socketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = socketPath

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	gitattributesContent := "*.lfs filter=lfs diff=lfs merge=lfs -text"
	lfsPointerContent := "version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: ".gitattributes", Mode: "100644", Content: gitattributesContent},
		gittest.TreeEntry{Path: "pointer.lfs", Mode: "100644", Content: lfsPointerContent},
		gittest.TreeEntry{Path: "LICENSE", Mode: "100644", Content: "license content"},
		gittest.TreeEntry{Path: "README.md", Mode: "100644", Content: "readme content"},
		gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "subfile", Mode: "100644", Content: "subfile content"},
			{Path: "subsubdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "subsubfile", Mode: "100644", Content: "subsubfile content"},
			})},
		})},
	))

	for _, format := range []gitalypb.GetArchiveRequest_Format{
		gitalypb.GetArchiveRequest_ZIP,
		gitalypb.GetArchiveRequest_TAR,
		gitalypb.GetArchiveRequest_TAR_GZ,
		gitalypb.GetArchiveRequest_TAR_BZ2,
	} {
		t.Run(format.String(), func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				desc             string
				request          *gitalypb.GetArchiveRequest
				expectedErr      error
				expectedContents map[string]string
			}{
				{
					desc: "invalid storage",
					request: &gitalypb.GetArchiveRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "fake",
							RelativePath: "path",
						},
						CommitId: commitID.String(),
						Format:   gitalypb.GetArchiveRequest_ZIP,
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("fake"),
					)),
				},
				{
					desc: "unset repository",
					request: &gitalypb.GetArchiveRequest{
						Repository: nil,
						CommitId:   commitID.String(),
						Format:     gitalypb.GetArchiveRequest_ZIP,
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				},
				{
					desc: "empty commit ID",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   "",
						Format:     gitalypb.GetArchiveRequest_ZIP,
					},
					expectedErr: structerr.NewInvalidArgument("invalid commitId: empty revision"),
				},
				{
					desc: "invalid format",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   commitID.String(),
						Format:     gitalypb.GetArchiveRequest_Format(-1),
					},
					expectedErr: structerr.NewInvalidArgument("invalid format"),
				},
				{
					desc: "non-existent path in commit",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   commitID.String(),
						Format:     gitalypb.GetArchiveRequest_ZIP,
						Path:       []byte("unknown-path"),
					},
					expectedErr: structerr.NewFailedPrecondition("path doesn't exist"),
				},
				{
					desc: "empty root tree",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   gittest.DefaultObjectHash.EmptyTreeOID.String(),
						Format:     gitalypb.GetArchiveRequest_ZIP,
						Path:       []byte("."),
					},
					expectedErr: structerr.NewFailedPrecondition("path doesn't exist"),
				},
				{
					desc: "nonexistent excluded path",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   commitID.String(),
						Format:     gitalypb.GetArchiveRequest_ZIP,
						Exclude:    [][]byte{[]byte("files/")},
					},
					expectedErr: structerr.NewFailedPrecondition("exclude[0] doesn't exist"),
				},
				{
					desc: "path contains directory traversal outside repository root",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   commitID.String(),
						Format:     gitalypb.GetArchiveRequest_ZIP,
						Path:       []byte("../../foo"),
					},
					expectedErr: structerr.NewInvalidArgument("relative path escapes root directory"),
				},
				{
					desc: "repo with missing relative path",
					request: &gitalypb.GetArchiveRequest{
						Repository: &gitalypb.Repository{
							StorageName: repo.GetStorageName(),
						},
						Prefix:   "qwert",
						CommitId: commitID.String(),
						Format:   gitalypb.GetArchiveRequest_TAR,
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
				},
				{
					desc: "with path is file and path elision",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						CommitId:   commitID.String(),
						Prefix:     "my-prefix",
						ElidePath:  true,
						Path:       []byte("subdir/subsubdir/subsubfile"),
						Exclude:    [][]byte{[]byte("subdir/subsubdir")},
					},
					expectedErr: structerr.NewInvalidArgument(`invalid exclude: "subdir/subsubdir" is not a subdirectory of "subdir/subsubdir/subsubfile"`),
				},
				{
					desc: "without-prefix",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/.gitattributes":              gitattributesContent,
						"/pointer.lfs":                 lfsPointerContent,
						"/LICENSE":                     "license content",
						"/README.md":                   "readme content",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with-prefix",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "my-prefix",
					},
					expectedContents: map[string]string{
						"my-prefix/":                            "",
						"my-prefix/.gitattributes":              gitattributesContent,
						"my-prefix/pointer.lfs":                 lfsPointerContent,
						"my-prefix/LICENSE":                     "license content",
						"my-prefix/README.md":                   "readme content",
						"my-prefix/subdir/":                     "",
						"my-prefix/subdir/subfile":              "subfile content",
						"my-prefix/subdir/subsubdir/":           "",
						"my-prefix/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with path as blank string",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "",
						Path:       []byte(""),
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/.gitattributes":              gitattributesContent,
						"/pointer.lfs":                 lfsPointerContent,
						"/LICENSE":                     "license content",
						"/README.md":                   "readme content",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with path as nil",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "",
						Path:       nil,
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/.gitattributes":              gitattributesContent,
						"/pointer.lfs":                 lfsPointerContent,
						"/LICENSE":                     "license content",
						"/README.md":                   "readme content",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with path",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "",
						Path:       []byte("subdir"),
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with path and trailing slash",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "",
						Path:       []byte("subdir/"),
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with exclusion",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "",
						Exclude:    [][]byte{[]byte("subdir")},
					},
					expectedContents: map[string]string{
						"/":               "",
						"/.gitattributes": gitattributesContent,
						"/pointer.lfs":    lfsPointerContent,
						"/LICENSE":        "license content",
						"/README.md":      "readme content",
					},
				},
				{
					desc: "with path elision",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "my-prefix",
						ElidePath:  true,
						Path:       []byte("subdir/"),
					},
					expectedContents: map[string]string{
						"my-prefix/":                     "",
						"my-prefix/subfile":              "subfile content",
						"my-prefix/subsubdir/":           "",
						"my-prefix/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with path elision and exclusion",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "my-prefix",
						ElidePath:  true,
						Path:       []byte("subdir/"),
						Exclude:    [][]byte{[]byte("subdir/subsubdir")},
					},
					expectedContents: map[string]string{
						"my-prefix/":        "",
						"my-prefix/subfile": "subfile content",
					},
				},
				{
					desc: "with path elision at root",
					request: &gitalypb.GetArchiveRequest{
						Repository: repo,
						Format:     format,
						CommitId:   commitID.String(),
						Prefix:     "my-prefix",
						ElidePath:  true,
					},
					expectedContents: map[string]string{
						"my-prefix/":                            "",
						"my-prefix/.gitattributes":              gitattributesContent,
						"my-prefix/pointer.lfs":                 lfsPointerContent,
						"my-prefix/LICENSE":                     "license content",
						"my-prefix/README.md":                   "readme content",
						"my-prefix/subdir/":                     "",
						"my-prefix/subdir/subfile":              "subfile content",
						"my-prefix/subdir/subsubdir/":           "",
						"my-prefix/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "without prefix and with LFS blobs",
					request: &gitalypb.GetArchiveRequest{
						Repository:      repo,
						Format:          format,
						CommitId:        commitID.String(),
						Prefix:          "",
						IncludeLfsBlobs: true,
					},
					expectedContents: map[string]string{
						"/":                            "",
						"/.gitattributes":              gitattributesContent,
						"/pointer.lfs":                 "replaced LFS pointer contents",
						"/LICENSE":                     "license content",
						"/README.md":                   "readme content",
						"/subdir/":                     "",
						"/subdir/subfile":              "subfile content",
						"/subdir/subsubdir/":           "",
						"/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
				{
					desc: "with prefix and with LFS blobs",
					request: &gitalypb.GetArchiveRequest{
						Repository:      repo,
						Format:          format,
						CommitId:        commitID.String(),
						Prefix:          "my-prefix",
						IncludeLfsBlobs: true,
					},
					expectedContents: map[string]string{
						"my-prefix/":                            "",
						"my-prefix/.gitattributes":              gitattributesContent,
						"my-prefix/pointer.lfs":                 "replaced LFS pointer contents",
						"my-prefix/LICENSE":                     "license content",
						"my-prefix/README.md":                   "readme content",
						"my-prefix/subdir/":                     "",
						"my-prefix/subdir/subfile":              "subfile content",
						"my-prefix/subdir/subsubdir/":           "",
						"my-prefix/subdir/subsubdir/subsubfile": "subsubfile content",
					},
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					t.Parallel()

					stream, err := client.GetArchive(ctx, tc.request)
					require.NoError(t, err)

					data, err := consumeArchive(stream)
					testhelper.RequireGrpcError(t, tc.expectedErr, err)
					if err != nil {
						require.Empty(t, data)
						return
					}

					require.Equal(t, tc.expectedContents, compressedFileContents(t, format, data))
				})
			}
		})
	}
}

func TestGetArchive_pathInjection(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryService(t)

	// It used to be possible to inject options into `git-archive(1)`, with the worst outcome
	// being that an adversary may create or overwrite arbitrary files in the filesystem in case
	// they passed `--output`.
	//
	// We pretend that the adversary wants to override a fixed file `/non/existent`. In
	// practice, thus would be something more interesting like `/etc/shadow` or `allowed_keys`.
	// We then create a file inside the repository itself that has `--output=/non/existent` as
	// relative path. This is done to verify that git-archive(1) indeed treats the parameter as
	// a path and does not interpret it as an option.
	outputPath := "/non/existent"

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "--output=", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "non", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "existent", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Path: "injected file", Mode: "100644", Content: "injected content"},
				})},
			})},
		})},
	})))

	// And now we fire the request path our bogus path to try and overwrite the output path.
	stream, err := client.GetArchive(ctx, &gitalypb.GetArchiveRequest{
		Repository: repo,
		CommitId:   commitID.String(),
		Prefix:     "",
		Format:     gitalypb.GetArchiveRequest_TAR,
		Path:       []byte("--output=" + outputPath),
	})
	require.NoError(t, err)

	content, err := consumeArchive(stream)
	require.NoError(t, err)

	require.NoFileExists(t, outputPath)
	require.Equal(t, map[string]string{
		"/":                                     "",
		"/--output=/":                           "",
		"/--output=/non/":                       "",
		"/--output=/non/existent/":              "",
		"/--output=/non/existent/injected file": "injected content",
	}, compressedFileContents(t, gitalypb.GetArchiveRequest_TAR, content))
}

func TestGetArchive_environment(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// Intercept commands to git-archive(1) to print the environment. Note that we continue to
	// execute any other Git commands so that the command factory behaves as expected.
	gitCmdFactory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(execEnv gitcmd.ExecutionEnvironment) string {
		return fmt.Sprintf(`#!/usr/bin/env bash
		if [[ ! "$@" =~ "archive" ]]; then
			exec %q "$@"
		fi
		env | grep -E '^GL_|CORRELATION|GITALY_'
		`, execEnv.BinaryPath)
	})

	testcfg.BuildGitalyHooks(t, cfg)

	client, serverSocketPath := runRepositoryService(t, cfg, testserver.WithGitCommandFactory(gitCmdFactory))
	cfg.SocketPath = serverSocketPath

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "some file", Mode: "100644", Content: "some content"},
	))

	correlationID := correlation.SafeRandomID()
	ctx = correlation.ContextWithCorrelation(ctx, correlationID)

	smudgeCfg := smudge.Config{
		GlRepository: gittest.GlRepository,
		Gitlab:       cfg.Gitlab,
		TLS:          cfg.TLS,
		DriverType:   smudge.DriverTypeProcess,
	}

	smudgeEnv, err := smudgeCfg.Environment()
	require.NoError(t, err)

	for _, tc := range []struct {
		desc            string
		includeLFSBlobs bool
		expectedEnv     map[string]struct{}
	}{
		{
			desc:            "without LFS blobs",
			includeLFSBlobs: false,
			expectedEnv: map[string]struct{}{
				"CORRELATION_ID=" + correlationID: {},
			},
		},
		{
			desc:            "with LFS blobs",
			includeLFSBlobs: true,
			expectedEnv: map[string]struct{}{
				"CORRELATION_ID=" + correlationID: {},
				smudgeEnv:                         {},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetArchive(ctx, &gitalypb.GetArchiveRequest{
				Repository:      repo,
				CommitId:        commitID.String(),
				IncludeLfsBlobs: tc.includeLFSBlobs,
			})
			require.NoError(t, err)

			data, err := consumeArchive(stream)
			require.NoError(t, err)

			actualEnv := map[string]struct{}{}
			for _, env := range strings.Split(text.ChompBytes(data), "\n") {
				actualEnv[env] = struct{}{}
			}

			// Check that EnvLogConfiguration is present but don't assert its
			// contents as part of this test.
			foundEnvLogConfiguration := false
			for env := range actualEnv {
				if strings.HasPrefix(env, command.EnvLogConfiguration+"=") {
					foundEnvLogConfiguration = true
					delete(actualEnv, env)
				}
			}

			require.True(t, foundEnvLogConfiguration)

			require.Equal(t, tc.expectedEnv, actualEnv)
		})
	}
}

func TestGetArchive_withCache(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	gitlabURL, cleanup := gitlab.NewTestServer(t, gitlab.TestServerOptions{
		SecretToken: "gitlab-secret-token",
		LfsBody:     "replaced LFS pointer contents",
	})
	t.Cleanup(cleanup)

	gitattributesContent := "*.lfs filter=lfs diff=lfs merge=lfs -text"
	lfsPointerContent := "version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"

	writeCommitToRepo := func(cfg config.Cfg, repoPath string) git.ObjectID {
		treeEntry := gittest.WithTreeEntries(
			gittest.TreeEntry{Path: ".gitattributes", Mode: "100644", Content: gitattributesContent},
			gittest.TreeEntry{Path: "pointer.lfs", Mode: "100644", Content: lfsPointerContent},
			gittest.TreeEntry{Path: "LICENSE", Mode: "100644", Content: "license content"},
			gittest.TreeEntry{Path: "README.md", Mode: "100644", Content: "readme content"},
			gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "subfile", Mode: "100644", Content: "subfile content"},
				{Path: "subsubdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Path: "subsubfile", Mode: "100644", Content: "subsubfile content"},
				})},
			})},
		)
		return gittest.WriteCommit(t, cfg, repoPath, treeEntry)
	}

	for _, format := range []gitalypb.GetArchiveRequest_Format{
		gitalypb.GetArchiveRequest_ZIP,
		gitalypb.GetArchiveRequest_TAR,
		gitalypb.GetArchiveRequest_TAR_GZ,
		gitalypb.GetArchiveRequest_TAR_BZ2,
	} {
		t.Run(format.String(), func(t *testing.T) {
			t.Parallel()

			testCases := []struct {
				desc             string
				numRequests      int
				minOccurrences   int
				packfileCreation []bool
			}{
				{
					desc:           "number of requests less than min occurrences",
					numRequests:    2,
					minOccurrences: 4,
					packfileCreation: []bool{
						true,
						true,
						true,
						true,
					},
				},
				{
					desc:           "number of requests equals min occurrences",
					numRequests:    4,
					minOccurrences: 4,
					packfileCreation: []bool{
						true,
						true,
						true,
						true,
					},
				},
				{
					desc:           "number of requests more than min occurrences",
					numRequests:    6,
					minOccurrences: 2,
					packfileCreation: []bool{
						true,
						true,
						true,
						false,
						false,
						false,
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					archiveCacheDir := filepath.Join(t.TempDir(), "archive_cache")

					cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
						Gitlab: config.Gitlab{
							URL:        gitlabURL,
							SecretFile: gitlab.WriteShellSecretFile(t, testhelper.TempDir(t), "gitlab-secret-token"),
						},
						ArchiveCache: config.StreamCacheConfig{
							Enabled:        true,
							Backpressure:   true,
							Dir:            archiveCacheDir,
							MaxAge:         duration.Duration(time.Second * 600),
							MinOccurrences: tc.minOccurrences,
							Name:           "archive_cache",
						},
					}))
					testcfg.BuildGitalyLFSSmudge(t, cfg)

					tlc := &streamcache.TestLoggingCache{}
					serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
						if _, ok := deps.ArchiveCache.(*streamcache.TestLoggingCache); !ok {
							tlc.Cache = deps.ArchiveCache
							deps.ArchiveCache = tlc
						}
						gitalypb.RegisterRepositoryServiceServer(srv, NewServer(deps))
					})
					cfg.SocketPath = serverSocketPath
					client := newRepositoryClient(t, cfg, serverSocketPath)

					repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
					commitID := writeCommitToRepo(cfg, repoPath)

					for i := 0; i < tc.numRequests; i++ {
						req := &gitalypb.GetArchiveRequest{
							Repository: repo,
							CommitId:   commitID.String(),
							Format:     format,
						}

						stream, err := client.GetArchive(ctx, req)
						require.NoError(t, err)

						data, err := consumeArchive(stream)
						require.NoError(t, err)

						// We ignore the response here because this test only validates
						// that the caching mechanisms is working. However, this call
						// will fail with an error if the archive format is not as
						// expected. So while we don't validate the content, we at least
						// here validate the format. There is another test above that
						// thoroughly tests the content of the archives.
						_ = compressedFileContents(t, format, data)
					}

					require.Len(t, tlc.Entries(), tc.numRequests)

					cacheEntries := tlc.Entries()
					for i := 0; i < len(cacheEntries); i++ {
						require.Equal(t, tc.packfileCreation[i], cacheEntries[i].Created)
					}
				})
			}
		})
	}
}

func compressedFileContents(t *testing.T, format gitalypb.GetArchiveRequest_Format, contents []byte) map[string]string {
	switch format {
	case gitalypb.GetArchiveRequest_TAR_GZ:
		reader, err := gzip.NewReader(bytes.NewReader(contents))
		require.NoError(t, err)

		contents, err = io.ReadAll(reader)
		require.NoError(t, err)
	case gitalypb.GetArchiveRequest_TAR_BZ2:
		reader := bzip2.NewReader(bytes.NewReader(contents))

		var err error
		contents, err = io.ReadAll(reader)
		require.NoError(t, err)
	}

	reader := bytes.NewReader(contents)

	contentByName := map[string]string{}
	switch format {
	case gitalypb.GetArchiveRequest_TAR, gitalypb.GetArchiveRequest_TAR_GZ, gitalypb.GetArchiveRequest_TAR_BZ2:
		tarReader := tar.NewReader(reader)

		for {
			header, err := tarReader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)

			if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeDir {
				continue
			}

			content, err := io.ReadAll(tarReader)
			require.NoError(t, err)

			contentByName[header.Name] = string(content)
		}
	case gitalypb.GetArchiveRequest_ZIP:
		zipReader, err := zip.NewReader(reader, int64(len(contents)))
		require.NoError(t, err)

		for _, file := range zipReader.File {
			reader, err := file.Open()
			require.NoError(t, err)
			defer testhelper.MustClose(t, reader)

			content, err := io.ReadAll(reader)
			require.NoError(t, err)

			contentByName[file.Name] = string(content)

			require.NoError(t, reader.Close())
		}
	default:
		require.FailNow(t, "unsupported archive format: %v", format)
	}

	return contentByName
}

func consumeArchive(stream gitalypb.RepositoryService_GetArchiveClient) ([]byte, error) {
	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	})

	return io.ReadAll(reader)
}
