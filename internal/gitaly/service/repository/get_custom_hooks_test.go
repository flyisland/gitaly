package repository

import (
	"archive/tar"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func TestGetCustomHooks_successful(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) io.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) io.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				})
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) io.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				})
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryService(t)
			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			customHookFiles := []string{
				"custom_hooks/pre-commit.sample",
				"custom_hooks/prepare-commit-msg.sample",
				"custom_hooks/pre-push.sample",
			}

			// Convert the string paths to testFile structs for the archive
			var hookFiles []testFile
			for _, fileName := range customHookFiles {
				hookFiles = append(hookFiles, testFile{
					name:    strings.TrimPrefix(fileName, "custom_hooks/"),
					content: "Some hooks",
					mode:    mode.Executable,
				})
			}

			archivePath := mustCreateCustomHooksArchive(t, ctx, hookFiles, repoutil.CustomHooksDir)
			archiveFile, err := os.Open(archivePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, archiveFile)

			stream, err := client.SetCustomHooks(ctx)
			require.NoError(t, err)

			writer := streamio.NewWriter(func(p []byte) error {
				return stream.Send(&gitalypb.SetCustomHooksRequest{
					Repository: repo,
					Data:       p,
				})
			})

			_, err = io.Copy(writer, archiveFile)
			require.NoError(t, err)
			_, err = stream.CloseAndRecv()
			require.NoError(t, err)

			reader := tc.streamReader(t, ctx, repo, client)
			expected := testhelper.DirectoryState{
				"custom_hooks": {
					Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir,
				},
			}
			for _, fileName := range customHookFiles {
				expected[fileName] = testhelper.DirectoryEntry{
					Mode:    archive.TarFileMode | archive.ExecuteMode,
					Content: []byte("Some hooks"),
				}
			}
			testhelper.RequireTarState(t, reader, expected)
		})
	}
}

func TestGetCustomHooks_symlink(t *testing.T) {
	testhelper.SkipWithWAL(t, `
The repositories generally shouldn't have symlinks in them and the TransactionManager never writes any
symlinks. Symlinks are not supported when creating a snapshot of the repository. Disable the test as it
doesn't seem to test a realistic scenario.`)

	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) *tar.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryService(t)
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			linkTarget := "/var/empty"
			require.NoError(t, os.Symlink(linkTarget, filepath.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

			reader := tc.streamReader(t, ctx, repo, client)

			file, err := reader.Next()
			require.NoError(t, err)

			require.Equal(t, "custom_hooks", file.Name, "tar entry name")
			require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
			require.Equal(t, linkTarget, file.Linkname, "link target")

			_, err = reader.Next()
			require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
		})
	}
}

func TestGetCustomHooks_nonexistentHooks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) io.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) io.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				})
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) io.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				})
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryService(t)
			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			buf, err := io.ReadAll(tc.streamReader(t, ctx, repo, client))
			require.NoError(t, err)
			require.Empty(t, buf, "Returned stream should be empty")
		})
	}
}

func TestGetCustomHooks_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.GetCustomHooksRequest
		expectedErr error
	}{
		{
			desc:        "repository not provided",
			req:         &gitalypb.GetCustomHooksRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetCustomHooks(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestBackupCustomHooks_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryService(t)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.BackupCustomHooksRequest
		expectedErr error
	}{
		{
			desc:        "repository not provided",
			req:         &gitalypb.BackupCustomHooksRequest{Repository: nil},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			stream, err := client.BackupCustomHooks(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
