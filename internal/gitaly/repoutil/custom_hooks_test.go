package repoutil

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/voting"
	"google.golang.org/grpc/peer"
)

func TestGetCustomHooks_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	expectedTarResponse := []string{
		"custom_hooks/",
		"custom_hooks/pre-commit.sample",
		"custom_hooks/prepare-commit-msg.sample",
		"custom_hooks/pre-push.sample",
	}
	require.NoError(t, os.Mkdir(filepath.Join(repoPath, "custom_hooks"), mode.Directory), "Could not create custom_hooks dir")
	for _, fileName := range expectedTarResponse[1:] {
		require.NoError(t, os.WriteFile(filepath.Join(repoPath, fileName), []byte("Some hooks"), mode.Executable), fmt.Sprintf("Could not create %s", fileName))
	}

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, testhelper.NewLogger(t), repoPath, &hooks))

	reader := tar.NewReader(&hooks)
	fileLength := 0
	for {
		file, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		fileLength++
		require.Contains(t, expectedTarResponse, file.Name)
	}
	require.Equal(t, fileLength, len(expectedTarResponse))
}

func TestGetCustomHooks_symlink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	linkTarget := "/var/empty"
	require.NoError(t, os.Symlink(linkTarget, filepath.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, testhelper.NewLogger(t), repoPath, &hooks))

	reader := tar.NewReader(&hooks)
	file, err := reader.Next()
	require.NoError(t, err)

	require.Equal(t, "custom_hooks", file.Name, "tar entry name")
	require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
	require.Equal(t, linkTarget, file.Linkname, "link target")

	_, err = reader.Next()
	require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
}

func TestGetCustomHooks_nonexistentHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, testhelper.NewLogger(t), repoPath, &hooks))

	reader := tar.NewReader(&hooks)
	buf := bytes.NewBuffer(nil)
	_, err := io.Copy(buf, reader)
	require.NoError(t, err)

	require.Empty(t, buf.String(), "Returned stream should be empty")
}

func TestExtractHooks(t *testing.T) {
	t.Parallel()

	umask := testhelper.Umask()

	writeFile := func(writer *tar.Writer, path string, mode fs.FileMode, content string) {
		require.NoError(t, writer.WriteHeader(&tar.Header{
			Name: path,
			Mode: int64(mode),
			Size: int64(len(content)),
		}))
		_, err := writer.Write([]byte(content))
		require.NoError(t, err)
	}

	validArchive := func() io.Reader {
		var buffer bytes.Buffer
		writer := tar.NewWriter(&buffer)
		writeFile(writer, "custom_hooks/pre-receive", fs.ModePerm, "pre-receive content")
		require.NoError(t, writer.WriteHeader(&tar.Header{
			Name: "custom_hooks/subdirectory/",
			Mode: int64(mode.Directory),
		}))
		writeFile(writer, "custom_hooks/subdirectory/supporting-file", mode.File, "supporting-file content")
		writeFile(writer, "ignored_file", fs.ModePerm, "ignored content")
		writeFile(writer, "ignored_directory/ignored_file", fs.ModePerm, "ignored content")
		defer testhelper.MustClose(t, writer)
		return &buffer
	}

	for _, tc := range []struct {
		desc                 string
		archive              io.Reader
		stripPrefix          bool
		expectedState        testhelper.DirectoryState
		expectedErrorMessage string
	}{
		{
			desc:    "empty reader",
			archive: strings.NewReader(""),
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc: "empty archive",
			archive: func() io.Reader {
				var buffer bytes.Buffer
				writer := tar.NewWriter(&buffer)
				defer testhelper.MustClose(t, writer)
				return &buffer
			}(),
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc: "just custom_hooks directory",
			archive: func() io.Reader {
				var buffer bytes.Buffer
				writer := tar.NewWriter(&buffer)
				require.NoError(t, writer.WriteHeader(&tar.Header{
					Name: "custom_hooks/",
					Mode: int64(fs.ModePerm),
				}))
				defer testhelper.MustClose(t, writer)
				return &buffer
			}(),
			expectedState: testhelper.DirectoryState{
				"/":             {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc:    "custom_hooks dir extracted",
			archive: validArchive(),
			expectedState: testhelper.DirectoryState{
				"/":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks":              {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks/pre-receive":  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
				"/custom_hooks/subdirectory": {Mode: mode.Directory},
				"/custom_hooks/subdirectory/supporting-file": {Mode: mode.File, Content: []byte("supporting-file content")},
			},
		},
		{
			desc:        "custom_hooks dir extracted with prefix stripped",
			archive:     validArchive(),
			stripPrefix: true,
			expectedState: testhelper.DirectoryState{
				"/":                             {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/pre-receive":                  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
				"/subdirectory":                 {Mode: mode.Directory},
				"/subdirectory/supporting-file": {Mode: mode.File, Content: []byte("supporting-file content")},
			},
		},
		{
			desc:                 "corrupted archive",
			archive:              strings.NewReader("invalid tar content"),
			expectedErrorMessage: "waiting for tar command completion: exit status",
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			tmpDir := t.TempDir()
			err := ExtractHooks(ctx, testhelper.NewLogger(t), tc.archive, tmpDir, tc.stripPrefix)
			if tc.expectedErrorMessage != "" {
				require.ErrorContains(t, err, tc.expectedErrorMessage)
			} else {
				require.NoError(t, err)
			}
			testhelper.RequireDirectoryState(t, tmpDir, "", tc.expectedState)
		})
	}
}

func TestSetCustomHooks_success(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	txManager := transaction.NewTrackingManager()
	logger := testhelper.NewLogger(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	archivePath := mustCreateCustomHooksArchive(t, ctx, []testFile{
		{name: "pre-commit.sample", content: "foo", mode: 0o755},
		{name: "pre-push.sample", content: "bar", mode: 0o755},
	}, CustomHooksDir)

	file, err := os.Open(archivePath)
	require.NoError(t, err)

	ctx = peer.NewContext(ctx, &peer.Peer{})
	ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)

	require.NoError(t, SetCustomHooks(ctx, logger, locator, txManager, file, repo))

	voteHash, err := newDirectoryVote(filepath.Join(repoPath, CustomHooksDir))
	require.NoError(t, err)

	testhelper.MustClose(t, file)

	expectedVote, err := voteHash.Vote()
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(repoPath, "custom_hooks", "pre-push.sample"))
	require.Equal(t, 3, len(txManager.Votes()))
	assert.Equal(t, voting.Preparing, txManager.Votes()[0].Phase)
	assert.Equal(t, voting.Prepared, txManager.Votes()[1].Phase)
	assert.Equal(t, expectedVote, txManager.Votes()[1].Vote)
	assert.Equal(t, voting.Committed, txManager.Votes()[2].Phase)
}

func TestSetCustomHooks_corruptTar(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	txManager := &transaction.MockManager{}
	logger := testhelper.NewLogger(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	archivePath := mustCreateCorruptHooksArchive(t)

	file, err := os.Open(archivePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, file)

	err = SetCustomHooks(ctx, logger, locator, txManager, file, repo)
	require.ErrorContains(t, err, "extracting hooks: waiting for tar command completion: exit status ")
}

type testFile struct {
	name    string
	content string
	mode    os.FileMode
}

func TestNewDirectoryVote(t *testing.T) {
	// The vote hash depends on the permission bits, so we must make sure that the files we
	// write have the same permission bits on all systems. As the umask can get in our way we
	// reset it to a known value here and restore it after the test. This also means that we
	// cannot parallelize this test.
	currentUmask := syscall.Umask(0)
	defer func() {
		syscall.Umask(currentUmask)
	}()
	syscall.Umask(0o022)

	for _, tc := range []struct {
		desc         string
		files        []testFile
		expectedHash string
	}{
		{
			desc: "generated hash matches",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: mode.Executable},
				{name: "pre-push.sample", content: "bar", mode: mode.Executable},
			},
			expectedHash: "edf456d64829c9519bd692d5f6a9695c4cca7d17",
		},
		{
			desc: "generated hash matches with changed file name",
			files: []testFile{
				{name: "pre-commit.sample.diff", content: "foo", mode: mode.Executable},
				{name: "pre-push.sample", content: "bar", mode: mode.Executable},
			},
			expectedHash: "70c2821e79862399f6fe68c858ec3df20282530a",
		},
		{
			desc: "generated hash matches with changed file content",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: mode.Executable},
				{name: "pre-push.sample", content: "bar.diff", mode: mode.Executable},
			},
			expectedHash: "be06b7a1f74928aa53c5751d8f9b066aa4b09222",
		},
		{
			desc: "generated hash matches with changed file mode",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: mode.File},
				{name: "pre-push.sample", content: "bar", mode: mode.Executable},
			},
			expectedHash: "88e59654d920c86ad17286e59fbce4b70eaaea67",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := mustWriteCustomHookDirectory(t, tc.files, CustomHooksDir)

			voteHash, err := newDirectoryVote(path)
			require.NoError(t, err)

			vote, err := voteHash.Vote()
			require.NoError(t, err)

			hash := vote.String()
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func mustWriteCustomHookDirectory(t *testing.T, files []testFile, dirName string) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)
	hooksPath := filepath.Join(tmpDir, dirName)

	err := os.Mkdir(hooksPath, mode.Directory)
	require.NoError(t, err)

	for _, f := range files {
		err = os.WriteFile(filepath.Join(hooksPath, f.name), []byte(f.content), f.mode)
		require.NoError(t, err)
	}

	return hooksPath
}

func mustCreateCustomHooksArchive(t *testing.T, ctx context.Context, files []testFile, dirName string) string {
	t.Helper()

	hooksPath := mustWriteCustomHookDirectory(t, files, dirName)
	hooksDir := filepath.Dir(hooksPath)

	tmpDir := testhelper.TempDir(t)
	archivePath := filepath.Join(tmpDir, "custom_hooks.tar")

	file, err := os.Create(archivePath)
	require.NoError(t, err)

	err = archive.WriteTarball(ctx, testhelper.NewLogger(t), file, hooksDir, dirName)
	require.NoError(t, err)

	return archivePath
}

func mustCreateCorruptHooksArchive(t *testing.T) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)
	archivePath := filepath.Join(tmpDir, "corrupt_hooks.tar")

	err := os.WriteFile(archivePath, []byte("This is a corrupted tar file"), 0o755)
	require.NoError(t, err)

	return archivePath
}
