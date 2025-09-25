package gitaly

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/mode"
)

func TestUnpackAuxiliaryBinaries_success(t *testing.T) {
	destinationDir := t.TempDir()
	require.NoError(t, UnpackAuxiliaryBinaries(destinationDir, func(string) bool { return true }))

	entries, err := os.ReadDir(destinationDir)
	require.NoError(t, err)

	require.Greater(t, len(entries), 1, "expected multiple packed binaries present")

	for _, entry := range entries {
		fileInfo, err := entry.Info()
		require.NoError(t, err)
		require.Equal(t, fileInfo.Mode(), mode.Executable)

		sourceBinary, err := os.ReadFile(filepath.Join(buildDir, fileInfo.Name()))
		require.NoError(t, err)

		unpackedBinary, err := os.ReadFile(filepath.Join(destinationDir, fileInfo.Name()))
		require.NoError(t, err)

		require.Equal(t, sourceBinary, unpackedBinary, "unpacked binary does not match the source binary")
	}
}

func TestUnpackAuxiliaryBinaries_alreadyExists(t *testing.T) {
	destinationDir := t.TempDir()
	existingFile := filepath.Join(destinationDir, "gitaly-hooks")
	require.NoError(t, os.WriteFile(existingFile, []byte("existing file"), mode.File))

	err := UnpackAuxiliaryBinaries(destinationDir, func(_ string) bool { return true })
	require.EqualError(t, err, fmt.Sprintf(`open %s: file exists`, existingFile), "expected unpacking to fail if destination binary already existed")
}
