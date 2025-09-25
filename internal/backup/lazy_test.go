package backup

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestLazyWriter(t *testing.T) {
	t.Parallel()

	expectedData := make([]byte, 512)
	_, err := rand.Read(expectedData)
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		tempDir := testhelper.TempDir(t)
		tempFilePath := filepath.Join(tempDir, "file")

		w := NewLazyWriter(func() (io.WriteCloser, error) {
			return os.Create(tempFilePath)
		})

		_, err = io.Copy(w, bytes.NewReader([]byte("")))
		require.NoError(t, err)
		require.NoFileExists(t, tempFilePath)

		_, err = io.Copy(w, iotest.OneByteReader(bytes.NewReader(expectedData)))
		require.NoError(t, err)
		require.FileExists(t, tempFilePath)
		require.Equal(t, len(expectedData), w.BytesWritten())

		data, err := os.ReadFile(tempFilePath)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
	})

	t.Run("create error", func(t *testing.T) {
		w := NewLazyWriter(func() (io.WriteCloser, error) {
			return nil, assert.AnError
		})

		n, err := w.Write(make([]byte, 100))
		require.Equal(t, 0, n)
		require.Equal(t, assert.AnError, err)
		require.Equal(t, 0, w.BytesWritten())
	})
}
