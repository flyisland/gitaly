package gitaly

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestBadgerDBCLI(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	testcfg.BuildGitaly(t, cfg)

	db, dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	repository := &gitalypb.Repository{StorageName: "default", RelativePath: "foo/bar.git"}
	repositoryBytes, err := proto.Marshal(repository)
	require.NoError(t, err)

	require.NoError(t, db.Update(func(txn keyvalue.ReadWriter) error {
		if err := txn.Set([]byte("partition_id_seq"), []byte("1")); err != nil {
			return err
		}
		if err := txn.Set([]byte("p/\x00\x00\x00\x00\x00\x00\x00\x09/applied_lsn"), []byte("1")); err != nil {
			return err
		}
		return txn.Set([]byte("p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/storage"), repositoryBytes)
	}))

	require.NoError(t, db.Close())

	t.Run("list command", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "list", "--db-path", dbPath)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err)

		expectedRawKey := "p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/storage"
		expectedOutput := fmt.Sprintf("%q", expectedRawKey)
		require.Contains(t, string(output), expectedOutput)
		require.Contains(t, string(output), "partition_id_seq")
	})

	t.Run("list command with prefix", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "list", "--db-path", dbPath, "--prefix", `p/\x00\x00\x00\x00\x00\x00\x00\x01/`)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err)
		expectedRawKey := "p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/storage"
		expectedOutput := fmt.Sprintf("%q", expectedRawKey)
		require.Contains(t, string(output), expectedOutput)
	})

	t.Run("list command with format-keys flag", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "list", "--db-path", dbPath, "--format-keys")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "p/9/applied_lsn")
	})
	t.Run("get command - existing key", func(t *testing.T) {
		key := `p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/storage`
		cmd := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "get", "--db-path", dbPath, key)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err)

		require.Contains(t, string(output), "default")
	})

	t.Run("get command - non-existing key", func(t *testing.T) {
		key := `p/\x00\x00\x00\x00\x00\x00\x00\x02/kv/hello-1`
		cmd := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "get", "--db-path", dbPath, key)
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), "Key not found")
	})

	t.Run("update command", func(t *testing.T) {
		cmdUpdate := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "update", "--db-path", dbPath, `p/\x00\x00\x00\x00\x00\x00\x00\x01/applied_lsn`, "100")
		output, err := cmdUpdate.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "Updated key:")

		cmdGet := exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "get", "--db-path", dbPath, `p/\x00\x00\x00\x00\x00\x00\x00\x01/applied_lsn`)
		output, err = cmdGet.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "100")

		cmdUpdate = exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "update", "--db-path", dbPath, "partition_id_seq", "10")
		_, err = cmdUpdate.CombinedOutput()
		require.NoError(t, err)

		cmdGet = exec.CommandContext(ctx, cfg.BinaryPath("gitaly"), "db", "get", "--db-path", dbPath, "partition_id_seq")
		output, err = cmdGet.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "10")
	})
}

func setupTestDB(t *testing.T) (keyvalue.Store, string, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)

	dbPath := filepath.Join(tempDir, "badger")

	db, err := keyvalue.NewBadgerStore(testhelper.SharedLogger(t), dbPath)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, os.RemoveAll(tempDir))
	}

	return db, dbPath, cleanup
}
