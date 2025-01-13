package log

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func setupEntryFiles(t *testing.T, files map[string][]byte) string {
	t.Helper()

	logEntryPath := testhelper.TempDir(t)
	for name, value := range files {
		path := filepath.Join(logEntryPath, name)
		require.NoError(t, os.WriteFile(path, value, mode.File))
	}

	return logEntryPath
}

func appendLogEntry(t *testing.T, manager *Manager, files map[string][]byte) storage.LSN {
	nextLSN, err := manager.AppendLogEntry(setupEntryFiles(t, files))
	require.NoError(t, err)

	return nextLSN
}

func newTracker(t *testing.T, consumer storage.LogConsumer) *PositionTracker {
	tracker := NewPositionTracker()
	if consumer != nil {
		require.NoError(t, tracker.Register(ConsumerPosition))
	}
	return tracker
}

func setupLogManager(t *testing.T, ctx context.Context, consumer storage.LogConsumer) *Manager {
	logManager := NewManager("test-storage", 1, testhelper.TempDir(t), testhelper.TempDir(t), consumer, newTracker(t, consumer))
	require.NoError(t, logManager.Initialize(ctx, 0))

	return logManager
}

func waitUntilPruningFinish(t *testing.T, manager *Manager) {
	// Users of log manager are blocked until log pruning task is done. Log pruning runs in parallel and should not
	// conflict other activities of log manager. In this test suite, we need to assert in-between states. Thus, the
	// tests must wait until a task finishes before asserting. The easiest solution is to use errgroup's Wait().
	manager.wg.Wait()
}

func assertDirectoryState(t *testing.T, manager *Manager, expected testhelper.DirectoryState) {
	waitUntilPruningFinish(t, manager)
	testhelper.RequireDirectoryState(t, manager.stateDirectory, "", expected)
}

func TestLogManager_Initialize(t *testing.T) {
	t.Parallel()

	t.Run("initial state without prior log entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(0), logManager.appendedLSN)
		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})

		require.NoError(t, logManager.Close())
	})

	t.Run("existing WAL entries without existing appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		logManager = NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(1), logManager.oldestLSN)
		require.Equal(t, storage.LSN(2), logManager.appendedLSN)
		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
		require.NoError(t, logManager.Close())
	})

	t.Run("existing WAL entries with appliedLSN in-between", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 3; i++ {
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}
		require.NoError(t, logManager.Close())

		logManager = NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 2))

		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 2))

		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(3), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(3), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})
		require.NoError(t, logManager.Close())
	})

	t.Run("existing WAL entries with up-to-date appliedLSN", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 3; i++ {
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}
		require.NoError(t, logManager.Close())

		logManager = NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 3))
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 3))

		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(4), logManager.oldestLSN)
		require.Equal(t, storage.LSN(3), logManager.appendedLSN)
		require.Equal(t, storage.LSN(4), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})

		require.NoError(t, logManager.Close())
	})

	t.Run("double initialization error", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		// Attempt to initialize again
		err := logManager.Initialize(ctx, 0)
		require.Error(t, err)
		require.Equal(t, "log manager already initialized", err.Error())

		require.NoError(t, logManager.Close())
	})

	t.Run("context canceled before initialization", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(testhelper.Context(t))
		cancel() // Cancel the context before initializing
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		err := logManager.Initialize(ctx, 0)

		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	})

	t.Run("context canceled after initialization", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(testhelper.Context(t))
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		// Cancel the context after initialization
		cancel()

		// Check if the manager's context was also canceled
		require.EqualError(t, logManager.ctx.Err(), context.Canceled.Error())
		require.NoError(t, logManager.Close())
	})
}

func TestLogManager_PruneLogEntries(t *testing.T) {
	t.Parallel()

	t.Run("no entries to remove", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Set this entry as applied
		waitUntilPruningFinish(t, logManager)

		require.Equal(t, storage.LSN(1), logManager.oldestLSN)

		// Assert on-disk state
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("remove single applied entry", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject a single log entry
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})

		// Before removal
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
		})

		// Set this entry as applied
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 1))
		waitUntilPruningFinish(t, logManager)

		// After removal
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("retain entry due to low-water mark constraint", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, &mockLogConsumer{})

		// Inject multiple log entries
		for i := 0; i < 3; i++ {
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		// Set the applied LSN to 2
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 2))
		// Manually set the consumer's position to the first entry, forcing low-water mark to retain it
		require.NoError(t, logManager.AcknowledgePosition(ConsumerPosition, 1))

		// Before removal
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		// Set the applied LSN to 2
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 2))
		// Manually set the consumer's position to the first entry, forcing low-water mark to retain it
		require.NoError(t, logManager.AcknowledgePosition(ConsumerPosition, 1))

		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(2), logManager.oldestLSN)

		// Assert on-disk state to ensure no entries were removed
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("remove multiple applied entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject multiple log entries
		for i := 0; i < 5; i++ {
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		// Before removal
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
			"/wal/0000000000005":   {Mode: mode.Directory},
			"/wal/0000000000005/1": {Mode: mode.File, Content: []byte("content-5")},
		})

		// Set the applied LSN to 3, allowing the first three entries to be pruned
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 3))
		waitUntilPruningFinish(t, logManager)

		// Ensure only entries starting from LSN 4 are retained
		require.Equal(t, storage.LSN(4), logManager.oldestLSN)

		// Assert on-disk state after removals
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
			"/wal/0000000000005":   {Mode: mode.Directory},
			"/wal/0000000000005/1": {Mode: mode.File, Content: []byte("content-5")},
		})
	})

	t.Run("log entry pruning fails", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 5; i++ {
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
		}

		infectedPath := logManager.GetEntryPath(3)

		// Get the current permissions
		info, err := os.Stat(infectedPath)
		require.NoError(t, err)
		originalMode := info.Mode()

		// Mark log entry 3 ready-only
		require.NoError(t, os.Chmod(infectedPath, 0o444))

		// The error is notified via notification queue so that the caller can act accordingly
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 5))
		require.ErrorContains(t, <-logManager.GetNotificationQueue(), "permission denied")

		require.NoError(t, logManager.Close())

		// Restore the permission to assert the state
		require.NoError(t, os.Chmod(infectedPath, originalMode))
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
			"/wal/0000000000005":   {Mode: mode.Directory},
			"/wal/0000000000005/1": {Mode: mode.File, Content: []byte("content-5")},
		})

		// Restart the manager
		logManager = NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 5))
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 5))

		waitUntilPruningFinish(t, logManager)
		testhelper.RequireDirectoryState(t, logManager.stateDirectory, "", testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("trigger log entry pruning concurrently", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		stagingDir := testhelper.TempDir(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, stagingDir, stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		var wg sync.WaitGroup

		const totalLSN = 25

		// One producer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < totalLSN; i++ {
				appendLogEntry(t, logManager, map[string][]byte{"1": []byte(fmt.Sprintf("content-%d", i+1))})
			}
		}()

		// Three goroutines spams acknowledgement constantly.
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if logManager.AppendedLSN() == totalLSN {
						return
					}
					require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, logManager.AppendedLSN()))
				}
			}()
		}
		wg.Wait()
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, logManager.AppendedLSN()))

		require.NoError(t, logManager.Close())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})
}

func TestLogManager_AppendLogEntry(t *testing.T) {
	t.Parallel()

	t.Run("append a log entry with a single file", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
		})
	})

	t.Run("append a log entry with multiple files", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, logManager, map[string][]byte{
			"1": []byte("content-1"),
			"2": []byte("content-2"),
			"3": []byte("content-3"),
		})

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000001/2": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000001/3": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("append multiple entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2-1"), "2": []byte("content-2-2")})
		appendLogEntry(t, logManager, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(3))

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2-1")},
			"/wal/0000000000002/2": {Mode: mode.File, Content: []byte("content-2-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
		})
	})
}

func TestLogManager_CompareAndAppendLogEntry(t *testing.T) {
	t.Parallel()

	t.Run("compare and append a log entry with a single file", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		lsn, err := logManager.CompareAndAppendLogEntry(
			storage.LSN(1),
			setupEntryFiles(t, map[string][]byte{
				"1": []byte("content-1"),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(1))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
		})
	})

	t.Run("compare and append a log entry with multiple files", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		lsn, err := logManager.CompareAndAppendLogEntry(
			storage.LSN(1),
			setupEntryFiles(t, map[string][]byte{
				"1": []byte("content-1"),
				"2": []byte("content-2"),
				"3": []byte("content-3"),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(1))

		require.Equal(t, logManager.appendedLSN, storage.LSN(1))
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000001/2": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000001/3": {Mode: mode.File, Content: []byte("content-3")},
		})
	})

	t.Run("append multiple entries", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		require.Equal(t, logManager.appendedLSN, storage.LSN(0))

		lsn, err := logManager.CompareAndAppendLogEntry(
			storage.LSN(1),
			setupEntryFiles(t, map[string][]byte{
				"1": []byte("content-1"),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(1))

		lsn, err = logManager.CompareAndAppendLogEntry(
			storage.LSN(2),
			setupEntryFiles(t, map[string][]byte{
				"1": []byte("content-2-1"),
				"2": []byte("content-2-2"),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(2))

		lsn, err = logManager.CompareAndAppendLogEntry(
			storage.LSN(3),
			setupEntryFiles(t, map[string][]byte{}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(3))

		require.Equal(t, logManager.appendedLSN, storage.LSN(3))
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2-1")},
			"/wal/0000000000002/2": {Mode: mode.File, Content: []byte("content-2-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
		})
	})

	t.Run("compare and append a log entry at LSN 0", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		require.Equal(t, logManager.appendedLSN, storage.LSN(1))

		lsn, err := logManager.CompareAndAppendLogEntry(
			storage.LSN(0),
			setupEntryFiles(t, map[string][]byte{
				"1": []byte("content-2"),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, lsn, storage.LSN(2))

		require.Equal(t, logManager.appendedLSN, storage.LSN(2))
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	for _, invalidCase := range []struct {
		desc     string
		inputLSN storage.LSN
	}{
		{
			"compare and append a log entry at a LSN < appended LSN", 2,
		},
		{
			"compare and append a log entry at a LSN == appended LSN", 3,
		},
		// Only appending at LSN 4 is successful.
		{
			"compare and append a log entry at a LSN > appended LSN + 1", 5,
		},
	} {
		t.Run(invalidCase.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			logManager := setupLogManager(t, ctx, nil)

			appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})
			appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")})

			require.Equal(t, logManager.appendedLSN, storage.LSN(3))

			_, err := logManager.CompareAndAppendLogEntry(
				invalidCase.inputLSN,
				setupEntryFiles(t, map[string][]byte{
					"1": []byte("should-not-append"),
				}),
			)
			require.ErrorIs(t, err, ErrLogEntryNotAppended)

			require.Equal(t, logManager.appendedLSN, storage.LSN(3))
			assertDirectoryState(t, logManager, testhelper.DirectoryState{
				"/":                    {Mode: mode.Directory},
				"/wal":                 {Mode: mode.Directory},
				"/wal/0000000000001":   {Mode: mode.Directory},
				"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
				"/wal/0000000000002":   {Mode: mode.Directory},
				"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
				"/wal/0000000000003":   {Mode: mode.Directory},
				"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			})
		})
	}
}

type mockLogConsumer struct {
	mu        sync.Mutex
	positions [][]storage.LSN
}

func (c *mockLogConsumer) NotifyNewEntries(storageName string, partitionID storage.PartitionID, oldestLSN, appendedLSN storage.LSN) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.positions = append(c.positions, []storage.LSN{oldestLSN, appendedLSN})
}

func TestLogManager_Positions(t *testing.T) {
	ctx := testhelper.Context(t)

	simulatePositions := func(t *testing.T, logManager *Manager, consumed storage.LSN, applied storage.LSN) {
		require.NoError(t, logManager.AcknowledgePosition(ConsumerPosition, consumed))
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, applied))
	}

	t.Run("consumer pos is set to 0 after initialized", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		require.Equal(t, [][]storage.LSN(nil), mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("notify consumer after restart", func(t *testing.T) {
		stateDir := testhelper.TempDir(t)

		// Before restart
		mockConsumer := &mockLogConsumer{}

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, mockConsumer, newTracker(t, mockConsumer))
		require.NoError(t, logManager.Initialize(ctx, 0))

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		// Apply to 3 but consume to 1
		simulatePositions(t, logManager, 1, 2)
		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		// Inject 3, 4
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-4")})

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
			"/wal/0000000000004":   {Mode: mode.Directory},
			"/wal/0000000000004/1": {Mode: mode.File, Content: []byte("content-4")},
		})

		// Restart the log consumer.
		mockConsumer = &mockLogConsumer{}
		logManager = NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, mockConsumer, newTracker(t, mockConsumer))
		require.NoError(t, logManager.Initialize(ctx, 2))

		// Notify consumer to consume from 2 -> 4
		require.Equal(t, [][]storage.LSN{{2, 4}}, mockConsumer.positions)

		// Both consumer and applier catch up.
		simulatePositions(t, logManager, 4, 4)
		waitUntilPruningFinish(t, logManager)

		// All log entries are pruned at this point. The consumer should not be notified again.
		require.Equal(t, [][]storage.LSN{{2, 4}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(5), logManager.LowWaterMark())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("unacknowledged entries are not pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 0, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("acknowledged entries got pruned", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 1, 2)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("entries consumed faster than applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		simulatePositions(t, logManager, 2, 0)

		require.Equal(t, [][]storage.LSN{{1, 1}, {1, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
		})
	})

	t.Run("acknowledge entries one by one", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		simulatePositions(t, logManager, 1, 1)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})
		simulatePositions(t, logManager, 2, 2)

		logManager.pruneLogEntries()

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})

		// The oldest LSN changes after each acknowledgement
		require.Equal(t, [][]storage.LSN{{1, 1}, {2, 2}}, mockConsumer.positions)
		require.Equal(t, storage.LSN(3), logManager.LowWaterMark())
	})

	t.Run("append while consumer is busy with prior entries", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		simulatePositions(t, logManager, 0, 1)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})
		simulatePositions(t, logManager, 0, 2)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")})
		simulatePositions(t, logManager, 3, 3)

		require.Equal(t, storage.LSN(4), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("acknowledged entries not pruned if not applied", func(t *testing.T) {
		mockConsumer := &mockLogConsumer{}
		logManager := setupLogManager(t, ctx, mockConsumer)

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")})

		// 2 and 3 are not applied, hence kept intact.
		simulatePositions(t, logManager, 3, 1)

		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		simulatePositions(t, logManager, 3, 3)
		require.Equal(t, storage.LSN(4), logManager.LowWaterMark())

		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})

	t.Run("more position types apart from defaults are supported", func(t *testing.T) {
		consumer := &mockLogConsumer{}

		tracker := newTracker(t, consumer)
		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), testhelper.TempDir(t), consumer, tracker)

		t1 := storage.PositionType{Name: "TestPosition1", ShouldNotify: false}
		t2 := storage.PositionType{Name: "TestPosition2", ShouldNotify: false}
		require.NoError(t, tracker.Register(t1))
		require.NoError(t, tracker.Register(t2))

		require.NoError(t, logManager.Initialize(ctx, 0))

		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")})

		// Consumed = 3, Applied = 2, TestPosition1 = 1, testPosition2 = 1
		simulatePositions(t, logManager, 3, 2)

		require.Equal(t, storage.LSN(1), logManager.LowWaterMark())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000001":   {Mode: mode.Directory},
			"/wal/0000000000001/1": {Mode: mode.File, Content: []byte("content-1")},
			"/wal/0000000000002":   {Mode: mode.Directory},
			"/wal/0000000000002/1": {Mode: mode.File, Content: []byte("content-2")},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		// Consumed = 3, Applied = 3, TestPosition1 = 2, testPosition2 = 2
		require.NoError(t, logManager.AcknowledgePosition(t1, 2))
		require.NoError(t, logManager.AcknowledgePosition(t2, 2))
		simulatePositions(t, logManager, 3, 3)

		require.Equal(t, storage.LSN(3), logManager.LowWaterMark())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":                    {Mode: mode.Directory},
			"/wal":                 {Mode: mode.Directory},
			"/wal/0000000000003":   {Mode: mode.Directory},
			"/wal/0000000000003/1": {Mode: mode.File, Content: []byte("content-3")},
		})

		// All positions are 3
		require.NoError(t, logManager.AcknowledgePosition(t1, 3))
		require.NoError(t, logManager.AcknowledgePosition(t2, 3))
		simulatePositions(t, logManager, 3, 3)

		require.Equal(t, storage.LSN(4), logManager.LowWaterMark())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/":    {Mode: mode.Directory},
			"/wal": {Mode: mode.Directory},
		})
	})
}

func TestLogManager_Close(t *testing.T) {
	t.Parallel()

	t.Run("close uninitialized manager", func(t *testing.T) {
		t.Parallel()
		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), testhelper.TempDir(t), nil, newTracker(t, nil))

		// Attempt to close the manager before initialization
		err := logManager.Close()
		require.Error(t, err)
		require.Equal(t, "log manager has not been initialized", err.Error())
	})

	t.Run("close after initialization", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), testhelper.TempDir(t), nil, newTracker(t, nil))

		// Properly initialize the manager
		require.NoError(t, logManager.Initialize(ctx, 0))

		// Close the manager
		require.NoError(t, logManager.Close())

		// Verify the context has been canceled
		require.EqualError(t, logManager.ctx.Err(), context.Canceled.Error())
	})

	t.Run("close after appending log entries", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Append some log entries
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"2": []byte("content-2")})

		// Close the manager
		require.NoError(t, logManager.Close())

		// Verify the context has been canceled
		require.EqualError(t, logManager.ctx.Err(), context.Canceled.Error())

		// Further appending should fail due to the canceled context
		_, err := logManager.AppendLogEntry(testhelper.TempDir(t))
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	})

	t.Run("close waits for pruning tasks", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)
		logManager := setupLogManager(t, ctx, nil)

		// Inject log entries
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"2": []byte("content-2")})

		// Trigger pruning
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 2))

		// Close the manager and ensure all tasks are completed
		require.NoError(t, logManager.Close())

		// Verify the oldestLSN after pruning
		require.Equal(t, storage.LSN(3), logManager.oldestLSN)
	})
}

func TestLogManager_NotifyNewEntries(t *testing.T) {
	t.Parallel()

	t.Run("notification channel is empty by default", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		select {
		case <-logManager.GetNotificationQueue():
			require.Fail(t, "notification must be empty by default")
		default:
		}
	})

	t.Run("notify new entries sequentially via the notification channel", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 10; i++ {
			logManager.NotifyNewEntries()
			select {
			case s := <-logManager.GetNotificationQueue():
				require.Nilf(t, s, "new entry signal must be a nil")
			default:
				require.Fail(t, "notification is empty")
			}
		}
	})

	t.Run("notify multiple entries at once via the notification channel", func(t *testing.T) {
		t.Parallel()

		ctx := testhelper.Context(t)
		stateDir := testhelper.TempDir(t)

		logManager := NewManager("test-storage", 1, testhelper.TempDir(t), stateDir, nil, newTracker(t, nil))
		require.NoError(t, logManager.Initialize(ctx, 0))

		for i := 0; i < 10; i++ {
			logManager.NotifyNewEntries()
		}

		// After notifying, the listener of notification queue receives only one signal
		select {
		case s := <-logManager.GetNotificationQueue():
			require.Nilf(t, s, "new entry signal must be a nil")
		default:
			require.Fail(t, "notification is empty")
		}

		// Now the queue is empty
		select {
		case <-logManager.GetNotificationQueue():
			require.Fail(t, "notification must be empty now")
		default:
		}
	})
}

func TestLogManager_DeleteTrailingLogEntries(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	t.Run("reject deletion if requested LSN is below low water mark", func(t *testing.T) {
		// Setup a log manager without a consumer.
		logManager := setupLogManager(t, ctx, nil)

		// Append two log entries.
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")})
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")})

		// Acknowledge the first entry as applied so that LowWaterMark() becomes 2.
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 1))
		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		// Attempt deletion starting from LSN 1 (which is below the low water mark).
		err := logManager.DeleteTrailingLogEntries(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requested LSN is below the low water mark")

		// This method does not delete any log entries. Log entry 1 was deleted by the background
		// pruning task, though.
		require.Equal(t, storage.LSN(2), logManager.AppendedLSN())
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/": {
				Mode: mode.Directory,
			},
			"/wal": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002/1": {
				Mode:    mode.File,
				Content: []byte("content-2"),
			},
		})
	})

	t.Run("successfully delete tail entries", func(t *testing.T) {
		// Setup a log manager without a consumer.
		logManager := setupLogManager(t, ctx, nil)

		// Append four log entries.
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")}) // LSN 1
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")}) // LSN 2
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")}) // LSN 3
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-4")}) // LSN 4

		// Acknowledge the first two entries as applied.
		// That makes the low water mark = 2
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 1))
		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		// Delete entries starting from LSN 3 (which is >= low water mark).
		err := logManager.DeleteTrailingLogEntries(3)
		require.NoError(t, err)

		// appendedLSN should now be lowered to one before the 'from' LSN.
		require.Equal(t, storage.LSN(2), logManager.AppendedLSN())

		// Assert that only LSN 2 remain in the on-disk WAL. 1 was removed by background pruning task.
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/": {
				Mode: mode.Directory,
			},
			"/wal": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002/1": {
				Mode:    mode.File,
				Content: []byte("content-2"),
			},
		})
	})

	t.Run("requested LSN above appended LSN (nothing to delete)", func(t *testing.T) {
		// Setup a log manager without a consumer.
		logManager := setupLogManager(t, ctx, nil)

		// Append two log entries.
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-A")}) // LSN 1
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-B")}) // LSN 2

		// Call DeleteLogEntriesFrom with a LSN greater than the current appendedLSN.
		err := logManager.DeleteTrailingLogEntries(5)
		require.NoError(t, err)

		// appendedLSN should remain unchanged.
		require.Equal(t, storage.LSN(2), logManager.appendedLSN)

		// Assert that the WAL still contains both log entries.
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/": {
				Mode: mode.Directory,
			},
			"/wal": {
				Mode: mode.Directory,
			},
			"/wal/0000000000001": {
				Mode: mode.Directory,
			},
			"/wal/0000000000001/1": {
				Mode:    mode.File,
				Content: []byte("content-A"),
			},
			"/wal/0000000000002": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002/1": {
				Mode:    mode.File,
				Content: []byte("content-B"),
			},
		})
	})

	t.Run("concurrent deletion invocation", func(t *testing.T) {
		// Setup a log manager without a consumer.
		logManager := setupLogManager(t, ctx, nil)

		// Append four log entries.
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-1")}) // LSN 1
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-2")}) // LSN 2
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-3")}) // LSN 3
		appendLogEntry(t, logManager, map[string][]byte{"1": []byte("content-4")}) // LSN 4

		// Acknowledge LSN 1 as applied so that low water mark = 2.
		require.NoError(t, logManager.AcknowledgePosition(AppliedPosition, 1))
		waitUntilPruningFinish(t, logManager)
		require.Equal(t, storage.LSN(2), logManager.LowWaterMark())

		// Launch concurrent deletion calls.
		const numRoutines = 3
		var wg sync.WaitGroup
		errCh := make(chan error, numRoutines)

		for i := 1; i <= numRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// All routines try deleting from LSN 3.
				errCh <- logManager.DeleteTrailingLogEntries(3)
			}()
		}
		wg.Wait()
		close(errCh)

		// Ensure that all calls returned without error.
		for err := range errCh {
			require.NoError(t, err)
		}

		// appendedLSN should now be 2 because deletion starting at LSN 3 should remove tail entries.
		require.Equal(t, storage.LSN(2), logManager.AppendedLSN())

		// Assert that only entries LSN 2.
		assertDirectoryState(t, logManager, testhelper.DirectoryState{
			"/": {
				Mode: mode.Directory,
			},
			"/wal": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002": {
				Mode: mode.Directory,
			},
			"/wal/0000000000002/1": {
				Mode:    mode.File,
				Content: []byte("content-2"),
			},
		})
	})
}
