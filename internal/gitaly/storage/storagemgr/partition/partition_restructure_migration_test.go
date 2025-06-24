package partition

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

var content = []byte("test content")

func TestPartitionRestructureMigration(t *testing.T) {
	logger := testhelper.SharedLogger(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
	ctx := testhelper.Context(t)
	// Setup test cases
	testCases := []struct {
		name          string
		partitionID   string
		numberOfFiles int
	}{
		{
			name:          "simple partition",
			partitionID:   "12345",
			numberOfFiles: 2,
		},
		{
			name:          "empty partition",
			partitionID:   "99999",
			numberOfFiles: 0,
		},
	}

	// Create the old directory structure
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary directory for testing
			baseDir := testhelper.TempDir(t)
			partitionsDir := filepath.Join(baseDir, "partitions")
			// Create partition path in old structure
			oldPartitionPath := filepath.Join(partitionsDir, storage.ComputePartition(tc.partitionID))

			// Create the wal directory - this is what the migration function processes
			walPath := filepath.Join(oldPartitionPath, "wal")
			require.NoError(t, os.MkdirAll(walPath, mode.Directory))

			if tc.numberOfFiles > 0 {
				// Setup old partition structure using the helper, it will create all necessary parent dirs
				setupDirectory(t, partitionsDir, map[string]bool{
					"/59/94/12345/wal/0000000000000001/RAFT":     false,
					"/59/94/12345/wal/0000000000000001/MANIFEST": false,
					"/59/94/12345/wal/0000000000000002/RAFT":     false,
					"/59/94/12345/wal/0000000000000002/MANIFEST": false,
				})
			}

			dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
			require.NoError(t, err)
			defer dbMgr.Close()

			migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
			require.NoError(t, err)
			// Run the migration
			require.NoError(t, migrator.partitionRestructureMigration())

			_, expectedNewWalPath := pathForMigratedDir("testStorage", partitionsDir, tc.partitionID)
			// Verify the directory structure exists if it should
			if tc.numberOfFiles > 0 {
				// Loop through each sequence directory based on numberOfFiles
				for i := 1; i <= tc.numberOfFiles; i++ {
					seqDir := fmt.Sprintf("%016d", i)
					newSeqPath := filepath.Join(expectedNewWalPath, seqDir)
					oldSeqPath := filepath.Join(walPath, seqDir)

					files := []struct {
						name    string
						oldPath string
						newPath string
					}{
						{"MANIFEST", filepath.Join(oldSeqPath, "MANIFEST"), filepath.Join(newSeqPath, "MANIFEST")},
						{"RAFT", filepath.Join(oldSeqPath, "RAFT"), filepath.Join(newSeqPath, "RAFT")},
					}
					verifyHardlinks(t, files)

					testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
						"/":                                 {Mode: mode.Directory},
						"/59":                               {Mode: mode.Directory},
						"/59/94":                            {Mode: mode.Directory},
						"/59/94/12345":                      {Mode: mode.Directory},
						"/59/94/12345/wal":                  {Mode: mode.Directory},
						"/59/94/12345/wal/0000000000000001": {Mode: mode.Directory},
						"/59/94/12345/wal/0000000000000001/MANIFEST":             {Mode: mode.File, Content: content},
						"/59/94/12345/wal/0000000000000001/RAFT":                 {Mode: mode.File, Content: content},
						"/59/94/12345/wal/0000000000000002":                      {Mode: mode.Directory},
						"/59/94/12345/wal/0000000000000002/MANIFEST":             {Mode: mode.File, Content: content},
						"/59/94/12345/wal/0000000000000002/RAFT":                 {Mode: mode.File, Content: content},
						"/a8":                                                    {Mode: mode.Directory},
						"/a8/42":                                                 {Mode: mode.Directory},
						"/a8/42/testStorage_12345":                               {Mode: mode.Directory},
						"/a8/42/testStorage_12345/wal":                           {Mode: mode.Directory},
						"/a8/42/testStorage_12345/wal/0000000000000001":          {Mode: mode.Directory},
						"/a8/42/testStorage_12345/wal/0000000000000001/RAFT":     {Mode: mode.File, Content: content},
						"/a8/42/testStorage_12345/wal/0000000000000001/MANIFEST": {Mode: mode.File, Content: content},
						"/a8/42/testStorage_12345/wal/0000000000000002":          {Mode: mode.Directory},
						"/a8/42/testStorage_12345/wal/0000000000000002/RAFT":     {Mode: mode.File, Content: content},
						"/a8/42/testStorage_12345/wal/0000000000000002/MANIFEST": {Mode: mode.File, Content: content},
					})
				}
			} else {
				testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
					"/":                            {Mode: mode.Directory},
					"/08":                          {Mode: mode.Directory},
					"/08/10":                       {Mode: mode.Directory},
					"/08/10/testStorage_99999":     {Mode: mode.Directory},
					"/08/10/testStorage_99999/wal": {Mode: mode.Directory},
					"/fd":                          {Mode: mode.Directory},
					"/fd/5f":                       {Mode: mode.Directory},
					"/fd/5f/99999":                 {Mode: mode.Directory},
					"/fd/5f/99999/wal":             {Mode: mode.Directory},
				})
			}
		})
	}
}

func TestPartitionRestructureMigration_Errors(t *testing.T) {
	logger := testhelper.SharedLogger(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
	ctx := testhelper.Context(t)

	t.Run("invalid partition ID", func(t *testing.T) {
		// Create a temporary directory for testing
		baseDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(baseDir, "partitions")
		// Create a partition with an invalid ID (no numeric part)
		setupDirectory(t, partitionsDir, map[string]bool{
			"/xx/yy/invalidPartition/wal/0000000000000001/MANIFEST": false,
		})
		// Run the migration, should skip the directory and not cause an error

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)
		// Run the migration
		require.NoError(t, migrator.partitionRestructureMigration())

		// Verify invalid structure remains unchanged
		testhelper.RequireDirectoryState(t, baseDir, "", testhelper.DirectoryState{
			"/":                                      {Mode: mode.Directory},
			"/partitions":                            {Mode: mode.Directory},
			"/partitions/xx":                         {Mode: mode.Directory},
			"/partitions/xx/yy":                      {Mode: mode.Directory},
			"/partitions/xx/yy/invalidPartition":     {Mode: mode.Directory},
			"/partitions/xx/yy/invalidPartition/wal": {Mode: mode.Directory},
			"/partitions/xx/yy/invalidPartition/wal/0000000000000001":          {Mode: mode.Directory},
			"/partitions/xx/yy/invalidPartition/wal/0000000000000001/MANIFEST": {Mode: mode.File, Content: []byte("test content")},
		})
	})
}

// TestDirectoryStructureMatches tests that our implementation of hashRaftPartitionPath
// matches the expected directory structure seen in production.
func TestDirectoryStructureMatches(t *testing.T) {
	// Create a temporary directory for testing
	baseDir := testhelper.TempDir(t)

	// Example partitions from the provided structure
	testCases := []struct {
		partitionID  string
		expectedDir1 string
		expectedDir2 string
	}{
		{"3703", "65", "d7"},
	}

	for _, tc := range testCases {
		t.Run(tc.partitionID, func(t *testing.T) {
			storageName := "storage"
			raftPartitionPath := storage.GetRaftPartitionName(storageName, tc.partitionID)

			// Calculate hash
			hasher := sha256.New()
			hasher.Write([]byte(raftPartitionPath))
			hash := fmt.Sprintf("%x", hasher.Sum(nil))

			// Check that our implementation produces the expected directories
			assert.Equal(t, tc.expectedDir1, hash[:2], "First level directory should match")
			assert.Equal(t, tc.expectedDir2, hash[2:4], "Second level directory should match")

			// Also test the full function
			path := storage.HashRaftPartitionPath(hasher, baseDir, raftPartitionPath)
			expectedPath := filepath.Join(baseDir, tc.expectedDir1, tc.expectedDir2, raftPartitionPath)
			assert.Equal(t, expectedPath, path)
		})
	}
}

// TestCleanupOldPartitionStructure tests that the cleanup function removes
// the old partition structure correctly after migration
func TestCleanupOldPartitionStructure(t *testing.T) {
	// Create a temporary directory for testing
	baseDir := testhelper.TempDir(t)
	partitionsDir := filepath.Join(baseDir, "partitions")

	logger := testhelper.SharedLogger(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
	ctx := testhelper.Context(t)

	setupDirectory(t, partitionsDir, map[string]bool{
		"/59/94/12345/wal/0000000000000001/RAFT": false,
	})

	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	defer dbMgr.Close()

	migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
	require.NoError(t, err)
	// Run the migration
	require.NoError(t, migrator.partitionRestructureMigration())

	// Verify both old and new structures exist
	testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
		"/":                                      {Mode: mode.Directory},
		"/59":                                    {Mode: mode.Directory},
		"/59/94":                                 {Mode: mode.Directory},
		"/59/94/12345":                           {Mode: mode.Directory},
		"/59/94/12345/wal":                       {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001":      {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		"/a8":                                    {Mode: mode.Directory},
		"/a8/42":                                 {Mode: mode.Directory},
		"/a8/42/testStorage_12345":               {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal":           {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001":      {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
	})

	// Run the cleanup
	require.NoError(t, cleanupOldPartitionStructure(partitionsDir))

	// Verify the old partition structure was removed although the parent dir of the old path remains
	testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
		"/":                            {Mode: mode.Directory},
		"/59":                          {Mode: mode.Directory},
		"/59/94":                       {Mode: mode.Directory}, // parent of partition 12345 still exists
		"/a8":                          {Mode: mode.Directory},
		"/a8/42":                       {Mode: mode.Directory},
		"/a8/42/testStorage_12345":     {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal": {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001":      {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
	})
}

func TestUndoPartitionRestructureMigration(t *testing.T) {
	// Create a temporary directory for testing
	baseDir := testhelper.TempDir(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
	logger := testhelper.SharedLogger(t)
	ctx := testhelper.Context(t)
	partitionsDir := filepath.Join(baseDir, "partitions")
	partitionID := storage.PartitionID(12345)

	// Create the new structure
	newBasePath := getRaftPartitionPath(cfg.Storages[0].Name, partitionID, partitionsDir)
	newWalPath := filepath.Join(newBasePath, "wal")
	seqDir := fmt.Sprintf("%016d", 1)
	newSeqPath := filepath.Join(newWalPath, seqDir)

	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	defer dbMgr.Close()

	migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
	require.NoError(t, err)

	setupDirectory(t, partitionsDir, map[string]bool{
		"/a8/42/testStorage_12345/wal/0000000000000001/RAFT":     false,
		"/a8/42/testStorage_12345/wal/0000000000000001/MANIFEST": false,
	})

	// Run the undo migration
	require.NoError(t, migrator.undoPartitionRestructureMigration())

	oldWalPath := filepath.Join(partitionsDir, storage.ComputePartition(partitionID.String()), "wal")
	oldSeqPath := filepath.Join(oldWalPath, seqDir)

	// Verify the old structure was recreated
	testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
		"/":                                      {Mode: mode.Directory},
		"/59":                                    {Mode: mode.Directory},
		"/59/94":                                 {Mode: mode.Directory},
		"/59/94/12345":                           {Mode: mode.Directory},
		"/59/94/12345/wal":                       {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001":      {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		"/59/94/12345/wal/0000000000000001/MANIFEST": {Mode: mode.File, Content: content},
		"/a8":                          {Mode: mode.Directory},
		"/a8/42":                       {Mode: mode.Directory},
		"/a8/42/testStorage_12345":     {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal": {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001":          {Mode: mode.Directory},
		"/a8/42/testStorage_12345/wal/0000000000000001/RAFT":     {Mode: mode.File, Content: content},
		"/a8/42/testStorage_12345/wal/0000000000000001/MANIFEST": {Mode: mode.File, Content: content},
	})

	// Verify the files that were hardlink live on same inode
	files := []struct {
		name    string
		oldPath string
		newPath string
	}{
		{"MANIFEST", filepath.Join(oldSeqPath, "MANIFEST"), filepath.Join(newSeqPath, "MANIFEST")},
		{"RAFT", filepath.Join(oldSeqPath, "RAFT"), filepath.Join(newSeqPath, "RAFT")},
	}
	verifyHardlinks(t, files)

	// Test cleanup of new structure
	require.NoError(t, cleanupNewPartitionStructure(partitionsDir))

	// Verify new structure is removed, old structure remains
	testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
		"/":                                      {Mode: mode.Directory},
		"/59":                                    {Mode: mode.Directory},
		"/59/94":                                 {Mode: mode.Directory},
		"/59/94/12345":                           {Mode: mode.Directory},
		"/59/94/12345/wal":                       {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001":      {Mode: mode.Directory},
		"/59/94/12345/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		"/59/94/12345/wal/0000000000000001/MANIFEST": {Mode: mode.File, Content: content},
		"/a8":    {Mode: mode.Directory},
		"/a8/42": {Mode: mode.Directory}, // parent of testStorage_12345 still exists
	})
}

func TestUndoPartitionRestructureMigration_Errors(t *testing.T) {
	storageName := "testStorage"
	cfg := testcfg.Build(t, testcfg.WithStorages(storageName))
	logger := testhelper.SharedLogger(t)
	ctx := testhelper.Context(t)
	t.Run("no matching directories", func(t *testing.T) {
		// Create a temporary directory for testing
		baseDir := testhelper.TempDir(t)

		partitionsDir := filepath.Join(baseDir, "partitions")
		// Create an empty state directory with no matching structure
		require.NoError(t, os.MkdirAll(partitionsDir, mode.Directory))

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)

		// Run the undo migration - should complete without error but do nothing
		assert.NoError(t, migrator.undoPartitionRestructureMigration(), "Should not error when no matching directories are found")

		// Verify the directory is still empty
		entries, err := os.ReadDir(partitionsDir)
		assert.NoError(t, err)
		assert.Empty(t, entries, "Directory should remain empty")
	})

	t.Run("invalid directories should be skipped", func(t *testing.T) {
		// Create a temporary directory for testing
		baseDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(baseDir, "partitions")

		// Create a structure that looks like the new structure but with invalid partition name
		setupDirectory(t, partitionsDir, map[string]bool{
			"/ab/cd/invalid_format/wal": true,
		})

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		migrator, err := NewReplicaPartitionMigrator(partitionsDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)

		// Run the undo migration - should skip the invalid directory and complete
		err = migrator.undoPartitionRestructureMigration()
		assert.NoError(t, err, "Should skip invalid directories and complete")

		// Verify no old structure was created
		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                         {Mode: mode.Directory},
			"/ab":                       {Mode: mode.Directory},
			"/ab/cd":                    {Mode: mode.Directory},
			"/ab/cd/invalid_format":     {Mode: mode.Directory},
			"/ab/cd/invalid_format/wal": {Mode: mode.Directory},
		})
	})
}

func TestCleanupNewPartitionStructure(t *testing.T) {
	// Create a temporary directory for testing
	baseDir := testhelper.TempDir(t)
	partitionsDir := filepath.Join(baseDir, "partitions")

	// Create multiple partition structures
	setupDirectory(t, partitionsDir, map[string]bool{
		"/a8/42/testStorage_12345/wal/0000000000000001/MANIFEST": false,
		"/xx/yy/testStorage_67890/wal/0000000000000001/MANIFEST": false,
		"/preserve/this/foobar.txt":                              false,
	})

	// Run the cleanup
	require.NoError(t, cleanupNewPartitionStructure(partitionsDir))

	testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
		"/":                         {Mode: mode.Directory},
		"/a8":                       {Mode: mode.Directory},
		"/a8/42":                    {Mode: mode.Directory}, // parent of testStorage_12345 still exists
		"/xx":                       {Mode: mode.Directory},
		"/xx/yy":                    {Mode: mode.Directory}, // parent of testStorage_67890 still exists
		"/preserve":                 {Mode: mode.Directory},
		"/preserve/this":            {Mode: mode.Directory},
		"/preserve/this/foobar.txt": {Mode: mode.File, Content: content},
	})
}

func TestNewReplicaPartitionMigrator(t *testing.T) {
	t.Parallel()

	// Create test directory and configuration
	tempDir := testhelper.TempDir(t)
	cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
	logger := testhelper.SharedLogger(t)
	ctx := testhelper.Context(t)

	// Create partitions directory
	err := os.MkdirAll(filepath.Join(tempDir, "partitions"), mode.Directory)
	require.NoError(t, err, "Failed to create partitions directory")

	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	defer dbMgr.Close()

	// Create and validate migrator
	migrator, err := NewReplicaPartitionMigrator(tempDir, cfg.Storages[0].Name, dbMgr)
	// Run the migration
	require.NoError(t, migrator.partitionRestructureMigration())

	require.NoError(t, err, "Failed to create migrator")
	require.NotNil(t, migrator, "Migrator should not be nil")

	// Validate migrator properties
	assert.Equal(t, cfg.Storages[0].Name, migrator.storageName, "Incorrect storage name")
	assert.Equal(t, filepath.Join(tempDir, "partitions"), migrator.partitionsDir, "Incorrect partitions directory")
}

func TestPartitionMigrator_Forward(t *testing.T) {
	t.Parallel()

	t.Run("successful migration", func(t *testing.T) {
		t.Parallel()
		// Create a new temp directory for this test
		tempDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(tempDir, "partitions")
		cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))

		logger := testhelper.SharedLogger(t)
		ctx := testhelper.Context(t)

		// Create partitions directory
		err := os.MkdirAll(filepath.Join(tempDir, "partitions"), mode.Directory)
		require.NoError(t, err, "Failed to create partitions directory")

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		// Run the migration
		migrator, err := NewReplicaPartitionMigrator(tempDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)
		require.NoError(t, migrator.partitionRestructureMigration())
		// Setup old partition structure using the helper
		setupDirectory(t, partitionsDir, map[string]bool{
			"xx/yy/123/wal/0000000000000001/RAFT": false,
			"zz/yy/456/wal/0000000000000001/RAFT": false,
		})

		// Run the migration
		require.NoError(t, migrator.Forward())

		// Verify old structure is gone, new structure should exist
		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                          {Mode: mode.Directory},
			"/xx":                        {Mode: mode.Directory},
			"/xx/yy":                     {Mode: mode.Directory}, // parent of partition 123 still remains
			"/zz":                        {Mode: mode.Directory},
			"/zz/yy":                     {Mode: mode.Directory}, // parent of partition 456 still remains
			"/43":                        {Mode: mode.Directory},
			"/43/02":                     {Mode: mode.Directory},
			"/43/02/testStorage_456":     {Mode: mode.Directory},
			"/43/02/testStorage_456/wal": {Mode: mode.Directory},
			"/43/02/testStorage_456/wal/0000000000000001":      {Mode: mode.Directory},
			"/43/02/testStorage_456/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
			"/7e":                        {Mode: mode.Directory},
			"/7e/8d":                     {Mode: mode.Directory},
			"/7e/8d/testStorage_123":     {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal": {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal/0000000000000001":      {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		})

		assertKeyExists(t, migrator.db)
		migrated, err := migrator.CheckMigrationStatus()
		require.NoError(t, err)
		require.True(t, migrated)
	})

	t.Run("can be run multiple times", func(t *testing.T) {
		t.Parallel()

		// Create a new temp directory for this test
		tempDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(tempDir, "partitions")

		cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))

		logger := testhelper.SharedLogger(t)
		ctx := testhelper.Context(t)

		// Create partitions directory
		err := os.MkdirAll(filepath.Join(tempDir, "partitions"), mode.Directory)
		require.NoError(t, err, "Failed to create partitions directory")

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		// Create a migrator for this test
		migrator, err := NewReplicaPartitionMigrator(tempDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)
		// Run the migration
		require.NoError(t, migrator.partitionRestructureMigration())

		// Setup structure with read-only directory to cause cleanup error
		setupDirectory(t, partitionsDir, map[string]bool{
			"xx/yy/123/wal/0000000000000001": true, // partition directory
		})

		// Make the directory read-only on the parent to cause error during cleanup
		oldPartitionPath := filepath.Join(partitionsDir, "xx/yy")
		err = os.Chmod(oldPartitionPath, 0o500) // read-only
		require.NoError(t, err)

		// Run the migration - should complete the migration but fail during cleanup
		require.Error(t, migrator.Forward())

		// Restore permissions for cleanup
		assert.NoError(t, os.Chmod(oldPartitionPath, mode.Directory))

		// Since cleanup failed, old structure still exists
		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                          {Mode: mode.Directory},
			"/xx":                        {Mode: mode.Directory},
			"/xx/yy":                     {Mode: mode.Directory},
			"/xx/yy/123":                 {Mode: mode.Directory}, // expected to not be cleaned up
			"/7e":                        {Mode: mode.Directory},
			"/7e/8d":                     {Mode: mode.Directory},
			"/7e/8d/testStorage_123":     {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal": {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal/0000000000000001": {Mode: mode.Directory},
		})
		assertKeyAbsent(t, migrator.db)
		migrated, err := migrator.CheckMigrationStatus()
		require.NoError(t, err)
		require.False(t, migrated)
		require.NoError(t, migrator.Forward())

		// /xx/yy/123 got cleaned up
		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                          {Mode: mode.Directory},
			"/xx":                        {Mode: mode.Directory},
			"/xx/yy":                     {Mode: mode.Directory},
			"/7e":                        {Mode: mode.Directory},
			"/7e/8d":                     {Mode: mode.Directory},
			"/7e/8d/testStorage_123":     {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal": {Mode: mode.Directory},
			"/7e/8d/testStorage_123/wal/0000000000000001": {Mode: mode.Directory},
		})
		assertKeyExists(t, migrator.db)
		migrated, err = migrator.CheckMigrationStatus()
		require.NoError(t, err)
		require.True(t, migrated)
	})
}

func TestPartitionMigrator_Backward(t *testing.T) {
	t.Parallel()

	t.Run("successful migration", func(t *testing.T) {
		t.Parallel()
		// Create a new temp directory for this test
		tempDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(tempDir, "partitions")
		cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
		logger := testhelper.SharedLogger(t)
		ctx := testhelper.Context(t)

		// Create partitions directory
		err := os.MkdirAll(filepath.Join(tempDir, "partitions"), mode.Directory)
		require.NoError(t, err, "Failed to create partitions directory")

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		// Create a migrator for this test
		migrator, err := NewReplicaPartitionMigrator(tempDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)
		// Setup new partition structure using the helper
		setupDirectory(t, partitionsDir, map[string]bool{
			"xx/yy/testStorage_123/wal/0000000000000001/RAFT": false,
			"zz/yy/testStorage_456/wal/0000000000000001/RAFT": false,
		})

		// Run the migration
		err = migrator.Backward()
		require.NoError(t, err)

		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                                    {Mode: mode.Directory},
			"/xx":                                  {Mode: mode.Directory},
			"/xx/yy":                               {Mode: mode.Directory},
			"/zz":                                  {Mode: mode.Directory},
			"/zz/yy":                               {Mode: mode.Directory},
			"/b3":                                  {Mode: mode.Directory},
			"/b3/a8":                               {Mode: mode.Directory},
			"/b3/a8/456":                           {Mode: mode.Directory},
			"/b3/a8/456/wal":                       {Mode: mode.Directory},
			"/b3/a8/456/wal/0000000000000001":      {Mode: mode.Directory},
			"/b3/a8/456/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
			"/a6":                                  {Mode: mode.Directory},
			"/a6/65":                               {Mode: mode.Directory},
			"/a6/65/123":                           {Mode: mode.Directory},
			"/a6/65/123/wal":                       {Mode: mode.Directory},
			"/a6/65/123/wal/0000000000000001":      {Mode: mode.Directory},
			"/a6/65/123/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		})
		assertKeyAbsent(t, migrator.db)
	})

	t.Run("can be run multiple times", func(t *testing.T) {
		t.Parallel()

		// Create a new temp directory for this test
		tempDir := testhelper.TempDir(t)
		partitionsDir := filepath.Join(tempDir, "partitions")
		cfg := testcfg.Build(t, testcfg.WithStorages("testStorage"))
		logger := testhelper.SharedLogger(t)
		ctx := testhelper.Context(t)

		// Create partitions directory
		err := os.MkdirAll(filepath.Join(tempDir, "partitions"), mode.Directory)
		require.NoError(t, err, "Failed to create partitions directory")

		dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
		require.NoError(t, err)
		defer dbMgr.Close()

		// Create a migrator for this test
		migrator, err := NewReplicaPartitionMigrator(tempDir, cfg.Storages[0].Name, dbMgr)
		require.NoError(t, err)
		// Setup structure with read-only directory to cause cleanup error
		structure := map[string]bool{
			"qq/yy/testStorage_123/wal/0000000000000001/RAFT": false, // partition directory
		}

		// Creates all the parent dirs in the path specified
		setupDirectory(t, partitionsDir, structure)

		// Make the directory read-only on the parent to cause error during cleanup
		newPartitionPath := filepath.Join(partitionsDir, "qq/yy")
		require.NoError(t, os.Chmod(newPartitionPath, 0o500)) // read-only

		// Run the migration - should complete the migration but fail during cleanup
		require.Error(t, migrator.Backward())

		_, err = os.Stat(filepath.Join(partitionsDir, "qq/yy/testStorage_123"))
		assert.NoError(t, err, "Dir should exist as it didn't get cleaned up")

		// Restore permissions for cleanup
		assert.NoError(t, os.Chmod(newPartitionPath, mode.Directory))
		require.NoError(t, migrator.Backward())

		testhelper.RequireDirectoryState(t, partitionsDir, "", testhelper.DirectoryState{
			"/":                                    {Mode: mode.Directory},
			"/qq":                                  {Mode: mode.Directory},
			"/qq/yy":                               {Mode: mode.Directory},
			"/a6":                                  {Mode: mode.Directory},
			"/a6/65":                               {Mode: mode.Directory},
			"/a6/65/123":                           {Mode: mode.Directory},
			"/a6/65/123/wal":                       {Mode: mode.Directory},
			"/a6/65/123/wal/0000000000000001":      {Mode: mode.Directory},
			"/a6/65/123/wal/0000000000000001/RAFT": {Mode: mode.File, Content: content},
		})
		assertKeyAbsent(t, migrator.db)
	})
}

// helper function to setup the directory structure with given files and directories.
func setupDirectory(t *testing.T, baseDir string, files map[string]bool) {
	// First, create all directory paths (including parent directories)
	for path, isDir := range files {
		fullPath := filepath.Join(baseDir, path)

		if isDir {
			err := os.MkdirAll(fullPath, mode.Directory)
			require.NoError(t, err)
		} else {
			// For files, ensure parent directory exists
			parentDir := filepath.Dir(fullPath)
			err := os.MkdirAll(parentDir, mode.Directory)
			require.NoError(t, err)

			// Then create the file
			err = os.WriteFile(fullPath, content, mode.File)
			require.NoError(t, err)
		}
	}
}

// verifyHardlinks checks if files at different paths are hardlink (share the same inode)
func verifyHardlinks(t *testing.T, files []struct {
	name    string
	oldPath string
	newPath string
},
) {
	for _, file := range files {
		t.Run(fmt.Sprintf("Verify hardlink for %s", file.name), func(t *testing.T) {
			// Get file info for both paths
			oldInfo, err := os.Stat(file.oldPath)
			require.NoError(t, err, "Failed to stat original file")

			newInfo, err := os.Stat(file.newPath)
			require.NoError(t, err, "Failed to stat new file")

			// Get system-specific file stats
			oldStat := oldInfo.Sys().(*syscall.Stat_t)
			newStat := newInfo.Sys().(*syscall.Stat_t)

			// Verify both files share the same inode (are hardlink)
			assert.Equal(t, oldStat.Ino, newStat.Ino,
				"Files should share the same inode (hardlink)")

			// Check file modification time is identical
			assert.Equal(t, oldInfo.ModTime(), newInfo.ModTime(), "Files should have identical modification times")

			// Check file size is identical
			assert.Equal(t, oldInfo.Size(), newInfo.Size(), "Files should have identical sizes")
		})
	}
}

func assertKeyExists(t *testing.T, db keyvalue.Store) {
	require.NoError(t, db.View(func(txn keyvalue.ReadWriter) error {
		item, err := txn.Get(migratedReplicaPartitionsKey)
		require.NoError(t, err)
		require.NotNil(t, item) // Ensure we got a valid item
		return nil
	}))
}

func assertKeyAbsent(t *testing.T, db keyvalue.Store) {
	require.NoError(t, db.View(func(txn keyvalue.ReadWriter) error {
		_, getErr := txn.Get(migratedReplicaPartitionsKey)
		require.ErrorIs(t, getErr, badger.ErrKeyNotFound)
		return nil
	}))
}
