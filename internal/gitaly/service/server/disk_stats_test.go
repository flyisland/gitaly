package server

import (
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

func TestStorageDiskStatistics(t *testing.T) {
	storageOpt := testcfg.WithStorages("default")
	if !testhelper.IsWALEnabled() {
		// The test is testing a broken storage by deleting the storage after initializing it.
		// This causes problems with WAL as the disk state expected to be present by the database
		// and the transaction manager suddenly don't exist. Skip the test here with WAL and rely
		// on the storage implementation to handle broken storage on initialization.
		storageOpt = testcfg.WithStorages("default", "broken")
	}

	cfg := testcfg.Build(t, storageOpt)

	addr := runServer(t, cfg)

	if !testhelper.IsWALEnabled() {
		require.NoError(t, os.RemoveAll(cfg.Storages[1].Path), "second storage needs to be invalid")
	}

	client := newServerClient(t, addr)
	ctx := testhelper.Context(t)

	c, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
	require.NoError(t, err)

	expectedStorages := len(cfg.Storages)
	if testhelper.IsPraefectEnabled() && !testhelper.IsWALEnabled() {
		// Praefect does not virtualize StorageDiskStatistics correctly. It proxies the call to each Gitaly
		// and returns the results of all of their storages. However, not all storages on a Gitaly node are
		// necessarily part of a virtual storage. Likewise, Praefect should not expose the individual storages
		// that make up a virtual storage externally but should instead provide a single result for a virtual
		// storage.
		//
		// In our test setup, we have two storages on a single Gitaly node. Both of the storages are the only
		// storage in their own virtual storages. Praefect returns statistics for all storages on a Gitaly node
		// that is part of a virtual storage, so it ends up returning both results for both physical storages
		// twice.
		expectedStorages = 2 * len(cfg.Storages)
	}

	require.Len(t, c.GetStorageStatuses(), expectedStorages)

	// used and available space may change so we check if it roughly matches (+/- 1GB)
	avail, used := getSpaceStats(t, cfg.Storages[0].Path)
	approxEqual(t, c.GetStorageStatuses()[0].GetAvailable(), avail)
	approxEqual(t, c.GetStorageStatuses()[0].GetUsed(), used)
	require.Equal(t, cfg.Storages[0].Name, c.GetStorageStatuses()[0].GetStorageName())

	if !testhelper.IsWALEnabled() {
		require.Equal(t, int64(0), c.GetStorageStatuses()[1].GetAvailable())
		require.Equal(t, int64(0), c.GetStorageStatuses()[1].GetUsed())
		require.Equal(t, cfg.Storages[1].Name, c.GetStorageStatuses()[1].GetStorageName())
	}

	if testhelper.IsPraefectEnabled() && !testhelper.IsWALEnabled() {
		// This is incorrect behavior caused by the bug explained above.
		approxEqual(t, c.GetStorageStatuses()[2].GetAvailable(), avail)
		approxEqual(t, c.GetStorageStatuses()[2].GetUsed(), used)
		require.Equal(t, cfg.Storages[0].Name, c.GetStorageStatuses()[2].GetStorageName())

		require.Equal(t, int64(0), c.GetStorageStatuses()[3].GetAvailable())
		require.Equal(t, int64(0), c.GetStorageStatuses()[3].GetUsed())
		require.Equal(t, cfg.Storages[1].Name, c.GetStorageStatuses()[3].GetStorageName())
	}
}

func approxEqual(t *testing.T, a, b int64) {
	const eps = 1024 * 1024 * 1024
	require.Truef(t, math.Abs(float64(a-b)) < eps, "expected %d to be equal %d with epsilon %d", a, b, eps)
}

func getSpaceStats(t *testing.T, path string) (available int64, used int64) {
	var stats unix.Statfs_t
	err := unix.Statfs(path, &stats)
	require.NoError(t, err)

	// Redundant conversions to handle differences between unix families
	available = int64(stats.Bavail) * int64(stats.Bsize)                   //nolint:unconvert,nolintlint
	used = (int64(stats.Blocks) - int64(stats.Bfree)) * int64(stats.Bsize) //nolint:unconvert,nolintlint
	return available, used
}
