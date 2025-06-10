package snapshot

import (
	"context"
	"io/fs"
	"maps"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestSnapshotFilter_WithOrWithoutFeatureFlag(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.SnapshotFilter,
	).Run(t, testSnapshotFilter)
}

func testSnapshotFilter(t *testing.T, ctx context.Context) {
	for _, tc := range []struct {
		desc                string
		isExclusiveSnapshot bool
	}{
		{
			desc:                "writable snapshot",
			isExclusiveSnapshot: true,
		},
		{
			desc:                "read-only snapshot",
			isExclusiveSnapshot: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			storageDir := filepath.Join(tmpDir, "storage-dir")
			workingDir := filepath.Join(storageDir, "working-dir")

			createFsForSnapshotFilterTest(t, storageDir)

			metrics := NewMetrics()
			mgr, err := NewManager(testhelper.SharedLogger(t), storageDir, workingDir, metrics.Scope("storage-name"))
			require.NoError(t, err)
			defer testhelper.MustClose(t, mgr)

			fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/j"}, tc.isExclusiveSnapshot)
			require.NoError(t, err)
			defer testhelper.MustClose(t, fs1)

			testhelper.RequireDirectoryState(t, fs1.Root(), "",
				getExpectedDirectoryStateAfterSnapshotFilter(ctx, tc.isExclusiveSnapshot))
		})
	}
}

// TestSnapshotFilter_WithFeatureFlagFlip verifies the following scenario:
//  1. When snapshot_filter is enabled, a read-only snapshot is created using the regex filter
//     and cached by the snapshot manager.
//  2. Later, if snapshot_filter is disabled, the manager must not reuse the cached snapshot
//     from step 1, which was created with filtering enabled.
//
// This test ensures that the snapshot from step 1 is not reused in step 2. If the regex filter
// in step 1 excluded essential files, the resulting snapshot may be incomplete or broken.
// Reusing such a snapshot after the feature is disabled could cause future requests to fail.
func TestSnapshotFilter_WithFeatureFlagFlip(t *testing.T) {
	ctx := testhelper.Context(t)
	tmpDir := t.TempDir()
	storageDir := filepath.Join(tmpDir, "storage-dir")
	workingDir := filepath.Join(storageDir, "working-dir")

	createFsForSnapshotFilterTest(t, storageDir)

	metrics := NewMetrics()
	mgr, err := NewManager(testhelper.SharedLogger(t), storageDir, workingDir, metrics.Scope("storage-name"))
	require.NoError(t, err)
	defer testhelper.MustClose(t, mgr)

	fs1, err1 := mgr.GetSnapshot(ctx, []string{"repositories/j"}, false)
	require.NoError(t, err1)
	defer testhelper.MustClose(t, fs1)

	testhelper.RequireDirectoryState(t, fs1.Root(), "",
		getExpectedDirectoryStateAfterSnapshotFilter(ctx, false))

	// disable the feature flag, and snapshot again
	ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.SnapshotFilter, false)
	fs2, err2 := mgr.GetSnapshot(ctx, []string{"repositories/j"}, false)
	require.NoError(t, err2)
	defer testhelper.MustClose(t, fs2)
	testhelper.RequireDirectoryState(t, fs2.Root(), "",
		getExpectedDirectoryStateAfterSnapshotFilter(ctx, false))
}

func createFsForSnapshotFilterTest(t *testing.T, storageDir string) {
	testhelper.CreateFS(t, storageDir, fstest.MapFS{
		".":            {Mode: mode.Directory},
		"working-dir":  {Mode: mode.Directory},
		"repositories": {Mode: mode.Directory},
		"pools":        {Mode: mode.Directory},

		"repositories/j": {Mode: mode.Directory},
		"pools/h":        {Mode: mode.Directory},

		// Directories and files that should be always included.
		"pools/h/HEAD":                 {Mode: mode.File, Data: []byte("pool h head")},
		"pools/h/packed-refs":          {Mode: mode.File, Data: []byte("pool h packed ref")},
		"pools/h/refs":                 {Mode: mode.Directory},
		"pools/h/refs/heads":           {Mode: mode.Directory},
		"pools/h/refs/heads/main":      {Mode: mode.File, Data: []byte("pool h main branch")},
		"pools/h/refs/tags":            {Mode: mode.Directory},
		"pools/h/refs/tags/v18.1.0":    {Mode: mode.File, Data: []byte("pool h tag 18.1.0")},
		"pools/h/reftable":             {Mode: mode.Directory},
		"pools/h/reftable/tables.list": {Mode: mode.File, Data: []byte("pool h reftable table.list")},
		"pools/h/reftable/0x000000000001-0x000000000004-a85b824b.ref": {Mode: mode.File, Data: []byte("pool h reftable ref")},
		"pools/h/objects":    {Mode: mode.Directory},
		"pools/h/objects/23": {Mode: mode.Directory},
		"pools/h/objects/23/8e5f3cba986d43d9a4d5e58e1f2305f0c0b6a1": {Mode: mode.File, Data: []byte("pooh h 8e5f3cba986d43d9a4d5e58e1f2305f0c0b6a1")},
		"pools/h/objects/b7": {Mode: mode.Directory},
		"pools/h/objects/b7/15e7de2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18": {Mode: mode.File, Data: []byte("pool h 15e7de2")},
		"pools/h/objects/pack": {Mode: mode.Directory},
		"pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.pack":                           {Mode: mode.File, Data: []byte("pool h SHA1 pack")},
		"pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.idx":                            {Mode: mode.File, Data: []byte("pool h idx")},
		"pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.rev":                            {Mode: mode.File, Data: []byte("pool h rev")},
		"pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.bitmap":                         {Mode: mode.File, Data: []byte("pool h bitmap")},
		"pools/h/objects/pack/pack-bcbfa3d3ce19c07ab982ccbd33e3ea1a8b2048b1c6c3d1f6e53e6ec01e5935f2.pack":   {Mode: mode.File, Data: []byte("pool h SHA256 pack")},
		"pools/h/objects/pack/pack-f9b4e556290cbc81a913b8b08f7d69bfaab9a9db9a1d8a9ec4e9a8cf867e3d04.idx":    {Mode: mode.File, Data: []byte("pool h SHA256 idx")},
		"pools/h/objects/pack/pack-fa5d4516c2d5e5cc1a35b2d0f2f3c0adca8b5c1d3a98b09e0a3e9b21a7b9f3a2.rev":    {Mode: mode.File, Data: []byte("pool h SHA256 rev")},
		"pools/h/objects/pack/pack-5a6c9e3e8b5fa2f4c4e8c3a9e1dc7f5e4c8b1a0dbf0c2d3f4b1d7d0c4f9a2e61.bitmap": {Mode: mode.File, Data: []byte("pool h SHA256 bitmap")},

		"repositories/j/config":                                            {Mode: mode.File, Data: []byte("I am a config")},
		"repositories/j/.gitaly-full-repack-timestamp":                     {Mode: mode.File, Data: []byte("1999-July-1")},
		"repositories/j/gitaly-language.stats":                             {Mode: mode.File, Data: []byte("en")},
		"repositories/j/custom_hooks":                                      {Mode: mode.Directory},
		"repositories/j/custom_hooks/a-hook":                               {Mode: mode.File, Data: []byte("a hook")},
		"repositories/j/refs":                                              {Mode: mode.Directory},
		"repositories/j/refs/heads":                                        {Mode: mode.Directory},
		"repositories/j/refs/tags":                                         {Mode: mode.Directory},
		"repositories/j/reftable":                                          {Mode: mode.Directory},
		"repositories/j/objects":                                           {Mode: mode.Directory},
		"repositories/j/objects/info":                                      {Mode: mode.Directory},
		"repositories/j/objects/pack":                                      {Mode: mode.Directory},
		"repositories/j/objects/info/commit-graphs":                        {Mode: mode.Directory},
		"repositories/j/objects/info/alternates":                           {Mode: mode.File, Data: []byte("../../../pools/h/objects")},
		"repositories/j/objects/info/commit-graph":                         {Mode: mode.File, Data: []byte("j commit graph")},
		"repositories/j/objects/info/commit-graphs/commit-graph-chain":     {Mode: mode.File, Data: []byte("j commit graph chains")},
		"repositories/j/objects/pack/multi-pack-index":                     {Mode: mode.File, Data: []byte("j multi pack index")},
		"repositories/j/objects/12":                                        {Mode: mode.Directory},
		"repositories/j/objects/12/7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1": {Mode: mode.File, Data: []byte("7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1")},
		"repositories/j/objects/a6":                                        {Mode: mode.Directory},
		"repositories/j/objects/a6/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":                 {Mode: mode.File, Data: []byte("1a4738d29c18")},
		"repositories/j/objects/info/commit-graphs/graph-1.graph":                                                  {Mode: mode.File, Data: []byte("graph-1.graph")},
		"repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.pack":                           {Mode: mode.File, Data: []byte("SHA1 pack")},
		"repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.idx":                            {Mode: mode.File, Data: []byte("SHA1 idx")},
		"repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.rev":                            {Mode: mode.File, Data: []byte("SHA1 rev")},
		"repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.bitmap":                         {Mode: mode.File, Data: []byte("SHA1 bitmap")},
		"repositories/j/objects/pack/pack-9c07ab982ccbd33e3ea1a8b2048b1c6c3d1f6e5bcbfa3d3ce13e6ec01e5935f2.pack":   {Mode: mode.File, Data: []byte("SHA256 pack")},
		"repositories/j/objects/pack/pack-c81a913b8b08f7d69bfaab9a9db9a1d8a9ec4ef9b4e556290cb9a8cf867e3d04.idx":    {Mode: mode.File, Data: []byte("SHA256 idx")},
		"repositories/j/objects/pack/pack-1a35b2d0f2f3c0adca8b5c1d3a98b09e0a3fa5d4516c2d5e5cce9b21a7b9f3a2.rev":    {Mode: mode.File, Data: []byte("SHA256 rev")},
		"repositories/j/objects/pack/pack-5fa2f4c4e8c3a9e1dc7f5e4c8b1a0dbf0c2d3f4b1d5a6c9e3e8b7d0c4f9a2e61.bitmap": {Mode: mode.File, Data: []byte("SHA256 bitmap")},

		"repositories/j/HEAD":                                                {Mode: mode.File, Data: []byte("j HEAD")},
		"repositories/j/packed-refs":                                         {Mode: mode.File, Data: []byte("j packed ref")},
		"repositories/j/refs/heads/master":                                   {Mode: mode.File, Data: []byte("j master")},
		"repositories/j/refs/heads/feature-x":                                {Mode: mode.File, Data: []byte("j feature x")},
		"repositories/j/refs/remotes":                                        {Mode: mode.Directory},
		"repositories/j/refs/remotes/origin":                                 {Mode: mode.Directory},
		"repositories/j/refs/remotes/origin/main":                            {Mode: mode.File, Data: []byte("j main")},
		"repositories/j/refs/tags/v1.0.0":                                    {Mode: mode.File, Data: []byte("j tag 1.0.0")},
		"repositories/j/reftable/0x000000000001-0x000000000004-b74a713a.ref": {Mode: mode.File, Data: []byte("j reftable ref b74a713a")},
		"repositories/j/reftable/tables.list":                                {Mode: mode.File, Data: []byte("j tables.list")},

		// Directories and files that should be included by default filter
		// and excluded by the regex based filter
		"pools/h/description":                       {Mode: mode.File, Data: []byte("pool h description")},
		"pools/h/refs/heads/main.lock":              {Mode: mode.File, Data: []byte("pool h main branch lock file")},
		"pools/h/reftable/tables.list.garbage.file": {Mode: mode.File, Data: []byte("pool h reftable garbage file")},
		"pools/h/reftable/4-a85b824b.ref.lock":      {Mode: mode.File, Data: []byte("pool h reftable ref lock")},
		"pools/h/info":                              {Mode: mode.Directory},
		"pools/h/info/attributes":                   {Mode: mode.File, Data: []byte("pool h info attributes")},
		"pools/h/objects/pack/pack-abcdef1234234567890abcdef12345678905678.keep":                           {Mode: mode.File, Data: []byte("pool h SHA1 keep")},
		"pools/h/objects/pack/pack-cdef12341234567890ab567890abcdef12345678.mtime":                         {Mode: mode.File, Data: []byte("pool h SHA1 mtime")},
		"pools/h/objects/pack/pack-1b9f3e7c4d2b1f0e8a3c73ed1a7c8f0b2e4c1d9a8e3f4b0c5d6ad5b2e0a9c6d3.keep":  {Mode: mode.File, Data: []byte("pool h SHA256 keep")},
		"pools/h/objects/pack/pack-d2b1f0e8a3c7d0b2e4c1d9a3ed1a7c8f8e3f4b0c5d6a1b9f3e7c45b2e0a9abcd.mtime": {Mode: mode.File, Data: []byte("pool h SHA256 mtime")},
		"pools/h/objects/23/short_invalid_305f0c0b6a1":                                                     {Mode: mode.File, Data: []byte("pool h short invalid")},
		"pools/h/objects/b7/long_invalid_f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":   {Mode: mode.File, Data: []byte("pool h long invalid")},
		"pools/h/objects/b7/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a47.invalid":                {Mode: mode.File, Data: []byte("pool h suffix invalid")},

		"repositories/j/description":               {Mode: mode.File, Data: []byte("j description")},
		"repositories/j/refs/heads/main.lock":      {Mode: mode.File, Data: []byte("j branch lock")},
		"repositories/j/logs":                      {Mode: mode.Directory},
		"repositories/j/logs/some.log":             {Mode: mode.File, Data: []byte("some log")},
		"repositories/j/info":                      {Mode: mode.Directory},
		"repositories/j/hooks":                     {Mode: mode.Directory},
		"repositories/j/some-garbage":              {Mode: mode.Directory},
		"repositories/j/info/attributes":           {Mode: mode.File, Data: []byte("info attributes")},
		"repositories/j/objects/some-garbage":      {Mode: mode.Directory},
		"repositories/j/packed-refs.useless.file":  {Mode: mode.File, Data: []byte("packed-refs.useless.file")},
		"repositories/j/custom_hooks.useless.file": {Mode: mode.File, Data: []byte("custom_hooks.useless.file")},
		"repositories/j/reftable/ref.useless.file": {Mode: mode.File, Data: []byte("reftable/ref.useless.file")},

		"repositories/j/reftable/0x000000000002-0x000000000008-c56a834b.ref.lock":                                 {Mode: mode.File, Data: []byte("reftable lock file")},
		"repositories/j/objects/pack/pack-234567890abcdef1234567890abcdef12345678.keep":                           {Mode: mode.File, Data: []byte("SHA1 keep")},
		"repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.mtime":                         {Mode: mode.File, Data: []byte("SHA1 mtime")},
		"repositories/j/objects/pack/pack-3ed1a7c8f0b2e4c1d9a8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9c6d3.keep":  {Mode: mode.File, Data: []byte("SHA256 keep")},
		"repositories/j/objects/pack/pack-0b2e4c1d9a3ed1a7c8f8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9abcd.mtime": {Mode: mode.File, Data: []byte("SHA256 mtime")},
		"repositories/j/objects/12/short_invalid_305f0c0b6a1":                                                     {Mode: mode.File, Data: []byte("short invalid")},
		"repositories/j/objects/a6/long_invalid_f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":   {Mode: mode.File, Data: []byte("long invalid")},
		"repositories/j/objects/a6/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a47.invalid":                {Mode: mode.File, Data: []byte("suffix invalid")},

		// Directories that should always be excluded.
		"repositories/j/worktrees":                         {Mode: mode.Directory},
		"repositories/j/worktrees/a_worktree":              {Mode: mode.File, Data: []byte("j worktree")},
		"repositories/j/gitlab-worktree":                   {Mode: mode.Directory},
		"repositories/j/gitlab-worktree/a_gitlab_worktree": {Mode: mode.File, Data: []byte("j gitlab worktree")},
	})
}

func getExpectedDirectoryStateAfterSnapshotFilter(ctx context.Context, isExclusiveSnapshot bool) testhelper.DirectoryState {
	snapshotFilterType := "default"
	if featureflag.SnapshotFilter.IsEnabled(ctx) && !isExclusiveSnapshot {
		snapshotFilterType = "regex"
	}
	dirMode := func() fs.FileMode {
		if isExclusiveSnapshot {
			return mode.Directory
		}
		return ModeReadOnlyDirectory
	}

	stateMustStay := testhelper.DirectoryState{
		// The snapshotting process does not use the existing permissions for
		// directories in the hierarchy before the repository directories.
		"/":             {Mode: dirMode()},
		"/repositories": {Mode: dirMode()},
		"/pools":        {Mode: dirMode()},

		// Directories and files that should be always included.
		"/pools/h":                      {Mode: dirMode()},
		"/pools/h/HEAD":                 {Mode: mode.File, Content: []byte("pool h head")},
		"/pools/h/refs":                 {Mode: dirMode()},
		"/pools/h/objects":              {Mode: dirMode()},
		"/pools/h/packed-refs":          {Mode: mode.File, Content: []byte("pool h packed ref")},
		"/pools/h/refs/heads":           {Mode: dirMode()},
		"/pools/h/refs/heads/main":      {Mode: mode.File, Content: []byte("pool h main branch")},
		"/pools/h/refs/tags":            {Mode: dirMode()},
		"/pools/h/refs/tags/v18.1.0":    {Mode: mode.File, Content: []byte("pool h tag 18.1.0")},
		"/pools/h/reftable":             {Mode: dirMode()},
		"/pools/h/reftable/tables.list": {Mode: mode.File, Content: []byte("pool h reftable table.list")},
		"/pools/h/reftable/0x000000000001-0x000000000004-a85b824b.ref": {Mode: mode.File, Content: []byte("pool h reftable ref")},
		"/pools/h/objects/23": {Mode: dirMode()},
		"/pools/h/objects/23/8e5f3cba986d43d9a4d5e58e1f2305f0c0b6a1": {Mode: mode.File, Content: []byte("pooh h 8e5f3cba986d43d9a4d5e58e1f2305f0c0b6a1")},
		"/pools/h/objects/b7": {Mode: dirMode()},
		"/pools/h/objects/b7/15e7de2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18": {Mode: mode.File, Content: []byte("pool h 15e7de2")},
		"/pools/h/objects/pack": {Mode: dirMode()},
		"/pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.pack":                           {Mode: mode.File, Content: []byte("pool h SHA1 pack")},
		"/pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.idx":                            {Mode: mode.File, Content: []byte("pool h idx")},
		"/pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.rev":                            {Mode: mode.File, Content: []byte("pool h rev")},
		"/pools/h/objects/pack/pack-7654321890abcdef1234567890abcdef12345678.bitmap":                         {Mode: mode.File, Content: []byte("pool h bitmap")},
		"/pools/h/objects/pack/pack-bcbfa3d3ce19c07ab982ccbd33e3ea1a8b2048b1c6c3d1f6e53e6ec01e5935f2.pack":   {Mode: mode.File, Content: []byte("pool h SHA256 pack")},
		"/pools/h/objects/pack/pack-f9b4e556290cbc81a913b8b08f7d69bfaab9a9db9a1d8a9ec4e9a8cf867e3d04.idx":    {Mode: mode.File, Content: []byte("pool h SHA256 idx")},
		"/pools/h/objects/pack/pack-fa5d4516c2d5e5cc1a35b2d0f2f3c0adca8b5c1d3a98b09e0a3e9b21a7b9f3a2.rev":    {Mode: mode.File, Content: []byte("pool h SHA256 rev")},
		"/pools/h/objects/pack/pack-5a6c9e3e8b5fa2f4c4e8c3a9e1dc7f5e4c8b1a0dbf0c2d3f4b1d7d0c4f9a2e61.bitmap": {Mode: mode.File, Content: []byte("pool h SHA256 bitmap")},

		"/repositories/j":                               {Mode: dirMode()},
		"/repositories/j/config":                        {Mode: mode.File, Content: []byte("I am a config")},
		"/repositories/j/.gitaly-full-repack-timestamp": {Mode: mode.File, Content: []byte("1999-July-1")},
		"/repositories/j/gitaly-language.stats":         {Mode: mode.File, Content: []byte("en")},
		"/repositories/j/custom_hooks":                  {Mode: dirMode()},
		"/repositories/j/custom_hooks/a-hook":           {Mode: mode.File, Content: []byte("a hook")},
		"/repositories/j/refs":                          {Mode: dirMode()},
		"/repositories/j/refs/heads":                    {Mode: dirMode()},
		"/repositories/j/refs/tags":                     {Mode: dirMode()},
		"/repositories/j/reftable":                      {Mode: dirMode()},
		"/repositories/j/objects":                       {Mode: dirMode()},
		"/repositories/j/objects/info":                  {Mode: dirMode()},
		"/repositories/j/objects/pack":                  {Mode: dirMode()},
		"/repositories/j/objects/info/commit-graphs":    {Mode: dirMode()},
		"/repositories/j/objects/info/alternates":       {Mode: mode.File, Content: []byte("../../../pools/h/objects")},
		"/repositories/j/objects/info/commit-graph":     {Mode: mode.File, Content: []byte("j commit graph")},

		"/repositories/j/objects/info/commit-graphs/commit-graph-chain":                                             {Mode: mode.File, Content: []byte("j commit graph chains")},
		"/repositories/j/objects/pack/multi-pack-index":                                                             {Mode: mode.File, Content: []byte("j multi pack index")},
		"/repositories/j/objects/12":                                                                                {Mode: dirMode()},
		"/repositories/j/objects/12/7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1":                                         {Mode: mode.File, Content: []byte("7d4e2cba986d43d9a4d5e58e1f2305f0c0b6a1")},
		"/repositories/j/objects/a6":                                                                                {Mode: dirMode()},
		"/repositories/j/objects/a6/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":                 {Mode: mode.File, Content: []byte("1a4738d29c18")},
		"/repositories/j/objects/info/commit-graphs/graph-1.graph":                                                  {Mode: mode.File, Content: []byte("graph-1.graph")},
		"/repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.pack":                           {Mode: mode.File, Content: []byte("SHA1 pack")},
		"/repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.idx":                            {Mode: mode.File, Content: []byte("SHA1 idx")},
		"/repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.rev":                            {Mode: mode.File, Content: []byte("SHA1 rev")},
		"/repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.bitmap":                         {Mode: mode.File, Content: []byte("SHA1 bitmap")},
		"/repositories/j/objects/pack/pack-9c07ab982ccbd33e3ea1a8b2048b1c6c3d1f6e5bcbfa3d3ce13e6ec01e5935f2.pack":   {Mode: mode.File, Content: []byte("SHA256 pack")},
		"/repositories/j/objects/pack/pack-c81a913b8b08f7d69bfaab9a9db9a1d8a9ec4ef9b4e556290cb9a8cf867e3d04.idx":    {Mode: mode.File, Content: []byte("SHA256 idx")},
		"/repositories/j/objects/pack/pack-1a35b2d0f2f3c0adca8b5c1d3a98b09e0a3fa5d4516c2d5e5cce9b21a7b9f3a2.rev":    {Mode: mode.File, Content: []byte("SHA256 rev")},
		"/repositories/j/objects/pack/pack-5fa2f4c4e8c3a9e1dc7f5e4c8b1a0dbf0c2d3f4b1d5a6c9e3e8b7d0c4f9a2e61.bitmap": {Mode: mode.File, Content: []byte("SHA256 bitmap")},

		"/repositories/j/HEAD":                                                {Mode: mode.File, Content: []byte("j HEAD")},
		"/repositories/j/packed-refs":                                         {Mode: mode.File, Content: []byte("j packed ref")},
		"/repositories/j/refs/heads/master":                                   {Mode: mode.File, Content: []byte("j master")},
		"/repositories/j/refs/heads/feature-x":                                {Mode: mode.File, Content: []byte("j feature x")},
		"/repositories/j/refs/remotes":                                        {Mode: dirMode()},
		"/repositories/j/refs/remotes/origin":                                 {Mode: dirMode()},
		"/repositories/j/refs/remotes/origin/main":                            {Mode: mode.File, Content: []byte("j main")},
		"/repositories/j/refs/tags/v1.0.0":                                    {Mode: mode.File, Content: []byte("j tag 1.0.0")},
		"/repositories/j/reftable/0x000000000001-0x000000000004-b74a713a.ref": {Mode: mode.File, Content: []byte("j reftable ref b74a713a")},
		"/repositories/j/reftable/tables.list":                                {Mode: mode.File, Content: []byte("j tables.list")},
	}

	stateMayGetFiltered := testhelper.DirectoryState{
		"/pools/h/description":                       {Mode: mode.File, Content: []byte("pool h description")},
		"/pools/h/refs/heads/main.lock":              {Mode: mode.File, Content: []byte("pool h main branch lock file")},
		"/pools/h/reftable/tables.list.garbage.file": {Mode: mode.File, Content: []byte("pool h reftable garbage file")},
		"/pools/h/reftable/4-a85b824b.ref.lock":      {Mode: mode.File, Content: []byte("pool h reftable ref lock")},
		"/pools/h/info":                              {Mode: dirMode()},
		"/pools/h/info/attributes":                   {Mode: mode.File, Content: []byte("pool h info attributes")},
		"/pools/h/objects/pack/pack-abcdef1234234567890abcdef12345678905678.keep":                           {Mode: mode.File, Content: []byte("pool h SHA1 keep")},
		"/pools/h/objects/pack/pack-cdef12341234567890ab567890abcdef12345678.mtime":                         {Mode: mode.File, Content: []byte("pool h SHA1 mtime")},
		"/pools/h/objects/pack/pack-1b9f3e7c4d2b1f0e8a3c73ed1a7c8f0b2e4c1d9a8e3f4b0c5d6ad5b2e0a9c6d3.keep":  {Mode: mode.File, Content: []byte("pool h SHA256 keep")},
		"/pools/h/objects/pack/pack-d2b1f0e8a3c7d0b2e4c1d9a3ed1a7c8f8e3f4b0c5d6a1b9f3e7c45b2e0a9abcd.mtime": {Mode: mode.File, Content: []byte("pool h SHA256 mtime")},
		"/pools/h/objects/23/short_invalid_305f0c0b6a1":                                                     {Mode: mode.File, Content: []byte("pool h short invalid")},
		"/pools/h/objects/b7/long_invalid_f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":   {Mode: mode.File, Content: []byte("pool h long invalid")},
		"/pools/h/objects/b7/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a47.invalid":                {Mode: mode.File, Content: []byte("pool h suffix invalid")},

		"/repositories/j/description":               {Mode: mode.File, Content: []byte("j description")},
		"/repositories/j/refs/heads/main.lock":      {Mode: mode.File, Content: []byte("j branch lock")},
		"/repositories/j/logs":                      {Mode: dirMode()},
		"/repositories/j/logs/some.log":             {Mode: mode.File, Content: []byte("some log")},
		"/repositories/j/info":                      {Mode: dirMode()},
		"/repositories/j/hooks":                     {Mode: dirMode()},
		"/repositories/j/some-garbage":              {Mode: dirMode()},
		"/repositories/j/info/attributes":           {Mode: mode.File, Content: []byte("info attributes")},
		"/repositories/j/objects/some-garbage":      {Mode: dirMode()},
		"/repositories/j/packed-refs.useless.file":  {Mode: mode.File, Content: []byte("packed-refs.useless.file")},
		"/repositories/j/custom_hooks.useless.file": {Mode: mode.File, Content: []byte("custom_hooks.useless.file")},
		"/repositories/j/reftable/ref.useless.file": {Mode: mode.File, Content: []byte("reftable/ref.useless.file")},

		"/repositories/j/reftable/0x000000000002-0x000000000008-c56a834b.ref.lock":                                 {Mode: mode.File, Content: []byte("reftable lock file")},
		"/repositories/j/objects/pack/pack-234567890abcdef1234567890abcdef12345678.keep":                           {Mode: mode.File, Content: []byte("SHA1 keep")},
		"/repositories/j/objects/pack/pack-1234567890abcdef1234567890abcdef12345678.mtime":                         {Mode: mode.File, Content: []byte("SHA1 mtime")},
		"/repositories/j/objects/pack/pack-3ed1a7c8f0b2e4c1d9a8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9c6d3.keep":  {Mode: mode.File, Content: []byte("SHA256 keep")},
		"/repositories/j/objects/pack/pack-0b2e4c1d9a3ed1a7c8f8e3f4b0c5d6a1b9f3e7c4d2b1f0e8a3c7d5b2e0a9abcd.mtime": {Mode: mode.File, Content: []byte("SHA256 mtime")},
		"/repositories/j/objects/12/short_invalid_305f0c0b6a1":                                                     {Mode: mode.File, Content: []byte("short invalid")},
		"/repositories/j/objects/a6/long_invalid_f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a4738d29c18":   {Mode: mode.File, Content: []byte("long invalid")},
		"/repositories/j/objects/a6/f4d6cb2e1b9a7930e01db68bc3c1f5943733e52ff6cb7b2dcf1a47.invalid":                {Mode: mode.File, Content: []byte("suffix invalid")},
	}

	if snapshotFilterType == "default" {
		maps.Copy(stateMustStay, stateMayGetFiltered)
	}
	return stateMustStay
}
