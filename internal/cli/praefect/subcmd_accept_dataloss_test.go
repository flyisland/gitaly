package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestAcceptDatalossSubcommand(t *testing.T) {
	t.Parallel()
	const (
		vs   = "test-virtual-storage-1"
		repo = "test-repository-1"
		st1  = "test-physical-storage-1"
		st2  = "test-physical-storage-2"
		st3  = "test-physical-storage-3"
	)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: vs,
				Nodes: []*config.Node{
					{Address: "stub", Storage: st1},
					{Address: "stub", Storage: st2},
					{Address: "stub", Storage: st3},
				},
			},
		},
	}
	ctx := testhelper.Context(t)

	db := testdb.New(t)
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
	startingGenerations := map[string]int{st1: 1, st2: 0, st3: datastore.GenerationUnknown}

	repoCreated := false
	for storage, generation := range startingGenerations {
		if generation == datastore.GenerationUnknown {
			continue
		}

		if !repoCreated {
			repoCreated = true
			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, repo, storage, nil, nil, false, false))
		}

		require.NoError(t, rs.SetGeneration(ctx, 1, storage, repo, generation))
	}

	ln, clean := listenAndServe(t, []svcRegistrar{registerPraefectInfoServer(info.NewServer(conf, testhelper.NewLogger(t), rs, nil, nil, nil))})
	defer clean()

	conf.SocketPath = ln.Addr().String()
	confPath := writeConfigToFile(t, conf)

	type errorMatcher func(t *testing.T, stderr string, exitCode int)

	matchErrorMsg := func(expected string) errorMatcher {
		return func(t *testing.T, actual string, exitCode int) {
			t.Helper()
			require.Equal(t, expected+"\n", actual)
			require.Equal(t, 1, exitCode)
		}
	}

	for _, tc := range []struct {
		desc                string
		args                []string
		virtualStorages     []*config.VirtualStorage
		output              string
		matchError          errorMatcher
		expectedGenerations map[string]int
	}{
		{
			desc:                "missing virtual storage",
			args:                []string{"-relative-path", "test-repository-1", "-authoritative-storage", "test-physical-storage-2"},
			matchError:          matchErrorMsg(`Required flag "virtual-storage" not set`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "missing repository",
			args:                []string{"-virtual-storage", "test-virtual-storage-1", "-authoritative-storage", "test-physical-storage-2"},
			matchError:          matchErrorMsg(`Required flag "relative-path" not set`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "missing authoritative storage",
			args:                []string{"-virtual-storage", "test-virtual-storage-1", "-relative-path", "test-repository-1"},
			matchError:          matchErrorMsg(`Required flag "authoritative-storage" not set`),
			expectedGenerations: startingGenerations,
		},
		{
			desc: "positional arguments",
			args: []string{"-virtual-storage", "test-virtual-storage-1", "-relative-path", "test-repository-1", "-authoritative-storage", "test-physical-storage-2", "positional-arg"},
			matchError: func(t *testing.T, stderr string, exitCode int) {
				t.Helper()
				require.Equal(t, unexpectedPositionalArgsError{Command: "accept-dataloss"}.Error()+"\n", stderr)
				require.Equal(t, 1, exitCode)
			},
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent virtual storage",
			args:                []string{"-virtual-storage", "non-existent", "-relative-path", "test-repository-1", "-authoritative-storage", "test-physical-storage-2"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = unknown virtual storage: "non-existent"`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent authoritative storage",
			args:                []string{"-virtual-storage", "test-virtual-storage-1", "-relative-path", "test-repository-1", "-authoritative-storage", "non-existent"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = unknown authoritative storage: "non-existent"`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent repository",
			args:                []string{"-virtual-storage", "test-virtual-storage-1", "-relative-path", "non-existent", "-authoritative-storage", "test-physical-storage-2"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = repository does not exist on virtual storage`),
			expectedGenerations: startingGenerations,
		},
		{
			desc: "success",
			args: []string{"-virtual-storage", "test-virtual-storage-1", "-relative-path", "test-repository-1", "-authoritative-storage", "test-physical-storage-2"},
			matchError: func(t *testing.T, actual string, exitCode int) {
				t.Helper()
				require.Empty(t, actual)
				require.Zero(t, exitCode)
			},
			expectedGenerations: map[string]int{st1: 1, st2: 2, st3: datastore.GenerationUnknown},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, stderr, exitCode := runApp(t, ctx, append([]string{"-config", confPath, "accept-dataloss"}, tc.args...))
			tc.matchError(t, stderr, exitCode)

			for storage, expected := range tc.expectedGenerations {
				actual, err := rs.GetGeneration(ctx, 1, storage)
				require.NoError(t, err)
				require.Equal(t, expected, actual, storage)
			}
		})
	}
}
