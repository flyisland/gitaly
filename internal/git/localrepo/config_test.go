package localrepo

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

func TestRepo_SetConfig(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	type configEntry struct {
		key, value string
	}

	for _, tc := range []struct {
		desc               string
		preexistingEntries []configEntry
		key                string
		value              string
		locked             bool
		expectedEntries    []string
		expectedErr        error
	}{
		{
			desc:            "simple addition",
			key:             "my.key",
			value:           "value",
			expectedEntries: []string{"my.key=value"},
		},
		{
			desc: "overwrite preexisting value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: []string{"preexisting.key=overridden"},
		},
		{
			desc: "overwrite multi-value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value-1"},
				{"preexisting.key", "value-2"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: []string{"preexisting.key=overridden"},
		},
		{
			desc:        "invalid key",
			key:         "missingsection",
			value:       "overridden",
			expectedErr: fmt.Errorf("%w: missing section or name", gitcmd.ErrInvalidArg),
		},
		{
			desc:        "locked",
			key:         "my.key",
			value:       "value",
			locked:      true,
			expectedErr: fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", safe.ErrFileAlreadyLocked)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := NewTestRepo(t, cfg, repoProto)

			for _, entry := range tc.preexistingEntries {
				gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", entry.key, entry.value)
			}

			if tc.locked {
				writer, err := safe.NewLockingFileWriter(filepath.Join(repoPath, "config"))
				require.NoError(t, err)
				defer func() { require.NoError(t, writer.Close()) }()
				require.NoError(t, writer.Lock())
			}

			require.Equal(t, tc.expectedErr, repo.SetConfig(ctx, tc.key, tc.value, &transaction.MockManager{}))

			standardEntries := []string{
				"core.filemode=true",
				"core.bare=true",
			}

			if gittest.ObjectHashIsSHA256() || testhelper.IsReftableEnabled() {
				standardEntries = append(standardEntries, "core.repositoryformatversion=1")
			} else {
				standardEntries = append(standardEntries, "core.repositoryformatversion=0")
			}

			if gittest.ObjectHashIsSHA256() {
				standardEntries = append(standardEntries, "extensions.objectformat=sha256")
			}

			if testhelper.IsReftableEnabled() {
				standardEntries = append(standardEntries, "extensions.refstorage=reftable")
			}

			if runtime.GOOS == "darwin" {
				standardEntries = append(standardEntries,
					"core.ignorecase=true",
					"core.precomposeunicode=true",
				)
			}

			output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
			require.ElementsMatch(t,
				append(standardEntries, tc.expectedEntries...),
				strings.Split(text.ChompBytes(output), "\n"),
			)
		})
	}

	t.Run("transactional", func(t *testing.T) {
		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := NewTestRepo(t, cfg, repoProto)

		backchannelPeer := &peer.Peer{
			AuthInfo: backchannel.WithID(nil, 1234),
		}

		ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
		ctx = peer.NewContext(ctx, backchannelPeer)

		txManager := transaction.NewTrackingManager()

		require.NoError(t, err)
		require.NoError(t, repo.SetConfig(ctx, "some.key", "value", txManager))

		require.Equal(t, 2, len(txManager.Votes()))
	})
}

func TestRepo_UnsetMatchingConfig(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	standardKeys := []string{
		"core.repositoryformatversion",
		"core.filemode",
		"core.bare",
	}
	if runtime.GOOS == "darwin" {
		standardKeys = append(standardKeys, "core.ignorecase", "core.precomposeunicode")
	}
	if gittest.ObjectHashIsSHA256() {
		standardKeys = append(standardKeys, "extensions.objectformat")
	}
	if testhelper.IsReftableEnabled() {
		standardKeys = append(standardKeys, "extensions.refstorage")
	}

	for _, tc := range []struct {
		desc         string
		addEntries   map[string]string
		regex        string
		locked       bool
		expectedErr  error
		expectedKeys []string
	}{
		{
			desc:         "empty regex is refused",
			regex:        "",
			expectedErr:  fmt.Errorf("%w: \"regex\" is blank or empty", gitcmd.ErrInvalidArg),
			expectedKeys: standardKeys,
		},
		{
			desc: "simple match",
			addEntries: map[string]string{
				"foo.bar": "value1",
				"foo.qux": "value2",
			},
			regex:        "foo.bar",
			expectedKeys: append([]string{"foo.qux"}, standardKeys...),
		},
		{
			desc: "multiple matches",
			addEntries: map[string]string{
				"foo.bar": "value1",
				"foo.qux": "value2",
			},
			regex:        "foo.",
			expectedKeys: standardKeys,
		},
		{
			desc: "unanchored",
			addEntries: map[string]string{
				"foo.matchme": "value1",
				"foo.qux":     "value2",
			},
			regex:        "matchme",
			expectedKeys: append([]string{"foo.qux"}, standardKeys...),
		},
		{
			desc: "anchored",
			addEntries: map[string]string{
				"foo.matchme": "value1",
				"matchme.foo": "value2",
			},
			regex:        "^matchme",
			expectedKeys: append([]string{"foo.matchme"}, standardKeys...),
		},
		{
			desc:         "no matches",
			regex:        "idontmatch",
			expectedErr:  fmt.Errorf("%w: no matching keys", git.ErrNotFound),
			expectedKeys: standardKeys,
		},
		{
			desc:         "invalid regex",
			regex:        "?",
			expectedErr:  fmt.Errorf("%w: invalid regular expression", gitcmd.ErrInvalidArg),
			expectedKeys: standardKeys,
		},
		{
			desc:         "locked",
			regex:        ".*",
			locked:       true,
			expectedErr:  fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", safe.ErrFileAlreadyLocked)),
			expectedKeys: standardKeys,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)

			for key, value := range tc.addEntries {
				gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", key, value)
			}

			if tc.locked {
				writer, err := safe.NewLockingFileWriter(filepath.Join(repoPath, "config"))
				require.NoError(t, err)
				defer func() { require.NoError(t, writer.Close()) }()
				require.NoError(t, writer.Lock())
			}

			require.Equal(t, tc.expectedErr, repo.UnsetMatchingConfig(ctx, tc.regex, &transaction.MockManager{}))

			output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--name-only", "--local")
			require.ElementsMatch(t, tc.expectedKeys, strings.Split(text.ChompBytes(output), "\n"))
		})
	}

	t.Run("transactional", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := NewTestRepo(t, cfg, repoProto)

		gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", "some.key", "value")

		backchannelPeer := &peer.Peer{
			AuthInfo: backchannel.WithID(nil, 1234),
		}

		ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
		ctx = peer.NewContext(ctx, backchannelPeer)

		txManager := transaction.NewTrackingManager()

		require.NoError(t, err)
		require.NoError(t, repo.UnsetMatchingConfig(ctx, "some.key", txManager))

		require.Equal(t, 2, len(txManager.Votes()))
	})
}

func TestRepo_GetConfigValues(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc         string
		configKey    string
		configValues []string
		expectedErr  error
	}{
		{
			desc:         "a value for a key",
			configKey:    "remote.test.url",
			configValues: []string{"git@gitlab.com:gitlab-org/gitaly.git"},
			expectedErr:  nil,
		},
		{
			desc:      "multiple values for a key",
			configKey: "remote.origin.fetch",
			configValues: []string{
				"+refs/heads/*:refs/remotes/origin/*",
				"+refs/tags/*:refs/tags/*",
			},
			expectedErr: nil,
		},
		{
			desc:         "no values",
			configKey:    "remote.i_dont_exist.url",
			configValues: []string{},
			expectedErr:  git.ErrNotFound,
		},
		{
			desc:         "whitespace key",
			configKey:    "  ",
			configValues: []string{},
			expectedErr:  git.ErrNotFound,
		},
		{
			desc:         "empty key",
			configKey:    "",
			configValues: []string{},
			expectedErr:  git.ErrNotFound,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)
			if strings.TrimSpace(tc.configKey) != "" {
				for _, v := range tc.configValues {
					gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", tc.configKey, v)
				}
			}
			values, err := repo.GetConfigValues(ctx, tc.configKey)
			require.ElementsMatch(t, tc.configValues, values)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}
