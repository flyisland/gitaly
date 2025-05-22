package gitcmd_test

import (
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
)

func TestHooksPayload(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	tx := txinfo.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := gitcmd.NewHooksPayload(
			ctx,
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			nil,
			gitcmd.AllHooks,
			nil,
			0,
		).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, gitcmd.EnvHooksPayload+"="))
	})

	t.Run("roundtrip succeeds", func(t *testing.T) {
		ctx := metadata.AppendToIncomingContext(ctx, metadata.ClientContextMetadataKey, "foobar")

		logFields := log.Fields{"log_field": "log_value"}
		ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(nil).WithFields(logFields))

		env, err := gitcmd.NewHooksPayload(
			ctx,
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			nil,
			gitcmd.PreReceiveHook,
			map[featureflag.FeatureFlag]bool{
				{Name: "flag_key"}: true,
			},
			1,
		).Env()
		require.NoError(t, err)

		payload, err := gitcmd.HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			gitcmd.EnvHooksPayload + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, gitcmd.HooksPayload{
			Repo:           repo,
			ObjectFormat:   gittest.DefaultObjectHash.Format,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			RequestedHooks: gitcmd.PreReceiveHook,
			FeatureFlagsWithValue: []gitcmd.FeatureFlagWithValue{
				{
					Flag:    featureflag.FeatureFlag{Name: "flag_key"},
					Enabled: true,
				},
			},
			TransactionID:       1,
			GitalyClientContext: []byte("foobar"),
			LogFields:           logFields,
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := gitcmd.NewHooksPayload(
			ctx,
			cfg,
			repo,
			gittest.DefaultObjectHash,
			&tx,
			nil,
			gitcmd.UpdateHook,
			nil,
			0,
		).Env()
		require.NoError(t, err)

		payload, err := gitcmd.HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, gitcmd.HooksPayload{
			Repo:           repo,
			ObjectFormat:   gittest.DefaultObjectHash.Format,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			Transaction:    &tx,
			RequestedHooks: gitcmd.UpdateHook,
			LogFields:      log.Fields{},
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := gitcmd.HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, gitcmd.ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := gitcmd.HooksPayloadFromEnv([]string{gitcmd.EnvHooksPayload + "=foobar"})
		require.Error(t, err)
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := gitcmd.NewHooksPayload(
			ctx,
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			&gitcmd.UserDetails{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			}, gitcmd.PostReceiveHook, nil, 0).Env()
		require.NoError(t, err)

		payload, err := gitcmd.HooksPayloadFromEnv([]string{
			env,
			"GL_ID=wrong",
			"GL_USERNAME=wrong",
			"GL_PROTOCOL=wrong",
		})
		require.NoError(t, err)

		require.Equal(t, gitcmd.HooksPayload{
			Repo:                repo,
			ObjectFormat:        gittest.DefaultObjectHash.Format,
			RuntimeDir:          cfg.RuntimeDir,
			InternalSocket:      cfg.InternalSocketPath(),
			InternalSocketToken: cfg.Auth.Token,
			UserDetails: &gitcmd.UserDetails{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			},
			RequestedHooks: gitcmd.PostReceiveHook,
			LogFields:      log.Fields{},
		}, payload)
	})
}

func TestHooksPayload_IsHookRequested(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc       string
		configured gitcmd.Hook
		request    gitcmd.Hook
		expected   bool
	}{
		{
			desc:       "exact match",
			configured: gitcmd.PreReceiveHook,
			request:    gitcmd.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "hook matches a set",
			configured: gitcmd.PreReceiveHook | gitcmd.PostReceiveHook,
			request:    gitcmd.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "no match",
			configured: gitcmd.PreReceiveHook,
			request:    gitcmd.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with a set",
			configured: gitcmd.PreReceiveHook | gitcmd.UpdateHook,
			request:    gitcmd.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with nothing set",
			configured: 0,
			request:    gitcmd.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "pre-receive hook with AllHooks",
			configured: gitcmd.AllHooks,
			request:    gitcmd.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "post-receive hook with AllHooks",
			configured: gitcmd.AllHooks,
			request:    gitcmd.PostReceiveHook,
			expected:   true,
		},
		{
			desc:       "update hook with AllHooks",
			configured: gitcmd.AllHooks,
			request:    gitcmd.UpdateHook,
			expected:   true,
		},
		{
			desc:       "reference-transaction hook with AllHooks",
			configured: gitcmd.AllHooks,
			request:    gitcmd.ReferenceTransactionHook,
			expected:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := gitcmd.HooksPayload{
				RequestedHooks: tc.configured,
			}.IsHookRequested(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
