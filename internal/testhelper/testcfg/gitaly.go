package testcfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// UnconfiguredSocketPath is used to bypass config validation errors
// when building the configuration. The socket path is now known yet
// at the time of building the configuration and is substituted later
// when the service is actually spun up.
const UnconfiguredSocketPath = "it is a stub to bypass Validate method"

// Option is a configuration option for the builder.
type Option func(*GitalyCfgBuilder)

// WithBase allows use cfg as a template for start building on top of.
// override parameter signals if settings of the cfg can be overridden or not
// (if setting has a default value it is considered "not configured" and can be
// set despite flag value).
func WithBase(cfg config.Cfg) Option {
	return func(builder *GitalyCfgBuilder) {
		builder.cfg = cfg
	}
}

// WithStorages allows to configure list of storages under this gitaly instance.
// All storages will have a test repository by default.
func WithStorages(name string, names ...string) Option {
	return func(builder *GitalyCfgBuilder) {
		builder.storages = append([]string{name}, names...)
	}
}

// WithPackObjectsCacheEnabled enables the pack object cache.
func WithPackObjectsCacheEnabled() Option {
	return func(builder *GitalyCfgBuilder) {
		builder.packObjectsCacheEnabled = true
	}
}

// WithDisableBundledGitSymlinking stops the builder from symlinking the
// compiled bundled Git binaries in _build/bin to cfg.RuntimeDir. This is
// useful for tests like cmd/gitaly/check_test.go which executes a test
// against a compiled Gitaly binary. If this option is not supplied, both
// the test and the Gitaly process it spawns will try to populate RuntimeDir
// with the same binaries, which causes a "file exists" exception in
// packed_binaries.go
func WithDisableBundledGitSymlinking() Option {
	return func(builder *GitalyCfgBuilder) {
		builder.disableBundledGitSymlinking = true
	}
}

// NewGitalyCfgBuilder returns gitaly configuration builder with configured set of options.
func NewGitalyCfgBuilder(opts ...Option) GitalyCfgBuilder {
	cfgBuilder := GitalyCfgBuilder{}

	for _, opt := range opts {
		opt(&cfgBuilder)
	}

	return cfgBuilder
}

// GitalyCfgBuilder automates creation of the gitaly configuration and filesystem structure required.
type GitalyCfgBuilder struct {
	cfg config.Cfg

	storages                    []string
	packObjectsCacheEnabled     bool
	disableBundledGitSymlinking bool
}

// Build setups required filesystem structure, creates and returns configuration of the gitaly service.
func (gc *GitalyCfgBuilder) Build(tb testing.TB) config.Cfg {
	tb.Helper()

	cfg := gc.cfg
	if cfg.SocketPath == "" {
		cfg.SocketPath = UnconfiguredSocketPath
	}

	root := testhelper.TempDir(tb)

	if cfg.BinDir == "" {
		cfg.BinDir = filepath.Join(root, "bin.d")
		require.NoError(tb, os.Mkdir(cfg.BinDir, mode.Directory))
	}

	if cfg.GitlabShell.Dir == "" {
		cfg.GitlabShell.Dir = filepath.Join(root, "shell.d")
		require.NoError(tb, os.Mkdir(cfg.GitlabShell.Dir, mode.Directory))
	}

	if cfg.RuntimeDir == "" {
		cfg.RuntimeDir = filepath.Join(root, "runtime.d")
		require.NoError(tb, os.Mkdir(cfg.RuntimeDir, mode.Directory))
		require.NoError(tb, os.Mkdir(cfg.InternalSocketDir(), mode.Directory))

		if !gc.disableBundledGitSymlinking {
			// We do this symlinking to emulate what happens with the Git binaries in production:
			// - In production, the Git binaries get unpacked by packed_binaries.go into Gitaly's RuntimeDir, which for
			//   Omnibus is something like /var/opt/gitlab/gitaly/run/gitaly-<xxx>.
			// - In testing, the binaries remain in the _build/bin directory of the repository root.
			bundledGitBins, err := filepath.Glob(filepath.Join(testhelper.SourceRoot(tb), "_build", "bin", "gitaly-git-*"))
			require.NoError(tb, err)

			for _, targetPath := range bundledGitBins {
				destinationPath := filepath.Join(cfg.RuntimeDir, filepath.Base(targetPath))

				// It's possible that the same RuntimeDir is used across multiple invocations of
				// testcfg.Build(), so we simply remove any existing symlinks.
				if _, err := os.Lstat(destinationPath); err == nil {
					require.NoError(tb, os.Remove(destinationPath))
				}
				require.NoError(tb, os.Symlink(targetPath, destinationPath))
			}
		}
	}

	if len(cfg.Storages) != 0 && len(gc.storages) != 0 {
		require.FailNow(tb, "invalid configuration build setup: fix storages configured")
	}

	cfg.PackObjectsCache.Enabled = gc.packObjectsCacheEnabled

	// The tests don't require GitLab API to be accessible, but as it is required to pass
	// validation, so the artificial values are set to pass.
	if cfg.Gitlab.URL == "" {
		cfg.Gitlab.URL = "https://test.stub.gitlab.com"
	}

	if cfg.Gitlab.SecretFile == "" {
		cfg.Gitlab.SecretFile = filepath.Join(root, "gitlab", "http.secret")
		require.NoError(tb, os.MkdirAll(filepath.Dir(cfg.Gitlab.SecretFile), mode.Directory))
		require.NoError(tb, os.WriteFile(cfg.Gitlab.SecretFile, nil, mode.File))
	}

	if len(cfg.Storages) == 0 {
		storagesDir := filepath.Join(root, "storages.d")
		require.NoError(tb, os.Mkdir(storagesDir, mode.Directory))

		if len(gc.storages) == 0 {
			gc.storages = []string{"default"}
		}

		// creation of the required storages (empty storage directories)
		cfg.Storages = make([]config.Storage, len(gc.storages))
		for i, storageName := range gc.storages {
			storagePath := filepath.Join(storagesDir, storageName)
			require.NoError(tb, os.MkdirAll(storagePath, mode.Directory))
			cfg.Storages[i].Name = storageName
			cfg.Storages[i].Path = storagePath
		}
	}

	// Set a non-empty DailyJob so that calling Sanitize() below doesn't attempt to attach a storage to cfg.DailyMaintenance.
	cfg.DailyMaintenance = config.DailyJob{
		Hour:     12,
		Minute:   0,
		Duration: duration.Duration(10 * time.Minute),
	}

	cfg.Transactions.Enabled = testhelper.IsWALEnabled()

	// We force the use of bundled (embedded) binaries unless we're specifically executing tests
	// against a custom version of Git.
	cfg.Git.UseBundledBinaries = os.Getenv("GITALY_TESTING_GIT_BINARY") == ""

	require.NoError(tb, cfg.Sanitize())
	require.NoError(tb, cfg.ValidateV2())

	return cfg
}

// Build creates a minimal configuration setup.
func Build(tb testing.TB, opts ...Option) config.Cfg {
	cfgBuilder := NewGitalyCfgBuilder(opts...)

	return cfgBuilder.Build(tb)
}

// WriteTemporaryGitalyConfigFile writes the given Gitaly configuration into a temporary file and
// returns its path.
func WriteTemporaryGitalyConfigFile(tb testing.TB, cfg config.Cfg) string {
	tb.Helper()

	path := filepath.Join(testhelper.TempDir(tb), "config.toml")

	contents, err := toml.Marshal(cfg)
	require.NoError(tb, err)
	require.NoError(tb, os.WriteFile(path, contents, mode.File))

	return path
}
