package config

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/pelletier/go-toml/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

const (
	// GitalyDataPrefix is the top-level directory we use to store system
	// (non-user) data. We need to be careful that this path does not clash
	// with any directory name that could be provided by a user. The '+'
	// character is not allowed in GitLab namespaces or repositories.
	GitalyDataPrefix = "+gitaly"

	// defaultPackObjectsLimitingConcurrency defines the default concurrency for pack-objects limiting. Pack-objects
	// limiting is scoped by remote IPs. This limit means a single IP could only issue at most 200 distinct requests
	// at the same time. Requests fetching same data lead to only 1 pack-objects command; hence counted as 1.
	defaultPackObjectsLimitingConcurrency = 200
	// defaultPackObjectsLimitingQueueSize defines the default queue size for pack-objects limiting. A request is
	// put into a queue when there are more concurrent requests than defined. This default prevents the queue from
	// growing boundlessly.
	defaultPackObjectsLimitingQueueSize = 200

	// defaultConcurrencyQueueSize defines the default queue size for RPC concurrency limits. This type of limiter
	// is scoped by the RPC and by repository.
	defaultConcurrencyQueueSize = 500
)

// configKeyRegex is intended to verify config keys in their `core.gc` or
// `http.http://example.com.proxy` format.
var configKeyRegex = regexp.MustCompile(`^[[:alnum:]]+(\.[*-/_:@a-zA-Z0-9]+)+$`)

// DailyJob enables a daily task to be scheduled for specific storages
type DailyJob struct {
	Hour     uint              `json:"start_hour"   toml:"start_hour,omitempty"`
	Minute   uint              `json:"start_minute" toml:"start_minute,omitempty"`
	Duration duration.Duration `json:"duration"     toml:"duration,omitempty"`
	Storages []string          `json:"storages"     toml:"storages,omitempty"`

	// Disabled will completely disable a daily job, even in cases where a
	// default schedule is implied
	Disabled bool `json:"disabled" toml:"disabled,omitempty"`
}

// IsDisabled returns true if the daily job is disabled and should not run.
func (dj DailyJob) IsDisabled() bool {
	return dj.Duration == 0 || len(dj.Storages) == 0 || dj.Disabled
}

// Validate runs validation on all fields and compose all found errors.
func (dj DailyJob) Validate(allowedStorages []string) error {
	if dj.Disabled {
		return nil
	}

	inRangeOpts := []cfgerror.InRangeOpt{cfgerror.InRangeOptIncludeMin, cfgerror.InRangeOptIncludeMax}
	errs := cfgerror.New().
		Append(cfgerror.InRange(0, 23, dj.Hour, inRangeOpts...), "start_hour").
		Append(cfgerror.InRange(0, 59, dj.Minute, inRangeOpts...), "start_minute").
		Append(cfgerror.InRange(time.Duration(0), 24*time.Hour, dj.Duration.Duration(), inRangeOpts...), "duration")

	for i, storage := range dj.Storages {
		var found bool
		for _, allowed := range allowedStorages {
			if allowed == storage {
				found = true
				break
			}
		}
		if !found {
			cause := fmt.Errorf("%w: %q", cfgerror.ErrDoesntExist, storage)
			errs = errs.Append(cfgerror.NewValidationError(cause, "storages", fmt.Sprintf("[%d]", i)))
		}
	}

	return errs.AsError()
}

// Cfg is a container for all config derived from config.toml.
type Cfg struct {
	// ConfigCommand specifies the path to an executable that Gitaly will run after loading the
	// initial configuration from disk. The executable is expected to write JSON-formatted
	// configuration to its standard output that we will then deserialize and merge back into
	// the initially-loaded configuration again. This is an easy mechanism to generate parts of
	// the configuration at runtime, like for example secrets.
	ConfigCommand        string            `                    json:"config_command"                            toml:"config_command,omitempty"`
	SocketPath           string            `                    json:"socket_path"            split_words:"true" toml:"socket_path,omitempty"`
	ListenAddr           string            `                    json:"listen_addr"            split_words:"true" toml:"listen_addr,omitempty"`
	TLSListenAddr        string            `                    json:"tls_listen_addr"        split_words:"true" toml:"tls_listen_addr,omitempty"`
	PrometheusListenAddr string            `                    json:"prometheus_listen_addr" split_words:"true" toml:"prometheus_listen_addr,omitempty"`
	BinDir               string            `                    json:"bin_dir"                                   toml:"bin_dir,omitempty"`
	RuntimeDir           string            `                    json:"runtime_dir"                               toml:"runtime_dir,omitempty"`
	Git                  Git               `envconfig:"git"     json:"git"                                       toml:"git,omitempty"`
	Storages             []Storage         `envconfig:"storage" json:"storage"                                   toml:"storage,omitempty"`
	Logging              Logging           `envconfig:"logging" json:"logging"                                   toml:"logging,omitempty"`
	Prometheus           prometheus.Config `                    json:"prometheus"                                toml:"prometheus,omitempty"`
	Auth                 auth.Config       `                    json:"auth"                                      toml:"auth,omitempty"`
	TLS                  TLS               `                    json:"tls"                                       toml:"tls,omitempty"`
	Gitlab               Gitlab            `                    json:"gitlab"                                    toml:"gitlab,omitempty"`
	// GitlabShell contains the location of the gitlab-shell directory. This directory is expected to contain two
	// things:
	//
	// - The GitLab secret file ".gitlab_shell_secret", which is used to authenticate with GitLab. This should
	//   instead be configured via "gitlab.secret" or "gitlab.secret_file".
	//
	// - The custom hooks directory "hooks". This should instead be configured via "hooks.custom_hooks_dir".
	//
	// This setting is thus deprecated and should ideally not be used anymore.
	GitlabShell            GitlabShell         `json:"gitlab-shell"                toml:"gitlab-shell,omitempty"`
	Hooks                  Hooks               `json:"hooks"                       toml:"hooks,omitempty"`
	Concurrency            []Concurrency       `json:"concurrency"                 toml:"concurrency,omitempty"`
	RateLimiting           []RateLimiting      `json:"rate_limiting"               toml:"rate_limiting,omitempty"`
	GracefulRestartTimeout duration.Duration   `json:"graceful_restart_timeout"    toml:"graceful_restart_timeout,omitempty"`
	DailyMaintenance       DailyJob            `json:"daily_maintenance"           toml:"daily_maintenance,omitempty"`
	Cgroups                cgroups.Config      `json:"cgroups"                     toml:"cgroups,omitempty"`
	PackObjectsCache       StreamCacheConfig   `json:"pack_objects_cache"          toml:"pack_objects_cache,omitempty"`
	PackObjectsLimiting    PackObjectsLimiting `json:"pack_objects_limiting"       toml:"pack_objects_limiting,omitempty"`
	Backup                 BackupConfig        `json:"backup"                      toml:"backup,omitempty"`
	BundleURI              BundleURIConfig     `json:"bundle_uri"                  toml:"bundle_uri,omitempty"`
	Timeout                TimeoutConfig       `json:"timeout"                     toml:"timeout,omitempty"`
	Transactions           Transactions        `json:"transactions,omitempty"      toml:"transactions,omitempty"`
	AdaptiveLimiting       AdaptiveLimiting    `json:"adaptive_limiting,omitempty" toml:"adaptive_limiting,omitempty"`
	Raft                   Raft                `json:"raft,omitempty"              toml:"raft,omitempty"`
}

// Transactions configures transaction related options.
type Transactions struct {
	// Enabled enables transaction support. This option is experimental
	// and intended for development only. Do not enable for other uses.
	Enabled bool `json:"enabled,omitempty" toml:"enabled,omitempty"`
}

// TimeoutConfig represents negotiation timeouts for remote Git operations
type TimeoutConfig struct {
	// UploadPackNegotiation configures the timeout for git-upload-pack(1) when negotiating the packfile. This does not
	// influence any potential timeouts when the packfile is being sent to the client.
	UploadPackNegotiation duration.Duration `json:"upload_pack_negotiation,omitempty" toml:"upload_pack_negotiation,omitempty"`
	// UploadArchiveNegotiation configures the timeout for git-upload-archive(1) when negotiating the archive. This does not
	// influence any potential timeouts when the archive is being sent to the client.
	UploadArchiveNegotiation duration.Duration `json:"upload_archive_negotiation,omitempty" toml:"upload_archive_negotiation,omitempty"`
}

// TLS configuration
type TLS struct {
	CertPath string `json:"cert_path" toml:"certificate_path,omitempty"`
	KeyPath  string `json:"key_path"  toml:"key_path,omitempty"`
	Key      string `json:"key"       toml:"key,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (t TLS) Validate() error {
	if t.CertPath == "" && t.KeyPath == "" && t.Key == "" {
		return nil
	}

	if t.Key != "" && t.KeyPath != "" {
		return cfgerror.NewValidationError(
			errors.New("key_path and key cannot both be set"),
			"key_path",
			"key",
		)
	}

	errs := cfgerror.New().
		Append(cfgerror.FileExists(t.CertPath), "certificate_path")

	if t.Key == "" {
		errs = errs.Append(cfgerror.FileExists(t.KeyPath), "key_path")
	}

	if len(errs) != 0 {
		// In case of problems with files attempt to load
		// will fail and pollute output with useless info.
		return errs.AsError()
	}

	if _, err := t.Certificate(); err != nil {
		var field string

		if strings.Contains(err.Error(), "in certificate input") ||
			strings.Contains(err.Error(), "certificate_path") {
			field = "certificate_path"
		} else if t.Key != "" {
			field = "key"
		} else {
			field = "key_path"
		}

		return cfgerror.NewValidationError(err, field)
	}

	return nil
}

// Certificate gets the certificate with the certificate path and either the key
// path or the key.
func (t TLS) Certificate() (tls.Certificate, error) {
	if t.Key != "" {
		certPEMBlock, err := os.ReadFile(t.CertPath)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("reading certificate_path: %w", err)
		}

		cert, err := tls.X509KeyPair(certPEMBlock, []byte(t.Key))
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("loading x509 keypair: %w", err)
		}

		return cert, nil
	}

	cert, err := tls.LoadX509KeyPair(t.CertPath, t.KeyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("loading x509 keypair: %w", err)
	}

	return cert, nil
}

// GitlabShell contains the settings required for executing `gitlab-shell`
type GitlabShell struct {
	Dir string `json:"dir" toml:"dir"`
}

// Validate runs validation on all fields and compose all found errors.
func (gs GitlabShell) Validate() error {
	if len(gs.Dir) == 0 {
		return nil
	}

	return cfgerror.New().
		Append(cfgerror.DirExists(gs.Dir), "dir").
		AsError()
}

// Gitlab contains settings required to connect to the Gitlab api
type Gitlab struct {
	URL             string       `json:"url"               toml:"url,omitempty"`
	RelativeURLRoot string       `json:"relative_url_root" toml:"relative_url_root,omitempty"` // For UNIX sockets only
	HTTPSettings    HTTPSettings `json:"http_settings"     toml:"http-settings,omitempty"`
	SecretFile      string       `json:"secret_file"       toml:"secret_file,omitempty"`
	// Secret contains the Gitlab secret directly. Should not be set if secret file is specified.
	Secret string `json:"secret" toml:"secret,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (gl Gitlab) Validate() error {
	var errs cfgerror.ValidationErrors
	if err := cfgerror.NotBlank(gl.URL); err != nil {
		errs = errs.Append(err, "url")
	} else {
		if _, err := url.Parse(gl.URL); err != nil {
			errs = errs.Append(err, "url")
		}
	}

	// If both secret and secret_file are set, the configuration is considered ambiguous results a
	// validation error. Only one of the fields should be set.
	if gl.Secret != "" && gl.SecretFile != "" {
		errs = errs.Append(errors.New("ambiguous secret configuration"), "secret", "secret_file")
	}

	// The secrets file is only required to exist if the secret is not directly configured.
	if gl.Secret == "" {
		errs = errs.Append(cfgerror.FileExists(gl.SecretFile), "secret_file")
	}

	return errs.Append(gl.HTTPSettings.Validate(), "http-settings").AsError()
}

// Hooks contains the settings required for hooks
type Hooks struct {
	CustomHooksDir string `json:"custom_hooks_dir" toml:"custom_hooks_dir,omitempty"`
}

// HTTPSettings contains configuration settings used to setup HTTP transport
// and basic HTTP authorization.
type HTTPSettings struct {
	ReadTimeout uint64 `json:"read_timeout" toml:"read_timeout,omitempty"`
	User        string `json:"user"         toml:"user,omitempty"`
	Password    string `json:"password"     toml:"password,omitempty"`
	CAFile      string `json:"ca_file"      toml:"ca_file,omitempty"`
	CAPath      string `json:"ca_path"      toml:"ca_path,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (ss HTTPSettings) Validate() error {
	var errs cfgerror.ValidationErrors
	if ss.User != "" || ss.Password != "" {
		// If one of the basic auth parameters is set the other one must be set as well.
		errs = errs.Append(cfgerror.NotBlank(ss.User), "user").
			Append(cfgerror.NotBlank(ss.Password), "password")
	}

	if ss.CAFile != "" {
		errs = errs.Append(cfgerror.FileExists(ss.CAFile), "ca_file")
	}

	if ss.CAPath != "" {
		errs = errs.Append(cfgerror.DirExists(ss.CAPath), "ca_path")
	}

	return errs.AsError()
}

// Git contains the settings for the Git executable
type Git struct {
	UseBundledBinaries bool        `json:"use_bundled_binaries" toml:"use_bundled_binaries,omitempty"`
	BinPath            string      `json:"bin_path"             toml:"bin_path,omitempty"`
	CatfileCacheSize   int         `json:"catfile_cache_size"   toml:"catfile_cache_size,omitempty"`
	Config             []GitConfig `json:"config"               toml:"config,omitempty"`
	// SigningKey is the private key used for signing commits created by Gitaly
	SigningKey string `json:"signing_key" toml:"signing_key,omitempty"`
	// RotatedSigningKeys are the private keys that have used for commit signing before.
	// The keys from the SigningKey field is moved into this field for some time to rotate signing keys.
	RotatedSigningKeys []string `json:"rotated_signing_keys" toml:"rotated_signing_keys,omitempty"`
	// CommitterEmail is the committer email of the commits created by Gitaly, e.g. `noreply@gitlab.com`
	CommitterEmail string `json:"committer_email" toml:"committer_email,omitempty"`
	// CommitterName is the committer name of the commits created by Gitaly, e.g. `GitLab`
	CommitterName string `json:"committer_name" toml:"committer_name,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (g Git) Validate() error {
	var errs cfgerror.ValidationErrors
	for _, gc := range g.Config {
		errs = errs.Append(gc.Validate(), "config")
	}

	return errs.AsError()
}

// GitConfig contains a key-value pair which is to be passed to git as configuration.
type GitConfig struct {
	// Key is the key of the config entry, e.g. `core.gc`.
	Key string `json:"key" toml:"key,omitempty"`
	// Value is the value of the config entry, e.g. `false`.
	Value string `json:"value" toml:"value,omitempty"`
}

// Validate validates that the Git configuration conforms to a format that Git understands.
func (cfg GitConfig) Validate() error {
	// Even though redundant, this block checks for a few things up front to give better error
	// messages to the administrator in case any of the keys fails validation.
	if cfg.Key == "" {
		return cfgerror.NewValidationError(cfgerror.ErrNotSet, "key")
	}
	if strings.Contains(cfg.Key, "=") {
		return cfgerror.NewValidationError(
			fmt.Errorf(`key %q cannot contain "="`, cfg.Key),
			"key",
		)
	}
	if !strings.Contains(cfg.Key, ".") {
		return cfgerror.NewValidationError(
			fmt.Errorf("key %q must contain at least one section", cfg.Key),
			"key",
		)
	}
	if strings.HasPrefix(cfg.Key, ".") || strings.HasSuffix(cfg.Key, ".") {
		return cfgerror.NewValidationError(
			fmt.Errorf("key %q must not start or end with a dot", cfg.Key),
			"key",
		)
	}

	if !configKeyRegex.MatchString(cfg.Key) {
		return cfgerror.NewValidationError(
			fmt.Errorf("key %q failed regexp validation", cfg.Key),
			"key",
		)
	}

	return nil
}

// GlobalArgs generates a git `-c <key>=<value>` flag. Returns an error if `Validate()` fails to
// validate the config key.
func (cfg GitConfig) GlobalArgs() ([]string, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration key %q: %w", cfg.Key, err)
	}

	return []string{"-c", fmt.Sprintf("%s=%s", cfg.Key, cfg.Value)}, nil
}

// Storage contains a single storage-shard
type Storage struct {
	Name string `toml:"name"`
	Path string `toml:"path"`
}

// Validate runs validation on all fields and compose all found errors.
func (s Storage) Validate() error {
	return cfgerror.New().
		Append(cfgerror.NotEmpty(s.Name), "name").
		Append(cfgerror.DirExists(s.Path), "path").
		AsError()
}

func (cfg *Cfg) validateStorages() error {
	if len(cfg.Storages) == 0 {
		return cfgerror.NewValidationError(cfgerror.ErrNotSet)
	}

	var errs cfgerror.ValidationErrors
	for i, s := range cfg.Storages {
		errs = errs.Append(s.Validate(), fmt.Sprintf("[%d]", i))
	}

	for i, storage := range cfg.Storages {
		for _, other := range cfg.Storages[:i] {
			if other.Name == storage.Name {
				err := fmt.Errorf("%w: %q", cfgerror.ErrNotUnique, storage.Name)
				cause := cfgerror.NewValidationError(err, "name")
				errs = errs.Append(cause, fmt.Sprintf("[%d]", i))
			}

			if storage.Path == other.Path {
				err := fmt.Errorf("%w: %q", cfgerror.ErrNotUnique, storage.Path)
				cause := cfgerror.NewValidationError(err, "path")
				errs = errs.Append(cause, fmt.Sprintf("[%d]", i))
				continue
			}

			if storage.Path == "" || other.Path == "" {
				// If one of Path-s is not set the code below will produce an error
				// that only confuses, so we stop here.
				continue
			}

			if strings.HasPrefix(storage.Path, other.Path) || strings.HasPrefix(other.Path, storage.Path) {
				// If storages have the same subdirectory, that is allowed.
				if filepath.Dir(storage.Path) == filepath.Dir(other.Path) {
					continue
				}

				cause := fmt.Errorf("can't nest: %q and %q", storage.Path, other.Path)
				err := cfgerror.NewValidationError(cause, "path")
				errs = errs.Append(err, fmt.Sprintf("[%d]", i))
			}
		}
	}

	return errs.AsError()
}

// Sentry is a sentry.Config. We redefine this type to a different name so
// we can embed both structs into Logging
type Sentry sentry.Config

// Logging contains the logging configuration for Gitaly
type Logging struct {
	log.Config
	Sentry
}

// Concurrency allows endpoints to be limited to a maximum concurrency per repo.
// Requests that come in after the maximum number of concurrent requests are in progress will wait
// in a queue that is bounded by MaxQueueSize.
type Concurrency struct {
	// RPC is the name of the RPC to set concurrency limits for
	RPC string `json:"rpc" toml:"rpc"`
	// Adaptive determines the behavior of the concurrency limit. If set to true, the concurrency limit is dynamic
	// and starts at InitialLimit, then adjusts within the range [MinLimit, MaxLimit] based on current resource
	// usage. If set to false, the concurrency limit is static and is set to MaxPerRepo.
	Adaptive bool `json:"adaptive,omitempty" toml:"adaptive,omitempty"`
	// InitialLimit is the concurrency limit to start with.
	InitialLimit int `json:"initial_limit,omitempty" toml:"initial_limit,omitempty"`
	// MaxLimit is the minimum adaptive concurrency limit.
	MaxLimit int `json:"max_limit,omitempty" toml:"max_limit,omitempty"`
	// MinLimit is the mini adaptive concurrency limit.
	MinLimit int `json:"min_limit,omitempty" toml:"min_limit,omitempty"`
	// MaxPerRepo is the maximum number of concurrent calls for a given repository. This config is used only
	// if Adaptive is false.
	MaxPerRepo int `json:"max_per_repo" toml:"max_per_repo"`
	// MaxQueueSize is the maximum number of requests in the queue waiting to be picked up
	// after which subsequent requests will return with an error.
	MaxQueueSize int `json:"max_queue_size" toml:"max_queue_size"`
	// MaxQueueWait is the maximum time a request can remain in the concurrency queue
	// waiting to be picked up by Gitaly
	MaxQueueWait duration.Duration `json:"max_queue_wait" toml:"max_queue_wait"`
}

// Validate runs validation on all fields and compose all found errors.
func (c Concurrency) Validate() error {
	errs := cfgerror.New().
		Append(cfgerror.Comparable(c.MaxPerRepo).GreaterOrEqual(0), "max_per_repo").
		Append(cfgerror.Comparable(c.MaxQueueSize).GreaterThan(0), "max_queue_size").
		Append(cfgerror.Comparable(c.MaxQueueWait.Duration()).GreaterOrEqual(0), "max_queue_wait")

	if c.Adaptive {
		errs = errs.
			Append(cfgerror.Comparable(c.MinLimit).GreaterThan(0), "min_limit").
			Append(cfgerror.Comparable(c.MaxLimit).GreaterOrEqual(c.InitialLimit), "max_limit").
			Append(cfgerror.Comparable(c.InitialLimit).GreaterOrEqual(c.MinLimit), "initial_limit")
	}
	return errs.AsError()
}

// AdaptiveLimiting defines a set of global config for the adaptive limiter. This config customizes how the resource
// watchers and calculator works. Specific limits for each RPC or pack-objects operation should be configured
// individually using the Concurrency and PackObjectsLimiting structs respectively.
type AdaptiveLimiting struct {
	// CPUThrottledThreshold defines the CPU throttling ratio threshold for a backoff event. The resource watcher
	// compares the recorded total throttled time between two polls. If the throttled time exceeds this threshold of
	// the observation window, it returns a backoff event. By default, the threshold is 0.5 (50%).
	CPUThrottledThreshold float64 `json:"cpu_throttled_threshold" toml:"cpu_throttled_threshold"`
	// MemoryThreshold defines the memory threshold for a backoff event. The memory watcher compares the recorded
	// memory usage (excluding high evictable page caches) to the defined limit. If the ratio exceeds this
	// threshold, a backoff event is fired. By default, the threshold is 0.9 (90%).
	MemoryThreshold float64 `json:"memory_threshold" toml:"memory_threshold"`
}

// Validate runs validation on all fields and compose all found errors.
func (c AdaptiveLimiting) Validate() error {
	return cfgerror.New().
		Append(cfgerror.Comparable(c.CPUThrottledThreshold).GreaterOrEqual(0), "cpu_throttled_threshold").
		Append(cfgerror.Comparable(c.MemoryThreshold).GreaterOrEqual(0), "memory_threshold").
		AsError()
}

// RateLimiting allows endpoints to be limited to a maximum request rate per
// second. The rate limiter uses a concept of a "token bucket". In order to serve a
// request, a token is retrieved from the token bucket. The size of the token
// bucket is configured through the Burst value, while the rate at which the
// token bucket is refilled per second is configured through the RequestsPerSecond
// value.
type RateLimiting struct {
	// RPC is the full name of the RPC including the service name
	RPC string `json:"rpc" toml:"rpc"`
	// Interval sets the interval with which the token bucket will
	// be refilled to what is configured in Burst.
	Interval duration.Duration `json:"interval" toml:"interval"`
	// Burst sets the capacity of the token bucket (see above).
	Burst int `json:"burst" toml:"burst"`
}

// PackObjectsLimiting allows the concurrency of pack objects processes to be limited
// Requests that come in after the maximum number of concurrent pack objects
// processes have been reached will wait.
type PackObjectsLimiting struct {
	// Adaptive determines the behavior of the concurrency limit. If set to true, the concurrency limit is dynamic
	// and starts at InitialLimit, then adjusts within the range [MinLimit, MaxLimit] based on current resource
	// usage. If set to false, the concurrency limit is static and is set to MaxConcurrency.
	Adaptive bool `json:"adaptive,omitempty" toml:"adaptive,omitempty"`
	// InitialLimit is the concurrency limit to start with.
	InitialLimit int `json:"initial_limit,omitempty" toml:"initial_limit,omitempty"`
	// MaxLimit is the minimum adaptive concurrency limit.
	MaxLimit int `json:"max_limit,omitempty" toml:"max_limit,omitempty"`
	// MinLimit is the mini adaptive concurrency limit.
	MinLimit int `json:"min_limit,omitempty" toml:"min_limit,omitempty"`
	// MaxConcurrency is the static maximum number of concurrent pack objects processes for a given key. This config
	// is used only if Adaptive is false.
	MaxConcurrency int `json:"max_concurrency,omitempty" toml:"max_concurrency,omitempty"`
	// MaxQueueWait is the maximum time a request can remain in the concurrency queue
	// waiting to be picked up by Gitaly.
	MaxQueueWait duration.Duration `json:"max_queue_wait,omitempty" toml:"max_queue_wait,omitempty"`
	// MaxQueueLength is the maximum length of the request queue
	MaxQueueLength int `json:"max_queue_length,omitempty" toml:"max_queue_length,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (pol PackObjectsLimiting) Validate() error {
	return cfgerror.New().
		Append(cfgerror.Comparable(pol.MaxConcurrency).GreaterOrEqual(0), "max_concurrency").
		Append(cfgerror.Comparable(pol.MaxQueueLength).GreaterThan(0), "max_queue_length").
		Append(cfgerror.Comparable(pol.MaxQueueWait.Duration()).GreaterOrEqual(0), "max_queue_wait").
		Append(cfgerror.Comparable(pol.MinLimit).GreaterOrEqual(0), "min_limit").
		Append(cfgerror.Comparable(pol.MaxLimit).GreaterOrEqual(pol.InitialLimit), "max_limit").
		Append(cfgerror.Comparable(pol.InitialLimit).GreaterOrEqual(pol.MinLimit), "initial_limit").
		AsError()
}

// BackupConfig configures server-side and write-ahead log backups.
type BackupConfig struct {
	// GoCloudURL is the blob storage GoCloud URL that will be used to store
	// server-side backups.
	GoCloudURL string `json:"go_cloud_url,omitempty" toml:"go_cloud_url,omitempty"`
	// WALGoCloudURL is the blob storage GoCloud URL that will be used to store
	// write-ahead log backups.
	WALGoCloudURL string `json:"wal_backup_go_cloud_url,omitempty" toml:"wal_backup_go_cloud_url,omitempty"`
	// WALWorkerCount controls the number of goroutines used to backup write-ahead log entries.
	WALWorkerCount uint `json:"wal_backup_worker_count,omitempty" toml:"wal_backup_worker_count,omitempty"`
	// Layout determines how backup files are located.
	Layout string `json:"layout,omitempty" toml:"layout,omitempty"`
	// BufferSize specifies the size of the buffer used when uploading backup parts to object storage.
	BufferSize int `json:"buffer_size,omitempty" toml:"buffer_size,omitempty"`
}

// Validate runs validation on all fields and returns any errors found.
func (bc BackupConfig) Validate() error {
	var errs cfgerror.ValidationErrors

	if bc.GoCloudURL != "" {
		if _, err := url.Parse(bc.GoCloudURL); err != nil {
			errs = errs.Append(err, "go_cloud_url")
		}

		errs = errs.Append(cfgerror.NotBlank(bc.Layout), "layout")
	}

	if bc.WALGoCloudURL != "" {
		if _, err := url.Parse(bc.WALGoCloudURL); err != nil {
			errs = errs.Append(err, "wal_backup_go_cloud_url")
		}
	}

	return errs.AsError()
}

// BundleURIConfig configures use of Bundle-URI
type BundleURIConfig struct {
	// GoCloudURL is the blob storage GoCloud URL that will be used to store
	// Git bundles for Bundle-URI use.
	GoCloudURL string `json:"go_cloud_url,omitempty" toml:"go_cloud_url,omitempty"`
}

// Validate runs validation on all fields and returns any errors found.
func (bc BundleURIConfig) Validate() error {
	if bc.GoCloudURL == "" {
		return nil
	}
	var errs cfgerror.ValidationErrors

	if _, err := url.Parse(bc.GoCloudURL); err != nil {
		errs = errs.Append(err, "go_cloud_url")
	}

	return errs
}

// StreamCacheConfig contains settings for a streamcache instance.
type StreamCacheConfig struct {
	Enabled        bool              `json:"enabled"         toml:"enabled"` // Default: false
	Dir            string            `json:"dir"             toml:"dir"`     // Default: <FIRST STORAGE PATH>/+gitaly/PackObjectsCache
	MaxAge         duration.Duration `json:"max_age"         toml:"max_age"` // Default: 5m
	MinOccurrences int               `json:"min_occurrences" toml:"min_occurrences"`
}

// Validate runs validation on all fields and compose all found errors.
func (scc StreamCacheConfig) Validate() error {
	if !scc.Enabled {
		return nil
	}

	return cfgerror.New().
		Append(cfgerror.PathIsAbs(scc.Dir), "dir").
		Append(cfgerror.Comparable(scc.MaxAge.Duration()).GreaterOrEqual(0), "max_age").
		AsError()
}

func defaultPackObjectsCacheConfig() StreamCacheConfig {
	return StreamCacheConfig{
		// The Pack-Objects cache is effective at deduplicating concurrent
		// identical fetches such as those coming from CI pipelines. But for
		// unique requests, it adds no value. By setting this minimum to 1, we
		// prevent unique requests from being cached, which saves about 50% of
		// cache disk space. Also see
		// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/2222.
		MinOccurrences: 1,
	}
}

func defaultPackObjectsLimiting() PackObjectsLimiting {
	return PackObjectsLimiting{
		MaxConcurrency: defaultPackObjectsLimitingConcurrency,
		MaxQueueLength: defaultPackObjectsLimitingQueueSize,
		// Requests can stay in the queue as long as they want
		MaxQueueWait: 0,
	}
}

// Load initializes the Config variable from file and the environment.
// Environment variables take precedence over the file.
func Load(file io.Reader) (Cfg, error) {
	cfg := Cfg{
		Prometheus:          prometheus.DefaultConfig(),
		PackObjectsCache:    defaultPackObjectsCacheConfig(),
		PackObjectsLimiting: defaultPackObjectsLimiting(),
	}

	if err := toml.NewDecoder(file).Decode(&cfg); err != nil {
		return Cfg{}, fmt.Errorf("load toml: %w", err)
	}

	if cfg.ConfigCommand != "" {
		output, err := exec.Command(cfg.ConfigCommand).Output()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				return Cfg{}, fmt.Errorf("running config command: %w, stderr: %q", err, string(exitErr.Stderr))
			}

			return Cfg{}, fmt.Errorf("running config command: %w", err)
		}

		if err := json.Unmarshal(output, &cfg); err != nil {
			return Cfg{}, fmt.Errorf("unmarshalling generated config: %w", err)
		}
	}

	if err := cfg.Sanitize(); err != nil {
		return Cfg{}, err
	}

	for i := range cfg.Storages {
		cfg.Storages[i].Path = filepath.Clean(cfg.Storages[i].Path)
	}

	return cfg, nil
}

// Validate checks the current Config for sanity.
// Deprecated: Use ValidateV2 instead.
func (cfg *Cfg) Validate() error {
	for _, run := range []func() error{
		cfg.validateListeners,
		cfg.validateStorages,
		cfg.validateGit,
		cfg.validateGitlabSecret,
		cfg.validateBinDir,
		cfg.validateRuntimeDir,
		cfg.validateMaintenance,
		cfg.validateCgroups,
		cfg.configurePackObjectsCache,
	} {
		if err := run(); err != nil {
			return err
		}
	}

	return nil
}

// ValidateV2 is a new validation method that is a replacement for the existing Validate.
// It exists as a demonstration of the new validation implementation based on the usage
// of the cfgerror package.
func (cfg *Cfg) ValidateV2() error {
	var errs cfgerror.ValidationErrors
	for _, check := range []struct {
		field    string
		validate func() error
	}{
		{field: "", validate: func() error {
			if cfg.SocketPath == "" && cfg.ListenAddr == "" && cfg.TLSListenAddr == "" {
				return fmt.Errorf(`none of "socket_path", "listen_addr" or "tls_listen_addr" is set`)
			}
			return nil
		}},
		{field: "bin_dir", validate: func() error {
			return cfgerror.DirExists(cfg.BinDir)
		}},
		{field: "runtime_dir", validate: func() error {
			if cfg.RuntimeDir != "" {
				return cfgerror.DirExists(cfg.RuntimeDir)
			}
			return nil
		}},
		{field: "git", validate: cfg.Git.Validate},
		{field: "storage", validate: func() error {
			return cfg.validateStorages()
		}},
		{field: "prometheus", validate: cfg.Prometheus.Validate},
		{field: "tls", validate: cfg.TLS.Validate},
		{field: "gitlab", validate: cfg.Gitlab.Validate},
		{field: "gitlab-shell", validate: cfg.GitlabShell.Validate},
		{field: "graceful_restart_timeout", validate: func() error {
			return cfgerror.Comparable(cfg.GracefulRestartTimeout.Duration()).GreaterOrEqual(0)
		}},
		{field: "daily_maintenance", validate: func() error {
			storages := make([]string, len(cfg.Storages))
			for i := 0; i < len(cfg.Storages); i++ {
				storages[i] = cfg.Storages[i].Name
			}
			return cfg.DailyMaintenance.Validate(storages)
		}},
		{field: "cgroups", validate: cfg.Cgroups.Validate},
		{field: "concurrency", validate: func() error {
			var errs cfgerror.ValidationErrors
			for i, concurrency := range cfg.Concurrency {
				errs = errs.Append(concurrency.Validate(), fmt.Sprintf("[%d]", i))
			}
			return errs.AsError()
		}},
		{field: "pack_objects_cache", validate: cfg.PackObjectsCache.Validate},
		{field: "pack_objects_limiting", validate: cfg.PackObjectsLimiting.Validate},
		{field: "backup", validate: cfg.Backup.Validate},
		{field: "raft", validate: func() error {
			return cfg.Raft.Validate(cfg.Transactions)
		}},
	} {
		var fields []string
		if check.field != "" {
			fields = append(fields, check.field)
		}
		errs = errs.Append(check.validate(), fields...)
	}

	return errs.AsError()
}

// Sanitize sets the default options for Cfg and adjusts other options to conform
// to what Gitaly expects (such as using absolute paths, etc.).
func (cfg *Cfg) Sanitize() error {
	if cfg.BinDir != "" {
		var err error
		if cfg.BinDir, err = filepath.Abs(cfg.BinDir); err != nil {
			return err
		}
	}

	if cfg.RuntimeDir != "" {
		var err error
		if cfg.RuntimeDir, err = filepath.Abs(cfg.RuntimeDir); err != nil {
			return err
		}
	}

	if cfg.PackObjectsCache.Enabled {
		if cfg.PackObjectsCache.MaxAge == 0 {
			cfg.PackObjectsCache.MaxAge = duration.Duration(5 * time.Minute)
		}

		if cfg.PackObjectsCache.Dir == "" && len(cfg.Storages) > 0 {
			cfg.PackObjectsCache.Dir = filepath.Join(cfg.Storages[0].Path, GitalyDataPrefix, "PackObjectsCache")
		}
	}

	if cfg.PackObjectsLimiting.MaxQueueLength == 0 {
		cfg.PackObjectsLimiting.MaxQueueLength = defaultPackObjectsLimitingQueueSize
	}

	for i := range cfg.Concurrency {
		if cfg.Concurrency[i].MaxQueueSize == 0 {
			cfg.Concurrency[i].MaxQueueSize = defaultConcurrencyQueueSize
		}
	}

	if cfg.GracefulRestartTimeout.Duration() == 0 {
		cfg.GracefulRestartTimeout = duration.Duration(time.Minute)
	}

	// Only set default secret file if the secret is not configured directly.
	if cfg.Gitlab.SecretFile == "" && cfg.Gitlab.Secret == "" {
		cfg.Gitlab.SecretFile = filepath.Join(cfg.GitlabShell.Dir, ".gitlab_shell_secret")
	}

	if cfg.Hooks.CustomHooksDir == "" && cfg.GitlabShell.Dir != "" {
		cfg.Hooks.CustomHooksDir = filepath.Join(cfg.GitlabShell.Dir, "hooks")
	}

	if reflect.DeepEqual(cfg.DailyMaintenance, DailyJob{}) {
		cfg.DailyMaintenance = defaultMaintenanceWindow(cfg.Storages)
	}

	if cfg.Cgroups.Mountpoint == "" {
		cfg.Cgroups.Mountpoint = "/sys/fs/cgroup"
	}

	if cfg.Cgroups.HierarchyRoot == "" {
		cfg.Cgroups.HierarchyRoot = "gitaly"
	}

	cfg.Cgroups.FallbackToOldVersion()

	if cfg.Cgroups.Repositories.Count != 0 && cfg.Cgroups.Repositories.MaxCgroupsPerRepo == 0 {
		cfg.Cgroups.Repositories.MaxCgroupsPerRepo = 1
	}

	if cfg.Backup.Layout == "" {
		cfg.Backup.Layout = "pointer"
	}

	if cfg.Backup.WALWorkerCount == 0 {
		cfg.Backup.WALWorkerCount = 1
	}

	if cfg.Timeout.UploadPackNegotiation == 0 {
		cfg.Timeout.UploadPackNegotiation = duration.Duration(10 * time.Minute)
	}

	if cfg.Timeout.UploadArchiveNegotiation == 0 {
		cfg.Timeout.UploadArchiveNegotiation = duration.Duration(time.Minute)
	}

	if cfg.Raft.Enabled {
		cfg.Raft = cfg.Raft.fulfillDefaults()
		if cfg.Raft.SnapshotDir == "" && len(cfg.Storages) > 0 {
			cfg.Raft.SnapshotDir = filepath.Join(cfg.Storages[0].Path, GitalyDataPrefix, "raft/snapshots")
		}
	}

	if cfg.Logging.Config.Format == "" {
		cfg.Logging.Config.Format = "text"
	}

	if cfg.Logging.Config.Level == "" {
		cfg.Logging.Config.Level = "info"
	}

	return nil
}

func (cfg *Cfg) validateListeners() error {
	if len(cfg.SocketPath) == 0 && len(cfg.ListenAddr) == 0 && len(cfg.TLSListenAddr) == 0 {
		return fmt.Errorf("at least one of socket_path, listen_addr or tls_listen_addr must be set")
	}
	return nil
}

func (cfg *Cfg) validateGitlabSecret() error {
	switch {
	case len(cfg.Gitlab.Secret) > 0:
		return nil
	case len(cfg.Gitlab.SecretFile) > 0:
		// Ideally, we would raise an error if the secret file doesn't exist, but there are too many setups out
		// there right now where things are broken. So we don't and need to reintroduce this at a later point.
		return nil
	case len(cfg.GitlabShell.Dir) > 0:
		// Note that we do not verify that the secret actually exists, but only verify that the directory
		// exists. This is not as thorough as we could be, but is done in order to retain our legacy behaviour
		// in case the secret file wasn't explicitly configured.
		return validateIsDirectory(cfg.GitlabShell.Dir, "gitlab-shell.dir")
	default:
		return fmt.Errorf("GitLab secret not configured")
	}
}

func validateIsDirectory(path, name string) error {
	s, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: path doesn't exist: %q", name, path)
		}
		return fmt.Errorf("%s: %w", name, err)
	}
	if !s.IsDir() {
		return fmt.Errorf("%s: not a directory: %q", name, path)
	}

	return nil
}

// packedBinaries are the binaries that are packed in the main Gitaly binary. This should always match
// the actual list in <root>/packed_binaries.go so the binaries are correctly located. Git binaries are
// excepted, as they are wired up using a separate mechanism.
//
// Resolving the names automatically from the packed binaries is not possible at the moment due to how
// the packed binaries themselves depend on this config package. If this config package inspected the
// packed binaries, there would be a cyclic dependency. Anything that the packed binaries import must
// not depend on <root>/packed_binaries.go.
var packedBinaries = map[string]struct{}{
	"gitaly-hooks":      {},
	"gitaly-ssh":        {},
	"gitaly-lfs-smudge": {},
	"gitaly-gpg":        {},
}

// BinaryPath returns the path to a given binary. BinaryPath does not do any validation, it simply joins the binaryName
// with the correct base directory depending on whether the binary is a packed binary or not.
func (cfg *Cfg) BinaryPath(binaryName string) string {
	baseDirectory := cfg.BinDir
	if _, ok := packedBinaries[binaryName]; ok {
		baseDirectory = cfg.RuntimeDir
	}

	return filepath.Join(baseDirectory, binaryName)
}

// StoragePath looks up the base path for storageName. The second boolean
// return value indicates if anything was found.
func (cfg *Cfg) StoragePath(storageName string) (string, bool) {
	storage, ok := cfg.Storage(storageName)
	return storage.Path, ok
}

// Storage looks up storageName.
func (cfg *Cfg) Storage(storageName string) (Storage, bool) {
	for _, storage := range cfg.Storages {
		if storage.Name == storageName {
			return storage, true
		}
	}
	return Storage{}, false
}

// InternalSocketDir returns the location of the internal socket directory.
func (cfg *Cfg) InternalSocketDir() string {
	return filepath.Join(cfg.RuntimeDir, "sock.d")
}

// InternalSocketPath is the path to the internal Gitaly socket.
func (cfg *Cfg) InternalSocketPath() string {
	return filepath.Join(cfg.InternalSocketDir(), "intern")
}

func (cfg *Cfg) validateBinDir() error {
	if len(cfg.BinDir) == 0 {
		return fmt.Errorf("bin_dir: is not set")
	}

	if err := validateIsDirectory(cfg.BinDir, "bin_dir"); err != nil {
		return err
	}

	return nil
}

func (cfg *Cfg) validateRuntimeDir() error {
	if cfg.RuntimeDir == "" {
		return nil
	}

	if err := validateIsDirectory(cfg.RuntimeDir, "runtime_dir"); err != nil {
		return err
	}

	return nil
}

func (cfg *Cfg) validateGit() error {
	for _, cfg := range cfg.Git.Config {
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid configuration key %q: %w", cfg.Key, err)
		}
	}

	return nil
}

// defaultMaintenanceWindow specifies a 10 minute job that runs daily at +1200
// GMT time
func defaultMaintenanceWindow(storages []Storage) DailyJob {
	storageNames := make([]string, len(storages))
	for i, s := range storages {
		storageNames[i] = s.Name
	}

	return DailyJob{
		Hour:     12,
		Minute:   0,
		Duration: duration.Duration(10 * time.Minute),
		Storages: storageNames,
	}
}

func (cfg *Cfg) validateMaintenance() error {
	dm := cfg.DailyMaintenance

	sNames := map[string]struct{}{}
	for _, s := range cfg.Storages {
		sNames[s.Name] = struct{}{}
	}
	for _, sName := range dm.Storages {
		if _, ok := sNames[sName]; !ok {
			return fmt.Errorf("daily maintenance specified storage %q does not exist in configuration", sName)
		}
	}

	if dm.Hour > 23 {
		return fmt.Errorf("daily maintenance specified hour '%d' outside range (0-23)", dm.Hour)
	}
	if dm.Minute > 59 {
		return fmt.Errorf("daily maintenance specified minute '%d' outside range (0-59)", dm.Minute)
	}
	if dm.Duration.Duration() > 24*time.Hour {
		return fmt.Errorf("daily maintenance specified duration %s must be less than 24 hours", dm.Duration.Duration())
	}

	return nil
}

func (cfg *Cfg) validateCgroups() error {
	cg := cfg.Cgroups

	if cg.MemoryBytes > 0 && (cg.Repositories.MemoryBytes > cg.MemoryBytes) {
		return errors.New("cgroups.repositories: memory limit cannot exceed parent")
	}

	if cg.CPUShares > 0 && (cg.Repositories.CPUShares > cg.CPUShares) {
		return errors.New("cgroups.repositories: cpu shares cannot exceed parent")
	}

	if cg.CPUQuotaUs > 0 && (cg.Repositories.CPUQuotaUs > cg.CPUQuotaUs) {
		return errors.New("cgroups.repositories: cpu quota cannot exceed parent")
	}

	return nil
}

var (
	errPackObjectsCacheNegativeMaxAge  = errors.New("pack_objects_cache.max_age cannot be negative")
	errPackObjectsCacheNoStorages      = errors.New("pack_objects_cache: cannot pick default cache directory: no storages")
	errPackObjectsCacheRelativePath    = errors.New("pack_objects_cache: storage directory must be absolute path")
	errPackObjectsCacheSetToStorageDir = errors.New("pack_objects_cache: the specified cache directory cannot be the same or a parent of the storage path")
)

func (cfg *Cfg) configurePackObjectsCache() error {
	poc := &cfg.PackObjectsCache
	if !poc.Enabled {
		return nil
	}

	if poc.MaxAge < 0 {
		return errPackObjectsCacheNegativeMaxAge
	}

	if poc.Dir == "" {
		return errPackObjectsCacheNoStorages
	}

	if !filepath.IsAbs(poc.Dir) {
		return errPackObjectsCacheRelativePath
	}

	absCachePath, err := filepath.Abs(cfg.PackObjectsCache.Dir)
	if err != nil {
		return err
	}

	for _, storage := range cfg.Storages {
		absStoragePath, err := filepath.Abs(storage.Path)
		if err != nil {
			return err
		}

		if strings.HasPrefix(absStoragePath, absCachePath) {
			return errPackObjectsCacheSetToStorageDir
		}
	}

	return nil
}

// SetupRuntimeDirectory creates a new runtime directory. Runtime directory contains internal
// runtime data generated by Gitaly such as the internal sockets. If cfg.RuntimeDir is set,
// it's used as the parent directory for the runtime directory. Runtime directory owner process
// can be identified by the suffix process ID suffixed in the directory name. If a directory already
// exists for this process' ID, it's removed and recreated. If cfg.RuntimeDir is not set, a temporary
// directory is used instead. A directory is created for the internal socket as well since it is
// expected to be present in the runtime directory. SetupRuntimeDirectory returns the absolute path
// to the created runtime directory.
func SetupRuntimeDirectory(cfg Cfg, processID int) (string, error) {
	var runtimeDir string
	if cfg.RuntimeDir == "" {
		// If there is no parent directory provided, we just use a temporary directory
		// as the runtime directory. This may not always be an ideal choice given that
		// it's typically created at `/tmp`, which may get periodically pruned if `noatime`
		// is set.
		var err error
		runtimeDir, err = os.MkdirTemp("", "gitaly-")
		if err != nil {
			return "", fmt.Errorf("creating temporary runtime directory: %w", err)
		}
	} else {
		// Otherwise, we use the configured runtime directory. Note that we don't use the
		// runtime directory directly, but instead create a subdirectory within it which is
		// based on the process's PID. While we could use `MkdirTemp()` instead and don't
		// bother with preexisting directories, the benefit of using the PID here is that we
		// can determine whether the directory may still be in use by checking whether the
		// PID exists. Furthermore, it allows easier debugging in case one wants to inspect
		// the runtime directory of a running Gitaly node.

		runtimeDir = GetGitalyProcessTempDir(cfg.RuntimeDir, processID)

		if _, err := os.Stat(runtimeDir); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("statting runtime directory: %w", err)
		} else if err != nil {
			// If the directory exists already then it must be from an old invocation of
			// Gitaly. Because we use the PID as path component we know that the old
			// instance cannot exist anymore though, so it's safe to remove this
			// directory now.
			if err := os.RemoveAll(runtimeDir); err != nil {
				return "", fmt.Errorf("removing old runtime directory: %w", err)
			}
		}

		if err := os.Mkdir(runtimeDir, mode.Directory); err != nil {
			return "", fmt.Errorf("creating runtime directory: %w", err)
		}
	}

	// Set the runtime dir in the config as the internal socket helpers
	// rely on it.
	cfg.RuntimeDir = runtimeDir

	// The socket path must be short-ish because listen(2) fails on long
	// socket paths. We hope/expect that os.MkdirTemp creates a directory
	// that is not too deep. We need a directory, not a tempfile, because we
	// will later want to set its permissions to 0700
	if err := os.Mkdir(cfg.InternalSocketDir(), mode.Directory); err != nil {
		return "", fmt.Errorf("create internal socket directory: %w", err)
	}

	if err := trySocketCreation(cfg.InternalSocketDir()); err != nil {
		return "", fmt.Errorf("failed creating internal test socket: %w", err)
	}

	return runtimeDir, nil
}

func trySocketCreation(dir string) error {
	// To validate the socket can actually be created, we open and close a socket.
	// Any error will be assumed persistent for when the gitaly-ruby sockets are created
	// and thus fatal at boot time.
	//
	// There are two kinds of internal sockets we create: the internal server socket
	// called "intern", and then the Ruby worker sockets called "ruby.$N", with "$N"
	// being the number of the Ruby worker. Given that we typically wouldn't spawn
	// hundreds of Ruby workers, the maximum internal socket path name would thus be 7
	// characters long.
	socketPath := filepath.Join(dir, "tsocket")
	defer func() { _ = os.Remove(socketPath) }()

	// Attempt to create an actual socket and not just a file to catch socket path length problems
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == syscall.EINVAL {
			return fmt.Errorf("%w: your socket path is likely too long, please change Gitaly's runtime directory", errno)
		}

		return fmt.Errorf("socket could not be created in %s: %w", dir, err)
	}

	return l.Close()
}

// Raft contains configuration for the experimental Gitaly Raft cluster.
type Raft struct {
	// Enabled enables the experimental Gitaly Raft cluster.
	Enabled bool `json:"enabled" toml:"enabled"`
	// ClusterID is the unique ID of the cluster. It ensures the current node joins the right cluster.
	ClusterID string `json:"cluster_id" toml:"cluster_id"`
	// RTTMilliseconds is the maximum round trip between two nodes in the cluster. It's used to
	// calculate multiple types of timeouts of Raft protocol.
	RTTMilliseconds uint64 `json:"rtt_milliseconds" toml:"rtt_milliseconds"`
	// ElectionTicks is the minimum number of message RTT between elections.
	ElectionTicks uint64 `json:"election_rtt" toml:"election_rtt"`
	// HeartbeatTicks is the number of message RTT between heartbeats
	HeartbeatTicks uint64 `json:"heartbeat_rtt" toml:"heartbeat_rtt"`
	// SnapshotDir is the directory where raft snapshots are stored.
	SnapshotDir string `json:"snapshot_dir" toml:"snapshot_dir"` // Default: <FIRST STORAGE PATH>/+gitaly/raft/snapshots
}

const (
	// RaftDefaultRTT is the default Round Trip Time (RTT) in milliseconds between two nodes in the Raft
	// cluster. It's used to calculate the election timeout and heartbeat timeout.
	RaftDefaultRTT = 200
	// RaftDefaultElectionTicks is the default election RTT for the Raft cluster. It's the multiplier of
	// RTT between two nodes. The estimated election timeout is DefaultRTT * DefaultElectionTicks.
	RaftDefaultElectionTicks = 20
	// RaftDefaultHeartbeatTicks is the default heartbeat RTT for the Raft cluster. The estimated election
	// timeout is DefaultRTT * DefaultHeartbeatTicks.
	RaftDefaultHeartbeatTicks = 2
)

// DefaultRaftConfig returns a Raft configuration filled with default values.
func DefaultRaftConfig(clusterID string) Raft {
	r := Raft{Enabled: true, ClusterID: clusterID}
	return r.fulfillDefaults()
}

func (r Raft) fulfillDefaults() Raft {
	if r.RTTMilliseconds == 0 {
		r.RTTMilliseconds = RaftDefaultRTT
	}
	if r.ElectionTicks == 0 {
		r.ElectionTicks = RaftDefaultElectionTicks
	}
	if r.HeartbeatTicks == 0 {
		r.HeartbeatTicks = RaftDefaultHeartbeatTicks
	}
	return r
}

// Validate runs validation on all fields and compose all found errors.
func (r Raft) Validate(transactions Transactions) error {
	if !r.Enabled {
		return nil
	}
	cfgErr := cfgerror.New()

	if !transactions.Enabled {
		cfgErr = cfgErr.Append(
			fmt.Errorf("transactions must be enabled to enable Raft"),
			"enabled",
		)
	}

	cfgErr = cfgErr.
		Append(cfgerror.NotEmpty(r.ClusterID), "cluster_id").
		Append(cfgerror.Comparable(r.RTTMilliseconds).GreaterThan(0), "rtt_millisecond").
		Append(cfgerror.Comparable(r.ElectionTicks).GreaterThan(0), "election_rtt").
		Append(cfgerror.Comparable(r.HeartbeatTicks).GreaterThan(0), "heartbeat_rtt").
		Append(cfgerror.PathIsAbs(r.SnapshotDir), "snapshot_dir").
		Append(cfgerror.DirExists(r.SnapshotDir))

	// Validate UUID format of ClusterID
	if r.ClusterID != "" {
		if _, err := uuid.Parse(r.ClusterID); err != nil {
			cfgErr = cfgErr.Append(fmt.Errorf("invalid UUID format for ClusterID: %s", err.Error()), "cluster_id")
		}
	}

	return cfgErr.AsError()
}
