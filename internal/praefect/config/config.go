package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/pelletier/go-toml/v2"
	promclient "github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// ElectionStrategy is a Praefect primary election strategy.
type ElectionStrategy string

// validate validates the election strategy is a valid one.
func (es ElectionStrategy) validate() error {
	switch es {
	case ElectionStrategyLocal, ElectionStrategySQL, ElectionStrategyPerRepository:
		return nil
	default:
		return fmt.Errorf("invalid election strategy: %q", es)
	}
}

const (
	// ElectionStrategyLocal configures a single node, in-memory election strategy.
	ElectionStrategyLocal ElectionStrategy = "local"
	// ElectionStrategySQL configures an SQL based strategy that elects a primary for a virtual storage.
	ElectionStrategySQL ElectionStrategy = "sql"
	// ElectionStrategyPerRepository configures an SQL based strategy that elects different primaries per repository.
	ElectionStrategyPerRepository ElectionStrategy = "per_repository"

	minimalSyncCheckInterval = time.Minute
	minimalSyncRunInterval   = time.Minute
)

// Failover contains configuration for the mechanism that tracks healthiness of the cluster nodes.
type Failover struct {
	// Enabled is a trigger used to check if failover is enabled or not.
	Enabled bool `json:"enabled" toml:"enabled,omitempty"`
	// ElectionStrategy is the strategy to use for electing primaries nodes.
	ElectionStrategy         ElectionStrategy  `json:"election_strategy"           toml:"election_strategy,omitempty"`
	ErrorThresholdWindow     duration.Duration `json:"error_threshold_window"      toml:"error_threshold_window,omitempty"`
	WriteErrorThresholdCount uint32            `json:"write_error_threshold_count" toml:"write_error_threshold_count,omitempty"`
	ReadErrorThresholdCount  uint32            `json:"read_error_threshold_count"  toml:"read_error_threshold_count,omitempty"`
	// BootstrapInterval allows set a time duration that would be used on startup to make initial health check.
	// The default value is 1s.
	BootstrapInterval duration.Duration `json:"bootstrap_interval" toml:"bootstrap_interval,omitempty"`
	// MonitorInterval allows set a time duration that would be used after bootstrap is completed to execute health checks.
	// The default value is 3s.
	MonitorInterval duration.Duration `json:"monitor_interval" toml:"monitor_interval,omitempty"`
}

// ErrorThresholdsConfigured checks whether returns whether the errors thresholds are configured. If they
// are configured but in an invalid way, an error is returned.
func (f Failover) ErrorThresholdsConfigured() (bool, error) {
	if f.ErrorThresholdWindow == 0 && f.WriteErrorThresholdCount == 0 && f.ReadErrorThresholdCount == 0 {
		return false, nil
	}

	if f.ErrorThresholdWindow == 0 {
		return false, errors.New("threshold window not set")
	}

	if f.WriteErrorThresholdCount == 0 {
		return false, errors.New("write error threshold not set")
	}

	if f.ReadErrorThresholdCount == 0 {
		return false, errors.New("read error threshold not set")
	}

	return true, nil
}

// Validate returns a list of failed checks.
func (f Failover) Validate() error {
	if !f.Enabled {
		// If it is not enabled we shouldn't care about provided values
		// as they won't be used.
		return nil
	}

	errs := cfgerror.New().
		Append(cfgerror.IsSupportedValue(f.ElectionStrategy, ElectionStrategyLocal, ElectionStrategySQL, ElectionStrategyPerRepository), "election_strategy").
		Append(cfgerror.Comparable(f.BootstrapInterval.Duration()).GreaterOrEqual(0), "bootstrap_interval").
		Append(cfgerror.Comparable(f.MonitorInterval.Duration()).GreaterOrEqual(0), "monitor_interval").
		Append(cfgerror.Comparable(f.ErrorThresholdWindow.Duration()).GreaterOrEqual(0), "error_threshold_window")

	if f.ErrorThresholdWindow == 0 && f.WriteErrorThresholdCount == 0 && f.ReadErrorThresholdCount == 0 {
		return errs.AsError()
	}

	if f.ErrorThresholdWindow == 0 {
		errs = errs.Append(cfgerror.ErrNotSet, "error_threshold_window")
	}

	if f.WriteErrorThresholdCount == 0 {
		errs = errs.Append(cfgerror.ErrNotSet, "write_error_threshold_count")
	}

	if f.ReadErrorThresholdCount == 0 {
		errs = errs.Append(cfgerror.ErrNotSet, "read_error_threshold_count")
	}

	return errs.AsError()
}

// BackgroundVerification contains configuration options for the repository background verification.
type BackgroundVerification struct {
	// VerificationInterval determines the duration after a replica due for reverification.
	// The feature is disabled if verification interval is 0 or below.
	VerificationInterval duration.Duration `json:"verification_interval" toml:"verification_interval,omitempty"`
	// DeleteInvalidRecords controls whether the background verifier will actually delete the metadata
	// records that point to non-existent replicas.
	DeleteInvalidRecords bool `json:"delete_invalid_records" toml:"delete_invalid_records"`
}

// Validate runs validation on all fields and compose all found errors.
func (bv BackgroundVerification) Validate() error {
	return cfgerror.New().
		Append(cfgerror.Comparable(bv.VerificationInterval.Duration()).GreaterOrEqual(0), "verification_interval").
		AsError()
}

// DefaultBackgroundVerificationConfig returns the default background verification configuration.
func DefaultBackgroundVerificationConfig() BackgroundVerification {
	return BackgroundVerification{
		VerificationInterval: duration.Duration(7 * 24 * time.Hour),
		DeleteInvalidRecords: true,
	}
}

// Reconciliation contains reconciliation specific configuration options.
type Reconciliation struct {
	// SchedulingInterval the interval between each automatic reconciliation run. If set to 0,
	// automatic reconciliation is disabled.
	SchedulingInterval duration.Duration `json:"scheduling_interval" toml:"scheduling_interval,omitempty"`
	// HistogramBuckets configures the reconciliation scheduling duration histogram's buckets.
	HistogramBuckets []float64 `json:"histogram_buckets" toml:"histogram_buckets,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (r Reconciliation) Validate() error {
	errs := cfgerror.New().
		Append(cfgerror.Comparable(r.SchedulingInterval.Duration()).GreaterOrEqual(0), "scheduling_interval")

	if r.SchedulingInterval != 0 {
		if !sort.Float64sAreSorted(r.HistogramBuckets) {
			errs = errs.Append(cfgerror.ErrBadOrder, "histogram_buckets")
		}
	}

	return errs.AsError()
}

// DefaultReconciliationConfig returns the default values for reconciliation configuration.
func DefaultReconciliationConfig() Reconciliation {
	return Reconciliation{
		SchedulingInterval: 5 * duration.Duration(time.Minute),
		HistogramBuckets:   promclient.DefBuckets,
	}
}

// Replication contains replication specific configuration options.
type Replication struct {
	// BatchSize controls how many replication jobs to dequeue and lock
	// in a single call to the database.
	BatchSize uint `json:"batch_size" toml:"batch_size,omitempty"`
	// ParallelStorageProcessingWorkers is a number of workers used to process replication
	// events per virtual storage (how many storages would be processed in parallel).
	ParallelStorageProcessingWorkers uint `json:"parallel_storage_processing_workers" toml:"parallel_storage_processing_workers,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (r Replication) Validate() error {
	return cfgerror.New().
		Append(cfgerror.Comparable(r.BatchSize).GreaterOrEqual(1), "batch_size").
		Append(cfgerror.Comparable(r.ParallelStorageProcessingWorkers).GreaterOrEqual(1), "parallel_storage_processing_workers").
		AsError()
}

// DefaultReplicationConfig returns the default values for replication configuration.
func DefaultReplicationConfig() Replication {
	return Replication{BatchSize: 10, ParallelStorageProcessingWorkers: 1}
}

// Config is a container for everything found in the TOML config file
type Config struct {
	// ConfigCommand specifies the path to an executable that Praefect will run after loading the initial
	// configuration. The executable is expected to write JSON-formatted configuration to its standard
	// output that we will then deserialize and merge back into the initially-loaded configuration again.
	// This is an easy mechanism to generate parts of the configuration at runtime, like for example secrets.
	ConfigCommand          string                 `json:"config_command"                                                                       toml:"config_command,omitempty"`
	AllowLegacyElectors    bool                   `json:"i_understand_my_election_strategy_is_unsupported_and_will_be_removed_without_warning" toml:"i_understand_my_election_strategy_is_unsupported_and_will_be_removed_without_warning,omitempty"`
	BackgroundVerification BackgroundVerification `json:"background_verification"                                                              toml:"background_verification,omitempty"`
	Reconciliation         Reconciliation         `json:"reconciliation"                                                                       toml:"reconciliation,omitempty"`
	Replication            Replication            `json:"replication"                                                                          toml:"replication,omitempty"`
	ListenAddr             string                 `json:"listen_addr"                                                                          toml:"listen_addr,omitempty"`
	TLSListenAddr          string                 `json:"tls_listen_addr"                                                                      toml:"tls_listen_addr,omitempty"`
	SocketPath             string                 `json:"socket_path"                                                                          toml:"socket_path,omitempty"`
	VirtualStorages        []*VirtualStorage      `json:"virtual_storage"                                                                      toml:"virtual_storage,omitempty"`
	Logging                log.Config             `json:"logging"                                                                              toml:"logging,omitempty"`
	Sentry                 sentry.Config          `json:"sentry"                                                                               toml:"sentry,omitempty"`
	PrometheusListenAddr   string                 `json:"prometheus_listen_addr"                                                               toml:"prometheus_listen_addr,omitempty"`
	Prometheus             prometheus.Config      `json:"prometheus"                                                                           toml:"prometheus,omitempty"`
	Auth                   auth.Config            `json:"auth"                                                                                 toml:"auth,omitempty"`
	TLS                    config.TLS             `json:"tls"                                                                                  toml:"tls,omitempty"`
	DB                     `json:"database"                                                                             toml:"database,omitempty"`
	Failover               Failover            `json:"failover"                                                                             toml:"failover,omitempty"`
	MemoryQueueEnabled     bool                `json:"memory_queue_enabled"                                                                 toml:"memory_queue_enabled,omitempty"`
	GracefulStopTimeout    duration.Duration   `json:"graceful_stop_timeout"                                                                toml:"graceful_stop_timeout,omitempty"`
	RepositoriesCleanup    RepositoriesCleanup `json:"repositories_cleanup"                                                                 toml:"repositories_cleanup,omitempty"`
	Yamux                  Yamux               `json:"yamux"                                                                                toml:"yamux,omitempty"`
}

// Yamux contains Yamux related configuration values.
type Yamux struct {
	// MaximumStreamWindowSizeBytes sets the maximum window size in bytes used for yamux streams.
	// Higher value can increase throughput at the cost of more memory usage.
	MaximumStreamWindowSizeBytes uint32 `json:"maximum_stream_window_size_bytes" toml:"maximum_stream_window_size_bytes,omitempty"`
	// AcceptBacklog sets the maximum number of stream openings in-flight before further openings
	// block.
	AcceptBacklog uint `json:"accept_backlog" toml:"accept_backlog,omitempty"`
}

func (cfg Yamux) validate() error {
	if cfg.MaximumStreamWindowSizeBytes < 262144 {
		// Yamux requires the stream window size to be at minimum 256KiB.
		return fmt.Errorf("yamux.maximum_stream_window_size_bytes must be at least 262144 but it was %d", cfg.MaximumStreamWindowSizeBytes)
	}

	if cfg.AcceptBacklog < 1 {
		// Yamux requires accept backlog to be at least 1
		return fmt.Errorf("yamux.accept_backlog must be at least 1 but it was %d", cfg.AcceptBacklog)
	}

	return nil
}

// Validate runs validation on all fields and compose all found errors.
func (cfg Yamux) Validate() error {
	return cfgerror.New().
		Append(cfgerror.Comparable(cfg.MaximumStreamWindowSizeBytes).GreaterOrEqual(262144), "maximum_stream_window_size_bytes").
		Append(cfgerror.Comparable(cfg.AcceptBacklog).GreaterOrEqual(1), "accept_backlog").
		AsError()
}

// DefaultYamuxConfig returns the default Yamux configuration values.
func DefaultYamuxConfig() Yamux {
	defaultCfg := yamux.DefaultConfig()
	return Yamux{
		MaximumStreamWindowSizeBytes: defaultCfg.MaxStreamWindowSize,
		AcceptBacklog:                uint(defaultCfg.AcceptBacklog),
	}
}

// DefaultLoggingConfig returns the default logging configuration.
func DefaultLoggingConfig() log.Config {
	return log.Config{
		Format: "text",
		Level:  "info",
	}
}

// VirtualStorage represents a set of nodes for a storage
type VirtualStorage struct {
	Name  string  `json:"name" toml:"name,omitempty"`
	Nodes []*Node `json:"node" toml:"node,omitempty"`
	// DefaultReplicationFactor is the replication factor set for new repositories.
	// A valid value is inclusive between 1 and the number of configured storages in the
	// virtual storage. Setting the value to 0 or below causes Praefect to not store any
	// host assignments, falling back to the behavior of replicating to every configured
	// storage
	DefaultReplicationFactor int `json:"default_replication_factor" toml:"default_replication_factor,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (vs VirtualStorage) Validate() error {
	errs := cfgerror.New().
		Append(cfgerror.NotBlank(vs.Name), "name").
		Append(cfgerror.NotEmptySlice(vs.Nodes), "node")

	for i, node := range vs.Nodes {
		errs = errs.Append(node.Validate(), "node", fmt.Sprintf("[%d]", i))
	}

	return errs.AsError()
}

// FromFile loads the config for the passed file path
func FromFile(filePath string) (Config, error) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return Config{}, err
	}

	return FromReader(bytes.NewReader(b))
}

// FromReader loads the config for the passed data stream.
func FromReader(reader io.Reader) (Config, error) {
	conf := &Config{
		BackgroundVerification: DefaultBackgroundVerificationConfig(),
		Reconciliation:         DefaultReconciliationConfig(),
		Replication:            DefaultReplicationConfig(),
		Prometheus:             prometheus.DefaultConfig(),
		// Sets the default Failover, to be overwritten when deserializing the TOML
		Failover:            Failover{Enabled: true, ElectionStrategy: ElectionStrategyPerRepository},
		RepositoriesCleanup: DefaultRepositoriesCleanup(),
		Yamux:               DefaultYamuxConfig(),
		Logging:             DefaultLoggingConfig(),
		TLS:                 config.NewTLS(),
	}
	if err := toml.NewDecoder(reader).Decode(conf); err != nil {
		return Config{}, err
	}

	if conf.ConfigCommand != "" {
		output, err := exec.Command(conf.ConfigCommand).Output()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				return Config{}, fmt.Errorf("running config command: %w, stderr: %q", err, string(exitErr.Stderr))
			}

			return Config{}, fmt.Errorf("running config command: %w", err)
		}

		if err := json.Unmarshal(output, &conf); err != nil {
			return Config{}, fmt.Errorf("unmarshalling generated config: %w", err)
		}
	}

	conf.setDefaults()

	return *conf, nil
}

var (
	errDuplicateStorage         = errors.New("internal gitaly storages are not unique")
	errGitalyWithoutAddr        = errors.New("all gitaly nodes must have an address")
	errGitalyWithoutStorage     = errors.New("all gitaly nodes must have a storage")
	errNoGitalyServers          = errors.New("no primary gitaly backends configured")
	errNoListener               = errors.New("no listen address or socket path configured")
	errNoVirtualStorages        = errors.New("no virtual storages configured")
	errVirtualStoragesNotUnique = errors.New("virtual storages must have unique names")
	errVirtualStorageUnnamed    = errors.New("virtual storages must have a name")
)

// Validate establishes if the config is valid
func (c *Config) Validate() error {
	if err := c.Failover.ElectionStrategy.validate(); err != nil {
		return err
	}

	if c.ListenAddr == "" && c.SocketPath == "" && c.TLSListenAddr == "" {
		return errNoListener
	}

	if len(c.VirtualStorages) == 0 {
		return errNoVirtualStorages
	}

	if c.Replication.BatchSize < 1 {
		return fmt.Errorf("replication batch size was %d but must be >=1", c.Replication.BatchSize)
	}

	virtualStorages := make(map[string]struct{}, len(c.VirtualStorages))

	for _, virtualStorage := range c.VirtualStorages {
		if virtualStorage.Name == "" {
			return errVirtualStorageUnnamed
		}

		if len(virtualStorage.Nodes) == 0 {
			return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errNoGitalyServers)
		}

		if _, ok := virtualStorages[virtualStorage.Name]; ok {
			return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errVirtualStoragesNotUnique)
		}
		virtualStorages[virtualStorage.Name] = struct{}{}

		storages := make(map[string]struct{}, len(virtualStorage.Nodes))
		for _, node := range virtualStorage.Nodes {
			if node.Storage == "" {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errGitalyWithoutStorage)
			}

			if node.Address == "" {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errGitalyWithoutAddr)
			}

			if _, found := storages[node.Storage]; found {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errDuplicateStorage)
			}
			storages[node.Storage] = struct{}{}
		}

		if virtualStorage.DefaultReplicationFactor > len(virtualStorage.Nodes) {
			return fmt.Errorf(
				"virtual storage %q has a default replication factor (%d) which is higher than the number of storages (%d)",
				virtualStorage.Name, virtualStorage.DefaultReplicationFactor, len(virtualStorage.Nodes),
			)
		}
	}

	if c.RepositoriesCleanup.RunInterval.Duration() > 0 {
		if c.RepositoriesCleanup.CheckInterval.Duration() < minimalSyncCheckInterval {
			return fmt.Errorf("repositories_cleanup.check_interval is less then %s, which could lead to a database performance problem", minimalSyncCheckInterval.String())
		}
		if c.RepositoriesCleanup.RunInterval.Duration() < minimalSyncRunInterval {
			return fmt.Errorf("repositories_cleanup.run_interval is less then %s, which could lead to a database performance problem", minimalSyncRunInterval.String())
		}
	}

	if err := c.Yamux.validate(); err != nil {
		return err
	}

	return nil
}

// ValidateV2 is a new validation method that is a replacement for the existing Validate.
// It exists as a demonstration of the new validation implementation based on the usage
// of the cfgerror package.
func (c *Config) ValidateV2() error {
	errs := cfgerror.New().
		Append(func() error {
			if c.SocketPath == "" && c.ListenAddr == "" && c.TLSListenAddr == "" {
				return fmt.Errorf(`none of "socket_path", "listen_addr" or "tls_listen_addr" is set`)
			}
			return nil
		}()).
		Append(c.BackgroundVerification.Validate(), "background_verification").
		Append(c.Reconciliation.Validate(), "reconciliation").
		Append(c.Replication.Validate(), "replication").
		Append(c.Prometheus.Validate(), "prometheus").
		Append(c.TLS.Validate(), "tls").
		Append(c.Failover.Validate(), "failover").
		Append(cfgerror.Comparable(c.GracefulStopTimeout.Duration()).GreaterOrEqual(0), "graceful_stop_timeout").
		Append(c.RepositoriesCleanup.Validate(), "repositories_cleanup").
		Append(c.Yamux.Validate(), "yamux").
		Append(cfgerror.NotEmptySlice(c.VirtualStorages), "virtual_storage")

	for i, storage := range c.VirtualStorages {
		errs = errs.Append(storage.Validate(), "virtual_storage", fmt.Sprintf("[%d]", i))
	}

	return errs.AsError()
}

// NeedsSQL returns true if the driver for SQL needs to be initialized
func (c *Config) NeedsSQL() bool {
	return !c.MemoryQueueEnabled || (c.Failover.Enabled && c.Failover.ElectionStrategy != ElectionStrategyLocal)
}

func (c *Config) setDefaults() {
	if c.GracefulStopTimeout.Duration() == 0 {
		c.GracefulStopTimeout = duration.Duration(time.Minute)
	}

	if c.Failover.Enabled {
		if c.Failover.BootstrapInterval.Duration() == 0 {
			c.Failover.BootstrapInterval = duration.Duration(time.Second)
		}

		if c.Failover.MonitorInterval.Duration() == 0 {
			c.Failover.MonitorInterval = duration.Duration(3 * time.Second)
		}
	}
}

// VirtualStorageNames returns names of all virtual storages configured.
func (c *Config) VirtualStorageNames() []string {
	names := make([]string, len(c.VirtualStorages))
	for i, virtual := range c.VirtualStorages {
		names[i] = virtual.Name
	}
	return names
}

// StorageNames returns storage names by virtual storage.
func (c *Config) StorageNames() map[string][]string {
	storages := make(map[string][]string, len(c.VirtualStorages))
	for _, vs := range c.VirtualStorages {
		nodes := make([]string, len(vs.Nodes))
		for i, n := range vs.Nodes {
			nodes[i] = n.Storage
		}

		storages[vs.Name] = nodes
	}

	return storages
}

// DefaultReplicationFactors returns a map with the default replication factors of
// the virtual storages.
func (c Config) DefaultReplicationFactors() map[string]int {
	replicationFactors := make(map[string]int, len(c.VirtualStorages))
	for _, vs := range c.VirtualStorages {
		replicationFactors[vs.Name] = vs.DefaultReplicationFactor
	}

	return replicationFactors
}

// DBConnection holds Postgres client configuration data.
type DBConnection struct {
	Host        string `json:"host"        toml:"host,omitempty"`
	Port        int    `json:"port"        toml:"port,omitempty"`
	User        string `json:"user"        toml:"user,omitempty"`
	Password    string `json:"password"    toml:"password,omitempty"`
	DBName      string `json:"dbname"      toml:"dbname,omitempty"`
	SSLMode     string `json:"sslmode"     toml:"sslmode,omitempty"`
	SSLCert     string `json:"sslcert"     toml:"sslcert,omitempty"`
	SSLKey      string `json:"sslkey"      toml:"sslkey,omitempty"`
	SSLRootCert string `json:"sslrootcert" toml:"sslrootcert,omitempty"`
}

// DB holds database configuration data.
type DB struct {
	Host        string `json:"host"        toml:"host,omitempty"`
	Port        int    `json:"port"        toml:"port,omitempty"`
	User        string `json:"user"        toml:"user,omitempty"`
	Password    string `json:"password"    toml:"password,omitempty"`
	DBName      string `json:"dbname"      toml:"dbname,omitempty"`
	SSLMode     string `json:"sslmode"     toml:"sslmode,omitempty"`
	SSLCert     string `json:"sslcert"     toml:"sslcert,omitempty"`
	SSLKey      string `json:"sslkey"      toml:"sslkey,omitempty"`
	SSLRootCert string `json:"sslrootcert" toml:"sslrootcert,omitempty"`

	SessionPooled DBConnection `json:"session_pooled" toml:"session_pooled,omitempty"`

	// The following configuration keys are deprecated and
	// will be removed. Use Host and Port attributes of
	// SessionPooled instead.
	HostNoProxy string `json:"host_no_proxy" toml:"host_no_proxy,omitempty"`
	PortNoProxy int    `json:"port_no_proxy" toml:"port_no_proxy,omitempty"`
}

// RepositoriesCleanup configures repository synchronisation.
type RepositoriesCleanup struct {
	// CheckInterval is a time period used to check if operation should be executed.
	// It is recommended to keep it less than run_interval configuration as some
	// nodes may be out of service, so they can be stale for too long.
	CheckInterval duration.Duration `json:"check_interval" toml:"check_interval,omitempty"`
	// RunInterval: the check runs if the previous operation was done at least RunInterval before.
	RunInterval duration.Duration `json:"run_interval" toml:"run_interval,omitempty"`
	// RepositoriesInBatch is the number of repositories to pass as a batch for processing.
	RepositoriesInBatch uint `json:"repositories_in_batch" toml:"repositories_in_batch,omitempty"`
}

// Validate runs validation on all fields and compose all found errors.
func (rc RepositoriesCleanup) Validate() error {
	if rc.RunInterval == 0 {
		// If RunInterval is set to 0 it means it is disabled. The validation doesn't make
		// sense then as it won't be used.
		return nil
	}

	return cfgerror.New().
		Append(cfgerror.Comparable(rc.CheckInterval.Duration()).GreaterOrEqual(minimalSyncCheckInterval), "check_interval").
		Append(cfgerror.Comparable(rc.RunInterval.Duration()).GreaterOrEqual(minimalSyncRunInterval), "run_interval").
		Append(cfgerror.Comparable(rc.RepositoriesInBatch).GreaterOrEqual(1), "repositories_in_batch").
		AsError()
}

// DefaultRepositoriesCleanup contains default configuration values for the RepositoriesCleanup.
func DefaultRepositoriesCleanup() RepositoriesCleanup {
	return RepositoriesCleanup{
		CheckInterval:       duration.Duration(30 * time.Minute),
		RunInterval:         duration.Duration(24 * time.Hour),
		RepositoriesInBatch: 16,
	}
}
