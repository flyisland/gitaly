package streamcache

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
)

var cacheIndexSizeGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gitaly_streamcache_index_entries",
		Help: "Number of index entries in streamcache",
	},
	[]string{"dir", "cache_name"},
)

type streamCacheMetrics struct {
	name           string
	cfg            config.StreamCacheConfig
	enabled        *prometheus.GaugeVec
	cacheIndexSize prometheus.Gauge
}

// newStreamCacheMetrics returns a new streamCacheMetrics
func newStreamCacheMetrics(cfg config.StreamCacheConfig) *streamCacheMetrics {
	// enabled is a metric of type Gauge that indicates if the cache is enabled (1) or not (0).
	// Previously, this metric was a global variable and its name was hardcoded as:
	// * `gitaly_<pack_objects>_cache_enabled`
	//
	// This situation became problematic what we wanted to reuse the streamcache for other purposes than caching
	// packfiles. So now, the name if formatted with a newly added cache parameter such as its name.
	// This allows to create multiple instances of a streamcache.
	enabledGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: fmt.Sprintf("gitaly_%s_cache_enabled", cfg.Name),
			Help: fmt.Sprintf("If set to 1, indicates that the cache for %s has been enabled in this process", cfg.Name),
		},
		[]string{"dir", "max_age"},
	)

	// During tests, it's possible that multiple instances of the cache exist at the same time with
	// the same name, hence causing the same metric to be registered twice.
	// To avoid that case in the tests, we handle the error manually.
	if err := prometheus.Register(enabledGauge); err != nil {
		if !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
			panic(err)
		}
	}

	return &streamCacheMetrics{
		name:           cfg.Name,
		cfg:            cfg,
		enabled:        enabledGauge,
		cacheIndexSize: cacheIndexSizeGauge.WithLabelValues(cfg.Dir, cfg.Name),
	}
}

// setEnabled set the metrics `enabled` to 1, which is treated as a `true`.
func (m *streamCacheMetrics) setEnabled() {
	if m.enabled == nil {
		return
	}
	m.enabled.
		WithLabelValues(m.cfg.Dir, strconv.Itoa(int(m.cfg.MaxAge.Duration().Seconds()))).
		Set(1)
}

// setIndexSize sets the index size of the cache.
func (m *streamCacheMetrics) setIndexSize(size float64) {
	if m.cacheIndexSize == nil {
		return
	}
	m.cacheIndexSize.Set(size)
}
