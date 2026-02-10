package backchannel

import (
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	yamuxWriteSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "gitaly",
		Subsystem: "backchannel",
		Name:      "yamux_write_size_bytes",
		Help:      "Size of individual writes to yamux connections",
		Buckets:   []float64{1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576},
	})

	yamuxReadSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "gitaly",
		Subsystem: "backchannel",
		Name:      "yamux_read_size_bytes",
		Help:      "Size of individual reads from yamux connections",
		Buckets:   []float64{1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576},
	})

	registerOnce sync.Once
)

func init() {
	registerOnce.Do(func() {
		prometheus.MustRegister(yamuxWriteSizeHistogram)
		prometheus.MustRegister(yamuxReadSizeHistogram)
	})
}

type instrumentedConn struct {
	net.Conn
}

func newInstrumentedConn(conn net.Conn) net.Conn {
	return &instrumentedConn{Conn: conn}
}

func (c *instrumentedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		yamuxWriteSizeHistogram.Observe(float64(n))
	}
	return n, err
}

func (c *instrumentedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		yamuxReadSizeHistogram.Observe(float64(n))
	}
	return n, err
}
