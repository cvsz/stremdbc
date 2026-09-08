package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics holds Prometheus metrics
type Metrics struct {
	registry        *prometheus.Registry
	streamsTotal    prometheus.Counter
	streamsLive     prometheus.Gauge
	viewersTotal    prometheus.Gauge
	bytesReceived   prometheus.Counter
	bytesSent       prometheus.Counter
	connectionTotal prometheus.Counter
	errorsTotal     prometheus.Counter
}

// NewMetrics creates a new Metrics instance
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	// Register standard Go metrics
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: registry,
		streamsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "stremdbc",
			Subsystem: "streams",
			Name:      "total",
			Help:      "Total number of streams created",
		}),
		streamsLive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "stremdbc",
			Subsystem: "streams",
			Name:      "live",
			Help:      "Number of currently live streams",
		}),
		viewersTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "stremdbc",
			Subsystem: "viewers",
			Name:      "total",
			Help:      "Total number of viewers across all streams",
		}),
		bytesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "stremdbc",
			Subsystem: "network",
			Name:      "received_bytes_total",
			Help:      "Total bytes received",
		}),
		bytesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "stremdbc",
			Subsystem: "network",
			Name:      "sent_bytes_total",
			Help:      "Total bytes sent",
		}),
		connectionTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "stremdbc",
			Subsystem: "connections",
			Name:      "total",
			Help:      "Total number of connections",
		}),
		errorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "stremdbc",
			Subsystem: "errors",
			Name:      "total",
			Help:      "Total number of errors",
		}),
	}

	// Register custom metrics
	registry.MustRegister(m.streamsTotal)
	registry.MustRegister(m.streamsLive)
	registry.MustRegister(m.viewersTotal)
	registry.MustRegister(m.bytesReceived)
	registry.MustRegister(m.bytesSent)
	registry.MustRegister(m.connectionTotal)
	registry.MustRegister(m.errorsTotal)

	return m
}

// Handler returns the Prometheus HTTP handler
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordStreamCreated records a stream creation
func (m *Metrics) RecordStreamCreated() {
	m.streamsTotal.Inc()
}

// RecordStreamLive records a stream going live
func (m *Metrics) RecordStreamLive() {
	m.streamsLive.Inc()
}

// RecordStreamOffline records a stream going offline
func (m *Metrics) RecordStreamOffline() {
	m.streamsLive.Dec()
}

// RecordViewers records the total viewer count
func (m *Metrics) RecordViewers(count int) {
	m.viewersTotal.Set(float64(count))
}

// RecordBytesReceived records bytes received
func (m *Metrics) RecordBytesReceived(bytes int64) {
	m.bytesReceived.Add(float64(bytes))
}

// RecordBytesSent records bytes sent
func (m *Metrics) RecordBytesSent(bytes int64) {
	m.bytesSent.Add(float64(bytes))
}

// RecordConnection records a connection
func (m *Metrics) RecordConnection() {
	m.connectionTotal.Inc()
}

// RecordError records an error
func (m *Metrics) RecordError() {
	m.errorsTotal.Inc()
}
