package writer

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	CreateStreamOps                  prometheus.Counter
	SubscribeToStreamOps             prometheus.Counter
	UnsubscribeFromStreamOps         prometheus.Counter
	AppendToStreamOps                prometheus.Counter
	AppendedLinesExclMeta            prometheus.Counter
	ChunkShippedToLongTermStorage    prometheus.Counter
	LiveReaderReadOps                prometheus.Counter
	SubscriptionActivityEventsRaised prometheus.Counter

	// so we can unregister all on close without
	// explicitly mentioning each counter
	allCollectors []prometheus.Collector
}

func NewMetrics() *Metrics {
	m := &Metrics{}

	m.CreateStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "create_stream_ops",
		Help: "Number of CreateStream() operations",
	})
	m.register(m.CreateStreamOps)

	m.SubscribeToStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "subscribe_to_stream_ops",
		Help: "Number of SubscribeToStream() operations",
	})
	m.register(m.SubscribeToStreamOps)

	m.UnsubscribeFromStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "unsubscribe_from_stream_ops",
		Help: "Number of UnsubscribeFromStream() operations",
	})
	m.register(m.UnsubscribeFromStreamOps)

	m.AppendToStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "append_to_stream_ops",
		Help: "Number of AppendToStream() operations",
	})
	m.register(m.AppendToStreamOps)

	m.AppendedLinesExclMeta = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "appended_lines_count_excl_meta",
		Help: "Number of appended lines across all streams (excludes meta lines)",
	})
	m.register(m.AppendedLinesExclMeta)

	m.ChunkShippedToLongTermStorage = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "chunks_shipped_to_long_term_storage",
		Help: "Number of chunks shipped to long term storage",
	})
	m.register(m.ChunkShippedToLongTermStorage)

	m.LiveReaderReadOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "live_reader_read_ops",
		Help: "Number of Read() operations for live reader",
	})
	m.register(m.LiveReaderReadOps)

	m.SubscriptionActivityEventsRaised = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "subscription_activity_events_raised",
		Help: "Number of events raised by activity on streams that have been subscribed to",
	})
	m.register(m.SubscriptionActivityEventsRaised)

	return m
}

func (m *Metrics) register(collector prometheus.Collector) {
	prometheus.MustRegister(collector)
	m.allCollectors = append(m.allCollectors, collector)
}

// this is to support instantiating Writer process more than one time per process.
// otherwise we'd get "duplicate metrics collector registration attempted" due
// to Prom's global state, blerch..
func (m *Metrics) Close() {
	for _, collector := range m.allCollectors {
		prometheus.Unregister(collector)
	}
}
