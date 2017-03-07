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
}

func NewMetrics() *Metrics {
	m := &Metrics{}

	m.CreateStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "create_stream_ops",
		Help: "Number of CreateStream() operations",
	})
	prometheus.MustRegister(m.CreateStreamOps)

	m.SubscribeToStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "subscribe_to_stream_ops",
		Help: "Number of SubscribeToStream() operations",
	})
	prometheus.MustRegister(m.SubscribeToStreamOps)

	m.UnsubscribeFromStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "unsubscribe_from_stream_ops",
		Help: "Number of UnsubscribeFromStream() operations",
	})
	prometheus.MustRegister(m.UnsubscribeFromStreamOps)

	m.AppendToStreamOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "append_to_stream_ops",
		Help: "Number of AppendToStream() operations",
	})
	prometheus.MustRegister(m.AppendToStreamOps)

	m.AppendedLinesExclMeta = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "appended_lines_count_excl_meta",
		Help: "Number of appended lines across all streams (excludes meta lines)",
	})
	prometheus.MustRegister(m.AppendedLinesExclMeta)

	m.ChunkShippedToLongTermStorage = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "chunks_shipped_to_long_term_storage",
		Help: "Number of chunks shipped to long term storage",
	})
	prometheus.MustRegister(m.ChunkShippedToLongTermStorage)

	m.LiveReaderReadOps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "live_reader_read_ops",
		Help: "Number of Read() operations for live reader",
	})
	prometheus.MustRegister(m.LiveReaderReadOps)

	m.SubscriptionActivityEventsRaised = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "subscription_activity_events_raised",
		Help: "Number of events raised by activity on streams that have been subscribed to",
	})
	prometheus.MustRegister(m.SubscriptionActivityEventsRaised)

	return m
}
