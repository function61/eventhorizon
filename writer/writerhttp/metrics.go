package writerhttp

import (
	"github.com/function61/eventhorizon/writer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func MetricsHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl http://localhost:9092/metrics
	http.Handle("/metrics", promhttp.Handler())
}
