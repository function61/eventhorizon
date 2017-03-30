package writerhttp

import (
	"github.com/function61/eventhorizon/writer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func MetricsHandlerInit(eventWriter *writer.EventstoreWriter) {
	// FIXME: No authentication for metrics. These probably aren't top secret.
	//        Does Prometheus support auth, and is it a known practice?
	//        Serve /metrics from different, an internal, port?

	// $ curl http://localhost:9092/metrics
	http.Handle("/metrics", promhttp.Handler())
}
