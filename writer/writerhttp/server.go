package writerhttp

import (
	"context"
	"crypto/tls"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/writer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

func HttpServe(eventWriter *writer.EventstoreWriter, shutdown chan bool, done chan bool, confCtx *config.Context) {
	writerSrv := &http.Server{Addr: ":" + strconv.Itoa(config.WriterHttpPort)}
	metricsSrv := &http.Server{Addr: ":9094"}

	ReadHandlerInit(eventWriter)
	CreateStreamHandlerInit(eventWriter)
	AppendToStreamHandlerInit(eventWriter)
	SubscribeToStreamHandlerInit(eventWriter)
	UnsubscribeFromStreamHandlerInit(eventWriter)

	go func() {
		log.Printf("WriterHttp: binding to %s", writerSrv.Addr)

		writerSrv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{confCtx.GetSignedServerCertificate()},
		}

		if err := writerSrv.ListenAndServeTLS("", ""); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("WriterHttp: ListenAndServe() error: %s", err)
		}
	}()

	go func() {
		promHandler := promhttp.Handler()

		metricsSrv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/metrics" {
				http.Error(w, "We only have /metrics path", http.StatusNotFound)
				return
			}

			promHandler.ServeHTTP(w, r)
		})

		if err := metricsSrv.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("metricsSrv: ListenAndServe() error: %s", err)
		}
	}()

	go func() {
		<-shutdown

		log.Printf("WriterHttp: shutting down")

		if err := writerSrv.Shutdown(context.TODO()); err != nil {
			panic(err) // failed shutting down
		}

		if err := metricsSrv.Shutdown(context.TODO()); err != nil {
			panic(err) // failed shutting down
		}

		log.Printf("WriterHttp: shutting down done")

		done <- true
	}()
}
