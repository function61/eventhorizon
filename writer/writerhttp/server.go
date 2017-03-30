package writerhttp

import (
	"crypto/tls"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/writer"
	"log"
	"net/http"
	"strconv"
)

func HttpServe(eventWriter *writer.EventstoreWriter, shutdown chan bool, done chan bool, confCtx *config.Context) {
	srv := &http.Server{Addr: ":" + strconv.Itoa(config.WriterHttpPort)}

	MetricsHandlerInit(eventWriter)
	ReadHandlerInit(eventWriter)
	CreateStreamHandlerInit(eventWriter)
	AppendToStreamHandlerInit(eventWriter)
	SubscribeToStreamHandlerInit(eventWriter)
	UnsubscribeFromStreamHandlerInit(eventWriter)

	go func() {
		log.Printf("WriterHttp: binding to %s", srv.Addr)

		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{confCtx.GetSignedServerCertificate()},
		}

		if err := srv.ListenAndServeTLS("", ""); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("WriterHttp: ListenAndServe() error: %s", err)
		}
	}()

	go func() {
		<-shutdown

		log.Printf("WriterHttp: shutting down")

		if err := srv.Shutdown(nil); err != nil {
			panic(err) // failed shutting down
		}

		log.Printf("WriterHttp: shutting down done")

		done <- true
	}()
}
