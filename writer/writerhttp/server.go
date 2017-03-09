package writerhttp

import (
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/writer"
	"log"
	"net/http"
	"strconv"
)

func HttpServe(eventWriter *writer.EventstoreWriter, shutdown chan bool, done chan bool) {
	srv := &http.Server{Addr: ":" + strconv.Itoa(config.WRITER_HTTP_PORT)}

	MetricsHandlerInit(eventWriter)
	ReadHandlerInit(eventWriter)
	CreateStreamHandlerInit(eventWriter)
	AppendToStreamHandlerInit(eventWriter)
	SubscribeToStreamHandlerInit(eventWriter)
	UnsubscribeFromStreamHandlerInit(eventWriter)

	go func() {
		log.Printf("WriterHttp: binding to %s", srv.Addr)

		if err := srv.ListenAndServe(); err != nil {
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
