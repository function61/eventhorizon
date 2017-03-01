package writerhttp

import (
	"encoding/json"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader"
	"github.com/function61/eventhorizon/writer"
	"log"
	"net/http"
	"strconv"
)

type ReadRequest struct {
	Cursor string
}

func HttpServe(eventWriter *writer.EventstoreWriter, shutdown chan bool, done chan bool) {
	reader := reader.NewEventstoreReader()

	srv := &http.Server{Addr: ":" + strconv.Itoa(config.WRITER_HTTP_PORT)}

	// $ curl -d '{"Cursor": "/tenants/foo:0:0"} http://localhost:9092/read
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		var readRequest ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&readRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cur := cursor.CursorFromserializedMust(readRequest.Cursor)
		readResult, err := reader.Read(cur)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			panic(err)
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		encoder.Encode(readResult)
	})

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
