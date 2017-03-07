package writerhttp

import (
	"encoding/json"
	"github.com/function61/eventhorizon/writer"
	"io"
	"net/http"
)

type AppendToStreamRequest struct {
	Stream string
	Lines  []string
}

func AppendToStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Stream": "/foostream", "Lines": [ "line 1" ]}' http://localhost:9092/append
	http.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		var appendToStreamRequest AppendToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&appendToStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.AppendToStream(appendToStreamRequest.Stream, appendToStreamRequest.Lines); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "OK\n")
	})
}
