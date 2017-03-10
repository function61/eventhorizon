package writerhttp

import (
	"encoding/json"
	"github.com/function61/pyramid/writer"
	"github.com/function61/pyramid/writer/authmiddleware"
	"github.com/function61/pyramid/writer/writerhttp/types"
	"io"
	"net/http"
)

func AppendToStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Stream": "/foostream", "Lines": [ "line 1" ]}' http://localhost:9092/append
	http.Handle("/append", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var appendToStreamRequest types.AppendToStreamRequest
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
	})))
}
