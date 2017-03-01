package writerhttp

import (
	"encoding/json"
	"github.com/function61/eventhorizon/writer"
	"io"
	"net/http"
)

type CreateStreamRequest struct {
	Name string
}

func CreateStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Name": "/foostream"}' http://localhost:9092/create_stream
	http.HandleFunc("/create_stream", func(w http.ResponseWriter, r *http.Request) {
		var createStreamRequest CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&createStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.CreateStream(createStreamRequest.Name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		io.WriteString(w, "OK\n")
	})
}
