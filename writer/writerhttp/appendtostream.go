package writerhttp

import (
	"encoding/json"
	"github.com/function61/pyramid/writer"
	"github.com/function61/pyramid/writer/authmiddleware"
	wtypes "github.com/function61/pyramid/writer/types"
	"net/http"
)

func AppendToStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	// $ curl -d '{"Stream": "/foostream", "Lines": [ "line 1" ]}' http://localhost:9092/append
	http.Handle("/append", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var appendToStreamRequest wtypes.AppendToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&appendToStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := eventWriter.AppendToStream(appendToStreamRequest.Stream, appendToStreamRequest.Lines)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(output)
	}), ctx))
}
