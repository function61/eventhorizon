package writerhttp

import (
	"encoding/json"
	"net/http"

	"github.com/function61/eventhorizon/pkg/legacy/writer"
	"github.com/function61/eventhorizon/pkg/legacy/writer/authmiddleware"
	wtypes "github.com/function61/eventhorizon/pkg/legacy/writer/types"
)

func AppendToStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	http.Handle("/writer/append", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		// FIXME
		_ = json.NewEncoder(w).Encode(output)
	}), ctx))
}
