package writerhttp

import (
	"encoding/json"
	"net/http"

	"github.com/function61/eventhorizon/pkg/legacy/writer"
	"github.com/function61/eventhorizon/pkg/legacy/writer/authmiddleware"
	wtypes "github.com/function61/eventhorizon/pkg/legacy/writer/types"
)

func CreateStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	http.Handle("/writer/create_stream", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var createStreamRequest wtypes.CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&createStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := eventWriter.CreateStream(createStreamRequest.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		// FIXME
		_ = json.NewEncoder(w).Encode(output)
	}), ctx))
}
