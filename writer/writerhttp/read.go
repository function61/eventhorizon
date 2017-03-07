package writerhttp

import (
	"encoding/json"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader/types"
	"github.com/function61/eventhorizon/writer"
	"net/http"
)

type ReadRequest struct {
	Cursor string
}

func ReadHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Cursor": "/tenants/foo:0:0"}' http://localhost:9092/liveread
	http.HandleFunc("/liveread", func(w http.ResponseWriter, r *http.Request) {
		var readRequest ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&readRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cur, errCursor := cursor.CursorFromserialized(readRequest.Cursor)
		if errCursor != nil {
			http.Error(w, errCursor.Error(), http.StatusBadRequest)
			return
		}

		readOpts := types.NewReadOptions()
		readOpts.Cursor = cur

		readResult, err := eventWriter.LiveReader.Read(readOpts)
		if err != nil {
			// TODO: not always server error, might be bad user input so should be bad req
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		encoder.Encode(readResult)
	})
}
