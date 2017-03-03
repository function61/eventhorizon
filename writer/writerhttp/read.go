package writerhttp

import (
	"encoding/json"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader"
	"github.com/function61/eventhorizon/writer"
	"net/http"
)

type ReadRequest struct {
	Cursor string
}

func ReadHandlerInit(eventWriter *writer.EventstoreWriter) {
	esReader := reader.NewEventstoreReader()

	// $ curl -d '{"Cursor": "/tenants/foo:0:0"}' http://localhost:9092/read
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
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

		readOpts := reader.NewReadOptions()
		readOpts.Cursor = cur

		readResult, err := esReader.Read(readOpts)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			panic(err)
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		encoder.Encode(readResult)
	})
}
