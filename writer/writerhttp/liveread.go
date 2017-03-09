package writerhttp

import (
	"encoding/json"
	"github.com/function61/pyramid/cursor"
	rtypes "github.com/function61/pyramid/reader/types"
	"github.com/function61/pyramid/writer"
	wtypes "github.com/function61/pyramid/writer/writerhttp/types"
	"net/http"
)

func ReadHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Cursor": "/tenants/foo:0:0"}' http://localhost:9092/liveread
	http.HandleFunc("/liveread", func(w http.ResponseWriter, r *http.Request) {
		var req wtypes.LiveReadInput
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cur, errCursor := cursor.CursorFromserialized(req.Cursor)
		if errCursor != nil {
			http.Error(w, errCursor.Error(), http.StatusBadRequest)
			return
		}

		readOpts := rtypes.NewReadOptions()
		readOpts.Cursor = cur

		readResult, err := eventWriter.LiveReader.Read(readOpts)
		if err != nil {
			// FIXME: do not assume it's always about 404
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		encoder.Encode(readResult)
	})
}
