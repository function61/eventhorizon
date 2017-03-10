package writerhttp

import (
	"encoding/json"
	"github.com/function61/pyramid/cursor"
	rtypes "github.com/function61/pyramid/reader/types"
	"github.com/function61/pyramid/writer"
	"github.com/function61/pyramid/writer/authmiddleware"
	wtypes "github.com/function61/pyramid/writer/types"
	"net/http"
	"os"
)

func ReadHandlerInit(eventWriter *writer.EventstoreWriter) {
	// $ curl -d '{"Cursor": "/tenants/foo:0:0"}' http://localhost:9092/liveread
	http.Handle("/liveread", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		// FIXME: since this read operation holds a writer-wide mutex, a slow
		//        consumer can currently cause DOS when writer blocks
		if err := eventWriter.LiveReader.ReadIntoWriter(readOpts, w); err != nil {
			if os.IsNotExist(err) {
				http.Error(w, err.Error(), http.StatusNotFound)

			} else {
				// FIXME: sometimes it's user's fault (f.ex. cursor past EOF)
				//        so 4xx would be more appropriate
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})))
}
