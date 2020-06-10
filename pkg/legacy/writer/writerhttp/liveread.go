package writerhttp

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/function61/eventhorizon/pkg/legacy/cursor"
	rtypes "github.com/function61/eventhorizon/pkg/legacy/reader/types"
	"github.com/function61/eventhorizon/pkg/legacy/writer"
	"github.com/function61/eventhorizon/pkg/legacy/writer/authmiddleware"
	wtypes "github.com/function61/eventhorizon/pkg/legacy/writer/types"
)

func ReadHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	http.Handle("/writer/liveread", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		readOpts.MaxLinesToRead = req.MaxLinesToRead

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
	}), ctx))
}
