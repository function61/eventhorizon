package writerhttp

import (
	"encoding/json"
	"github.com/function61/pyramid/writer"
	"github.com/function61/pyramid/writer/authmiddleware"
	wtypes "github.com/function61/pyramid/writer/types"
	"io"
	"net/http"
)

func SubscribeToStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	// $ curl -d '{"Stream": "/foostream", "SubscriptionId": "88c20701"}' http://localhost:9092/subscribe
	http.Handle("/subscribe", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var subscribeToStreamRequest wtypes.SubscribeToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&subscribeToStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.SubscribeToStream(subscribeToStreamRequest.Stream, subscribeToStreamRequest.SubscriptionId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		io.WriteString(w, "OK\n")
	}), ctx))
}

func UnsubscribeFromStreamHandlerInit(eventWriter *writer.EventstoreWriter) {
	ctx := eventWriter.GetConfigurationContext()

	// $ curl -d '{"Stream": "/foostream", "SubscriptionId": "88c20701"}' http://localhost:9092/unsubscribe
	http.Handle("/unsubscribe", authmiddleware.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var unsubscribeFromStreamRequest wtypes.UnsubscribeFromStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&unsubscribeFromStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.UnsubscribeFromStream(unsubscribeFromStreamRequest.Stream, unsubscribeFromStreamRequest.SubscriptionId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		io.WriteString(w, "OK\n")
	}), ctx))
}
