package writer

import (
	"encoding/json"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader"
	"io"
	"log"
	"net/http"
)

type ReadRequest struct {
	Cursor string
}

type CreateStreamRequest struct {
	Name string
}

type SubscribeToStreamRequest struct {
	Stream         string
	SubscriptionId string
}

type UnsubscribeFromStreamRequest struct {
	Stream         string
	SubscriptionId string
}

func HttpServe(eventWriter *EventstoreWriter, shutdown chan bool, done chan bool) {
	reader := reader.NewEventstoreReader()

	srv := &http.Server{Addr: ":8080"}

	// $ curl -d '{"Cursor": "/tenants/foo:0:0"} http://localhost:8080/read
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		var readRequest ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&readRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cur := cursor.CursorFromserializedMust(readRequest.Cursor)
		readResult, err := reader.Read(cur)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			panic(err)
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		encoder.Encode(readResult)
	})

	// $ curl -d '{"Name": "/foostream"}' http://localhost:8080/create_stream
	http.HandleFunc("/create_stream", func(w http.ResponseWriter, r *http.Request) {
		var createStreamRequest CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&createStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		eventWriter.CreateStream(createStreamRequest.Name)

		io.WriteString(w, "OK\n")
	})

	// $ curl -d '{"Stream": "/foostream", "SubscriptionId": "88c20701"}' http://localhost:8080/subscribe
	http.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		var subscribeToStreamRequest SubscribeToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&subscribeToStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.SubscribeToStream(subscribeToStreamRequest.Stream, subscribeToStreamRequest.SubscriptionId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		io.WriteString(w, "OK\n")
	})

	// $ curl -d '{"Stream": "/foostream", "SubscriptionId": "88c20701"}' http://localhost:8080/unsubscribe
	http.HandleFunc("/unsubscribe", func(w http.ResponseWriter, r *http.Request) {
		var unsubscribeFromStreamRequest UnsubscribeFromStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&unsubscribeFromStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := eventWriter.UnsubscribeFromStream(unsubscribeFromStreamRequest.Stream, unsubscribeFromStreamRequest.SubscriptionId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		io.WriteString(w, "OK\n")
	})

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("Httpserver: ListenAndServe() error: %s", err)
		}
	}()

	go func() {
		<-shutdown

		log.Printf("Httpserver: shutting down")

		if err := srv.Shutdown(nil); err != nil {
			panic(err) // failed shutting down
		}

		log.Printf("Httpserver: shutting down done")

		done <- true
	}()
}
