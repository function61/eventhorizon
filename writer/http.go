package writer

import (
	"log"
	"io"
	"encoding/json"
	"net/http"
)

type CreateStreamRequest struct {
	Name string
}

func HttpServe(eventWriter *EventstoreWriter, shutdown chan bool, done chan bool) {
	srv := &http.Server{Addr: ":8080"}

	http.HandleFunc("/create_stream", func(w http.ResponseWriter, r *http.Request) {
		var createStreamRequest CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&createStreamRequest); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		eventWriter.CreateStream(createStreamRequest.Name)

		io.WriteString(w, "OK")
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
