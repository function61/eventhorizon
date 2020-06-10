package writerproxy

// To issue any commands to Event Horizon, you must contact a Writer. But that means
// that you have to have correct TLS configuration, know the authentication tokens,
// know the IP addresses of all the Writers, which Writers are responsible for
// which streams etc.
//
// To make things easier, the Pusher mirrors the API surface of a Writer, and
// handles all that above stuff for you, so you can just issue HTTP requests to
// a hardcoded IP (loopback) which will proxy your commands to a Writer.
//
// This is the proxy portion for this described setup.

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/function61/eventhorizon/pkg/legacy/config"
	wtypes "github.com/function61/eventhorizon/pkg/legacy/writer/types"
	"github.com/function61/eventhorizon/pkg/legacy/writer/writerclient"
)

type Proxy struct {
	serverDone   *sync.WaitGroup
	server       *http.Server
	writerClient *writerclient.Client
}

func New(confCtx *config.Context, writerClient *writerclient.Client) *Proxy {
	// listen on loopback - this service is not intended to be called by
	// other components than the application
	p := &Proxy{
		serverDone:   &sync.WaitGroup{},
		server:       &http.Server{Addr: "127.0.0.1:9093"},
		writerClient: writerClient,
	}

	// these paths probably should be /writerproxy, but we decided to keep the
	// paths identical to Writer, so writerclient and pushlib writer proxy
	// client can be as similar as possible

	http.HandleFunc("/writer/append", func(w http.ResponseWriter, r *http.Request) {
		var appendToStreamRequest wtypes.AppendToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&appendToStreamRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := p.writerClient.Append(&appendToStreamRequest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		// FIXME
		_ = json.NewEncoder(w).Encode(output)
	})

	http.HandleFunc("/writer/create_stream", func(w http.ResponseWriter, r *http.Request) {
		var req wtypes.CreateStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		output, err := p.writerClient.CreateStream(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		// FIXME
		_ = json.NewEncoder(w).Encode(output)
	})

	http.HandleFunc("/writer/subscribe", func(w http.ResponseWriter, r *http.Request) {
		var req wtypes.SubscribeToStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := p.writerClient.SubscribeToStream(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// FIXME
		_, _ = w.Write([]byte("OK"))
	})

	http.HandleFunc("/writer/unsubscribe", func(w http.ResponseWriter, r *http.Request) {
		var req wtypes.UnsubscribeFromStreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := p.writerClient.UnsubscribeFromStream(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// FIXME
		_, _ = w.Write([]byte("OK"))
	})

	return p
}

func (p *Proxy) Run() {
	p.serverDone.Add(1)

	go func() {
		defer p.serverDone.Done()

		if err := p.server.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("writerproxy: ListenAndServe() error: %s", err)
		}
	}()
}

func (p *Proxy) Close() {
	// now close the server gracefully ("shutdown")
	// timeout could be given instead of nil as a https://golang.org/pkg/context/
	if err := p.server.Shutdown(context.TODO()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	p.serverDone.Wait()
}
