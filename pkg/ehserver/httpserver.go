// HTTP server that wraps concrete implementation of Event log and a snapshot store with
// authentication, authorization and subscription notifications.
package ehserver

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/system/ehcredstate"
	"github.com/function61/eventhorizon/pkg/system/ehpubsubstate"
	"github.com/function61/gokit/httputils"
	"github.com/function61/gokit/taskrunner"
	"github.com/gorilla/mux"
)

func Server(ctx context.Context, logger *log.Logger) error {
	systemClient, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	credState, err := ehcredstate.LoadUntilRealtime(ctx, systemClient, logger)
	if err != nil {
		return err
	}

	pubSubState, err := ehpubsubstate.LoadUntilRealtime(ctx, systemClient, logger)
	if err != nil {
		return err
	}

	tasks := taskrunner.New(ctx, logger)

	writerMaybeWithNotifier := func() eh.Writer {
		mqttConfig := pubSubState.State.MqttConfig()

		if mqttConfig == nil {
			return systemClient.EventLog
		} else {
			notifier := New(*mqttConfig, func(task func(context.Context) error) {
				tasks.Start("mqtt", task)
			})

			return wrapWriterWithNotifier(
				systemClient.EventLog,
				notifier,
				systemClient,
				logger)
		}
	}()

	auth := &authenticator{
		credentials: credState,

		rawWriter:        writerMaybeWithNotifier,
		rawReader:        systemClient.EventLog,
		rawSnapshotStore: systemClient.SnapshotStore,
	}

	srv := &http.Server{
		Addr:    ":80",
		Handler: serverHandler(auth),
	}

	tasks.Start("listener "+srv.Addr, func(_ context.Context) error {
		return httputils.RemoveGracefulServerClosedError(srv.ListenAndServe())
	})

	tasks.Start("listenershutdowner", httputils.ServerShutdownTask(srv))

	return tasks.Wait()
}

func serverHandler(auth *authenticator) http.Handler {
	router := mux.NewRouter()
	router.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		cursor, err := eh.DeserializeCursor(r.URL.Query().Get("after"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		res, err := user.Reader.Read(r.Context(), cursor)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondJson(w, res)
	}).Methods(http.MethodGet)

	router.HandleFunc("/stream-create", func(w http.ResponseWriter, r *http.Request) {
		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		events := []string{}
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.CreateStream(r.Context(), stream, events)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondJson(w, appendResult)
	}).Methods(http.MethodPost)

	router.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		// TODO: assert request content-type

		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		events := []string{}
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.Append(r.Context(), stream, events)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondJson(w, appendResult)
	}).Methods(http.MethodPost)

	router.HandleFunc("/append-after", func(w http.ResponseWriter, r *http.Request) {
		// TODO: assert request content-type

		after, err := eh.DeserializeCursor(r.URL.Query().Get("after"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		events := []string{}
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.AppendAfter(r.Context(), after, events)
		if err != nil {
			if _, isOptimisticLocking := err.(*eh.ErrOptimisticLockingFailed); isOptimisticLocking {
				http.Error(w, err.Error(), http.StatusConflict)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		respondJson(w, appendResult)
	}).Methods(http.MethodPost)

	router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snapshotContext := r.URL.Query().Get("context")
		if snapshotContext == "" {
			http.Error(w, "snapshot context not defined", http.StatusBadRequest)
			return
		}

		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		snap, err := user.Snapshots.ReadSnapshot(r.Context(), stream, snapshotContext)
		if err != nil {
			if os.IsNotExist(err) {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		respondJson(w, snap)
	}).Methods(http.MethodGet)

	router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snapshotContext := r.URL.Query().Get("context")
		if snapshotContext == "" {
			http.Error(w, "snapshot context not defined", http.StatusBadRequest)
			return
		}

		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		snap, err := user.Snapshots.ReadSnapshot(r.Context(), stream, snapshotContext)
		if err != nil {
			if os.IsNotExist(err) {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		respondJson(w, snap)
	}).Methods(http.MethodGet)

	router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		snapshot := &eh.Snapshot{}
		if err := json.NewDecoder(r.Body).Decode(snapshot); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := user.Snapshots.WriteSnapshot(r.Context(), *snapshot); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}).Methods(http.MethodPut)

	router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		snapshotContext := r.URL.Query().Get("context")
		if snapshotContext == "" {
			http.Error(w, "snapshot context not defined", http.StatusBadRequest)
			return
		}

		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := user.Snapshots.DeleteSnapshot(r.Context(), stream, snapshotContext); err != nil {
			if err == os.ErrNotExist {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}).Methods(http.MethodDelete)

	return router
}
