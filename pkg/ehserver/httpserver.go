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
	"github.com/function61/eventhorizon/pkg/ehreaderfactory"
	"github.com/function61/eventhorizon/pkg/ehserver/ehserverclient"
	"github.com/function61/eventhorizon/pkg/system/ehcredstate"
	"github.com/function61/eventhorizon/pkg/system/ehpubsubstate"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/net/http/httputils"
	"github.com/function61/gokit/sync/taskrunner"
	"github.com/gorilla/mux"
)

func Server(ctx context.Context, logger *log.Logger) error {
	systemClient, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	tasks := taskrunner.New(ctx, logger)

	httpHandler, _, err := createHttpHandler(ctx, systemClient, func(task func(context.Context) error) {
		tasks.Start("mqtt", task)
	}, logger)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:    ":80",
		Handler: httpHandler,
	}

	tasks.Start("listener "+srv.Addr, func(_ context.Context) error {
		return httputils.RemoveGracefulServerClosedError(srv.ListenAndServe())
	})

	tasks.Start("listenershutdowner", httputils.ServerShutdownTask(srv))

	return tasks.Wait()
}

func createHttpHandler(
	ctx context.Context,
	systemClient *ehreader.SystemClient,
	startMqttTask func(task func(context.Context) error),
	logger *log.Logger,
) (http.Handler, SubscriptionNotifier, error) {
	credState, err := ehcredstate.LoadUntilRealtime(ctx, systemClient, logger)
	if err != nil {
		return nil, nil, err
	}

	pubSubState, err := ehpubsubstate.LoadUntilRealtime(ctx, systemClient, logger)
	if err != nil {
		return nil, nil, err
	}

	writerMaybeWithNotifier, notifier := func() (eh.Writer, SubscriptionNotifier) {
		mqttConfig := pubSubState.State.MqttConfig()

		if mqttConfig == nil {
			return systemClient.EventLog, nil
		} else {
			notifier := newMqttNotifier(*mqttConfig, startMqttTask, logex.Prefix("mqtt", logger))

			return wrapWriterWithNotifier(
				systemClient.EventLog,
				notifier,
				systemClient,
				logger), notifier
		}
	}()

	auth := &authenticator{
		credentials: credState,

		rawWriter:        writerMaybeWithNotifier,
		rawReader:        systemClient.EventLog,
		rawSnapshotStore: systemClient.SnapshotStore,
	}

	return serverHandler(auth), notifier, nil
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

		input := &ehserverclient.CreateStreamInput{}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.CreateStream(
			r.Context(),
			stream,
			*input.DEK,
			input.Data)
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

		data := eh.LogData{}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.Append(r.Context(), stream, data)
		if err != nil {
			if _, wasAboutLocking := err.(*eh.ErrOptimisticLockingFailed); wasAboutLocking {
				http.Error(w, err.Error(), http.StatusConflict)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
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

		data := eh.LogData{}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		appendResult, err := user.Writer.AppendAfter(r.Context(), after, data)
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

		snapshot := &eh.PersistedSnapshot{}
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

var respondJson = httputils.RespondJson // shorthand
