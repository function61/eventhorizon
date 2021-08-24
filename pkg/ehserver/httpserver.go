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
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehserver/ehserverclient"
	"github.com/function61/eventhorizon/pkg/keyserver"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcred"
	"github.com/function61/eventhorizon/pkg/system/ehsettings"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/net/http/httpauth"
	"github.com/function61/gokit/net/http/httputils"
	"github.com/function61/gokit/sync/taskrunner"
	"github.com/gorilla/mux"
)

func Server(ctx context.Context, logger *log.Logger) error {
	systemClient, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromENV, logger)
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

	tasks.Start("listener "+srv.Addr, func(ctx context.Context) error {
		return httputils.CancelableServer(ctx, srv, func() error { return srv.ListenAndServe() })
	})

	return tasks.Wait()
}

func createHttpHandler(
	ctx context.Context,
	systemClient *ehclient.SystemClient,
	startMqttTask func(task func(context.Context) error),
	logger *log.Logger,
) (http.Handler, SubscriptionNotifier, error) {
	credState, err := ehcred.LoadUntilRealtime(ctx, systemClient)
	if err != nil {
		return nil, nil, err
	}

	pubSubState, err := ehsettings.LoadUntilRealtime(ctx, systemClient)
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

		logl: logex.Levels(logex.Prefix("authenticator", logger)),
	}

	keyServer, err := keyserver.NewServer("default.key", logex.Prefix("keyserver", logger))
	if err != nil {
		return nil, nil, err
	}

	// routePrefix:=os.Getenv("HTTP_ROUTE_PREFIX")
	routePrefix := "/api/eventhorizon"

	return serverHandler(auth, keyServer, routePrefix), notifier, nil
}

func serverHandler(
	auth *authenticator,
	keyServer keyserver.Unsealer,
	prefix string,
) http.Handler {
	router := mux.NewRouter()
	router.HandleFunc(prefix+"/read", func(w http.ResponseWriter, r *http.Request) {
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

	router.HandleFunc(prefix+"/stream-create", func(w http.ResponseWriter, r *http.Request) {
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

	router.HandleFunc(prefix+"/append", func(w http.ResponseWriter, r *http.Request) {
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

	router.HandleFunc(prefix+"/append-after", func(w http.ResponseWriter, r *http.Request) {
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

	parsePerspectiveOrOutputHTTPError := func(serialized string, w http.ResponseWriter) *eh.SnapshotPerspective {
		if serialized == "" {
			http.Error(w, "snapshot context not defined", http.StatusBadRequest)
			return nil
		}

		perspective := eh.ParseSnapshotPerspective(serialized)
		return &perspective
	}

	router.HandleFunc(prefix+"/snapshot", func(w http.ResponseWriter, r *http.Request) {
		perspective := parsePerspectiveOrOutputHTTPError(r.URL.Query().Get("perspective"), w)
		if perspective == nil {
			return // HTTP error was output
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

		input := eh.ReadSnapshotInput{
			Stream:      stream,
			Perspective: *perspective,
		}

		snapOutput, err := user.Snapshots.ReadSnapshot(r.Context(), input)
		if err != nil {
			if os.IsNotExist(err) {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		snap := snapOutput.Snapshot // we know there is nothing other interesting in the output structure

		// TODO: do this only if client requests so
		eagerRead, err := user.Reader.Read(r.Context(), snap.Cursor)
		if err != nil {
			// TODO: ignore error and just return snapshot?
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondJson(w, eh.ReadSnapshotOutput{
			Snapshot:  snap,
			EagerRead: eagerRead,
		})
	}).Methods(http.MethodGet)

	router.HandleFunc(prefix+"/snapshot", func(w http.ResponseWriter, r *http.Request) {
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

	router.HandleFunc(prefix+"/snapshot", func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		perspective := parsePerspectiveOrOutputHTTPError(r.URL.Query().Get("perspective"), w)
		if perspective == nil {
			return // HTTP error was output
		}

		stream, err := eh.DeserializeStreamName(r.URL.Query().Get("stream"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := user.Snapshots.DeleteSnapshot(r.Context(), stream, *perspective); err != nil {
			if err == os.ErrNotExist {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}).Methods(http.MethodDelete)

	router.HandleFunc(prefix+"/keyserver/envelope-decrypt", func(w http.ResponseWriter, r *http.Request) {
		user, err := auth.AuthenticateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		envelope := envelopeenc.Envelope{}
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// not checking for write here, because write implies also having read permissions,
		// and our policy language doesn't have "OR" yet
		if err := user.Policy.Authorize(eh.ActionStreamRead, policy.ResourceName(envelope.Label)); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		contentDecrypted, err := keyServer.UnsealEnvelope(r.Context(), envelope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(contentDecrypted)
	}).Methods(http.MethodPost)

	return router
}

var respondJson = httputils.RespondJson // shorthand
