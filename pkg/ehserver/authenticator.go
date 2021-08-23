package ehserver

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcred"
	"github.com/function61/gokit/log/logex"
)

// authenticates HTTP requests to a user who can be authorization checked
type authenticator struct {
	credentials *ehcred.App

	// these provide "raw" access which does not make any more authentication/authorization checks
	rawReader        eh.Reader
	rawWriter        eh.Writer // probably already wrapped with notifier
	rawSnapshotStore eh.SnapshotStore

	logl *logex.Leveled
}

func (a *authenticator) AuthenticateRequest(r *http.Request) (*user, error) {
	// so we're not operating on too old data
	if err := a.credentials.Reader.LoadUntilRealtimeIfStale(r.Context(), 10*time.Second); err != nil {
		a.logl.Error.Printf("LoadUntilRealtimeIfStale: %v", err)
	}

	apiKey, got := extractBearerToken(r.Header.Get("Authorization"))
	if !got {
		return nil, errors.New("missing header 'Authorization: Bearer ...'")
	}

	credential := a.credentials.State.CredentialByCombinedToken(apiKey)
	if credential == nil {
		return nil, errors.New("invalid API key")
	}

	policy := credential.MergedPolicy // shorthand

	return &user{
		Reader:    wrapReaderWithAuthorizer(a.rawReader, policy),
		Writer:    wrapWriterWithAuthorizer(a.rawWriter, policy),
		Snapshots: wrapSnapshotStoreWithAuthorizer(a.rawSnapshotStore, policy),
		Policy:    policy,
	}, nil
}

// data accessors tailored to user's data access policy
type user struct {
	Reader    eh.Reader
	Writer    eh.Writer
	Snapshots eh.SnapshotStore
	Policy    policy.Policy
}

func extractBearerToken(authHeader string) (string, bool) {
	token := strings.TrimPrefix(authHeader, "Bearer ")
	return token, token != authHeader
}
