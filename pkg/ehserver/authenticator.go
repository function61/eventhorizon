package ehserver

import (
	"errors"
	"net/http"
	"strings"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcred"
)

// authenticates HTTP requests to a user who can be authorization checked
type authenticator struct {
	credentials *ehcred.App

	// these provide "raw" access which does not make any more authentication/authorization checks
	rawReader        eh.Reader
	rawWriter        eh.Writer // probably already wrapped with notifier
	rawSnapshotStore eh.SnapshotStore
}

func (a *authenticator) AuthenticateRequest(r *http.Request) (*user, error) {
	apiKey, got := extractBearerToken(r.Header.Get("Authorization"))
	if !got {
		return nil, errors.New("missing header 'Authorization: Bearer ...'")
	}

	credential := a.credentials.State.CredentialByApiKey(apiKey)
	if credential == nil {
		return nil, errors.New("invalid API key")
	}

	policy := credential.Policy // shorthand

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
