// EventHorizon credentials state: API keys, authorizations
package ehcredstate

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/gokit/syncutil"
)

type Credential struct {
	Id     string
	Name   string
	Policy policy.Policy
}

type CredentialWithApiKey struct {
	Credential
	ApiKey string
}

type stateFormat struct {
	Credentials map[string]*Credential `json:"credentials"`
}

func newStateFormat() stateFormat {
	return stateFormat{
		Credentials: map[string]*Credential{},
	}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New() *Store {
	return &Store{
		version: eh.SysCredentials.Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) Credential(apiKey string) *Credential {
	defer lockAndUnlock(&s.mu)()

	return s.state.Credentials[apiKey]
}

func (s *Store) Credentials() []Credential {
	defer lockAndUnlock(&s.mu)()

	credentials := []Credential{}
	for _, cred := range s.state.Credentials {
		credentials = append(credentials, *cred)
	}

	sort.Slice(credentials, func(i, j int) bool { return credentials[i].Id < credentials[j].Id })

	return credentials
}

// needed b/c of weird'ish stateFormat
func (s *Store) CredentialById(id string) (*CredentialWithApiKey, error) {
	defer lockAndUnlock(&s.mu)()

	for apiKey, cred := range s.state.Credentials {
		if cred.Id == id {
			return &CredentialWithApiKey{*cred, apiKey}, nil
		}
	}

	return nil, fmt.Errorf("API key for credential '%s' not found", id)
}

func (s *Store) Version() eh.Cursor {
	defer lockAndUnlock(&s.mu)()

	return s.version
}

func (s *Store) InstallSnapshot(snap *eh.Snapshot) error {
	defer lockAndUnlock(&s.mu)()

	s.version = snap.Cursor
	s.state = stateFormat{}

	return json.Unmarshal(snap.Data, &s.state)
}

func (s *Store) Snapshot() (*eh.Snapshot, error) {
	defer lockAndUnlock(&s.mu)()

	data, err := json.MarshalIndent(s.state, "", "\t")
	if err != nil {
		return nil, err
	}

	return eh.NewSnapshot(s.version, data, s.SnapshotContextAndVersion()), nil
}

func (s *Store) SnapshotContextAndVersion() string {
	return "eh:credentials:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() (ehevent.Types, ehevent.Types) {
	return ehcreddomain.Types, nil
}

func (s *Store) ProcessEvents(_ context.Context, processAndCommit ehreader.EventProcessorHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return processAndCommit(
		s.version,
		func(ev ehevent.Event) error { return s.processEvent(ev) },
		func(version eh.Cursor) error {
			s.version = version
			return nil
		})
}

func (s *Store) processEvent(ev ehevent.Event) error {
	switch e := ev.(type) {
	case *ehcreddomain.CredentialCreated:
		pol, err := policy.Deserialize([]byte(e.Policy))
		if err != nil {
			panic(err)
		}

		s.state.Credentials[e.ApiKey] = &Credential{
			Id:     e.Id,
			Name:   e.Name,
			Policy: *pol,
		}
	case *ehcreddomain.CredentialRevoked:
		for key, cred := range s.state.Credentials {
			if cred.Id == e.Id {
				delete(s.state.Credentials, key)
				break
			}
		}
	default:
		return ehreader.UnsupportedEventTypeErr(ev)
	}

	return nil
}

type App struct {
	State  *Store
	Reader *ehreader.Reader
	Writer eh.Writer
	Logger *log.Logger
}

func LoadUntilRealtime(
	ctx context.Context,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (*App, error) {
	store := New()

	a := &App{
		store,
		ehreader.NewWithSnapshots(
			store,
			client.EventLog,
			client.SnapshotStore,
			logger),
		client.EventLog,
		logger}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
