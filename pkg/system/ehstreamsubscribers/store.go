// Subscriptions for a given stream
package ehstreamsubscribers

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/gokit/logex"
	"github.com/function61/gokit/sliceutil"
)

const (
	LogPrefix = "ehstreamsubscribers"
)

type stateFormat struct {
	Subscriptions []string `json:"subscriptions"`
}

func newStateFormat() stateFormat {
	return stateFormat{[]string{}}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New(stream eh.StreamName) *Store {
	return &Store{
		version: stream.Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) Subscriptions() []string {
	defer lockAndUnlock(&s.mu)()

	return s.state.Subscriptions
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
	return "eh:sub:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() (ehevent.Types, ehevent.Types) {
	return nil, eh.MetaTypes
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
	case *eh.SubscriptionSubscribed:
		s.state.Subscriptions = append(s.state.Subscriptions, e.Id)
	case *eh.SubscriptionUnsubscribed:
		s.state.Subscriptions = sliceutil.FilterString(s.state.Subscriptions, func(id string) bool {
			return id != e.Id
		})
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
	stream eh.StreamName,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (*App, error) {
	store := New(stream)

	a := &App{
		store,
		ehreader.NewWithSnapshots(
			store,
			client.EventLog,
			client.SnapshotStore,
			logex.Prefix("Reader", logger)),
		client.EventLog,
		logger}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

func lockAndUnlock(mu *sync.Mutex) func() {
	mu.Lock()

	return func() {
		mu.Unlock()
	}
}
