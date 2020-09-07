// "Directory service" - allows enumerating EventHorizon child streams in a performant way,
// because projection snapshots are used as a cache.
// NOTE: you cannot list streams if there are so many child streams that they won't fit
//       in one projection record. (consider this as a debug tool)
package ehchildstreams

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/gokit/logex"
)

type stateFormat struct {
	ChildStreams []string `json:"child_streams"` // child base names to conserve space
}

func newStateFormat() stateFormat {
	return stateFormat{
		ChildStreams: []string{},
	}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
	logl    *logex.Leveled
}

func New(streamName eh.StreamName, logger *log.Logger) *Store {
	return &Store{
		version: streamName.Beginning(),
		state:   newStateFormat(),
		logl:    logex.Levels(logger),
	}
}

func (s *Store) Version() eh.Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.version
}

func (s *Store) InstallSnapshot(snap *eh.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.version = snap.Cursor
	s.state = stateFormat{}

	return json.Unmarshal(snap.Data, &s.state)
}

func (s *Store) Snapshot() (*eh.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(s.state, "", "\t")
	if err != nil {
		return nil, err
	}

	return eh.NewSnapshot(s.version, data, s.SnapshotContextAndVersion()), nil
}

func (s *Store) SnapshotContextAndVersion() string {
	return "eh:dir:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) ChildStreams() []eh.StreamName {
	s.mu.Lock()
	defer s.mu.Unlock()

	ourName := s.version.Stream()

	children := []eh.StreamName{}
	for _, chilName := range s.state.ChildStreams {
		children = append(children, ourName.Child(chilName))
	}

	return children
}

func (s *Store) GetEventTypes() []ehreader.LogDataKindDeserializer {
	return ehreader.MetaDeserializer()
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
	case *eh.StreamChildStreamCreated:
		s.state.ChildStreams = append(s.state.ChildStreams, e.Stream.Base())
	default:
		// we're only interested in child stream creation
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
	streamName eh.StreamName,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (*App, error) {
	store := New(streamName, logger)

	a := &App{
		store,
		ehreader.New(
			store,
			client,
			logger),
		client.EventLog,
		logger}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}
