// Metadata (subscriptions, encryption keys, child streams, ...) for a given stream.
package ehstreammeta

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/sync/syncutil"
)

//go:generate genny -in=../../cachegen/cache.go -out=cache.gen.go -pkg=ehstreammeta gen CacheItemType=*App

const (
	LogPrefix              = "ehstreammeta"
	maxKeepTrackOfChildren = 500
)

var (
	GlobalCache = NewCache()
)

type stateFormat struct {
	Subscriptions []eh.SubscriptionId   `json:"Subscriptions"`
	DekEnvelope   *envelopeenc.Envelope `json:"DekEnvelope"`
	ChildStreams  []string              `json:"ChildStreams"` // child base names to conserve space
}

func newStateFormat() stateFormat {
	return stateFormat{
		Subscriptions: []eh.SubscriptionId{},
		DekEnvelope:   nil,
		ChildStreams:  []string{},
	}
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

func (s *Store) DekEnvelope() *envelopeenc.Envelope {
	defer lockAndUnlock(&s.mu)()

	return s.state.DekEnvelope
}

func (s *Store) Subscriptions() []eh.SubscriptionId {
	defer lockAndUnlock(&s.mu)()

	return s.state.Subscriptions
}

func (s *Store) Subscribed(id eh.SubscriptionId) bool {
	defer lockAndUnlock(&s.mu)()

	for _, candidate := range s.state.Subscriptions {
		if candidate.Equal(id) {
			return true
		}
	}

	return false
}

// - Snapshot size must be finite, so we can't keep track of infinite child streams. Consider
// this a tool for debug. If you really need guaranteed *all*, you need to build a service for it.
// - 2nd return is truncated flag
func (s *Store) ChildStreams() ([]eh.StreamName, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ourName := s.version.Stream()

	children := []eh.StreamName{}
	for _, chilName := range s.state.ChildStreams {
		children = append(children, ourName.Child(chilName))
	}

	return children, len(children) == maxKeepTrackOfChildren
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
	return "eh:streammeta:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	return ehclient.MetaDeserializer()
}

func (s *Store) ProcessEvents(_ context.Context, processAndCommit ehclient.EventProcessorHandler) error {
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
	case *eh.StreamStarted:
		s.state.DekEnvelope = &e.DekEnvelope
	case *eh.SubscriptionSubscribed:
		s.state.Subscriptions = append(s.state.Subscriptions, e.Id)
	case *eh.SubscriptionUnsubscribed:
		s.state.Subscriptions = remove(s.state.Subscriptions, e.Id)
	case *eh.StreamChildStreamCreated:
		s.state.ChildStreams = append(s.state.ChildStreams, e.Stream.Base())

		if len(s.state.ChildStreams) > maxKeepTrackOfChildren {
			s.state.ChildStreams = s.state.ChildStreams[1 : 1+maxKeepTrackOfChildren]
		}
	}

	return nil
}

type App struct {
	State  *Store
	Reader *ehclient.Reader
	Writer eh.Writer
	Logger *log.Logger
}

func LoadUntilRealtime(
	ctx context.Context,
	stream eh.StreamName,
	client *ehclient.SystemClient,
	cache *Cache,
	logger *log.Logger,
) (*App, error) {
	app := cache.Get(stream.String(), func() *App {
		store := New(stream)

		return &App{
			store,
			ehclient.NewReader(
				store,
				client,
				logex.Prefix("Reader", logger)),
			client.EventLog,
			logger}
	})

	return app, app.Reader.LoadUntilRealtimeIfStale(ctx, 5*time.Second)
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)

func remove(input []eh.SubscriptionId, remove eh.SubscriptionId) []eh.SubscriptionId {
	removed := []eh.SubscriptionId{}
	for _, item := range input {
		if !item.Equal(remove) {
			removed = append(removed, item)
		}
	}
	return removed
}
