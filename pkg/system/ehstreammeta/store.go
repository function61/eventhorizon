// Metadata (subscriptions, encryption keys, child streams, ...) for a given stream.
package ehstreammeta

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/sync/syncutil"
)

//go:generate genny -in=../../cachegen/cache.go -out=cache.gen.go -pkg=ehstreammeta gen CacheItemType=*App

const (
	maxKeepTrackOfChildren = 500
)

var (
	GlobalCache = NewCache()
)

type stateFormat struct {
	Created       time.Time
	Subscriptions []eh.SubscriberID
	DEK           *envelopeenc.Envelope
	KeyGroupId    *string
	ChildStreams  []string // child base names to conserve space
	TotalBytes    int64
}

func newStateFormat() stateFormat {
	return stateFormat{
		Subscriptions: []eh.SubscriberID{},
		DEK:           nil,
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

func (s *Store) Data() stateFormat {
	defer lockAndUnlock(&s.mu)()

	return s.state
}

// minimum number of events in the stream. matches # of events only if each log message has exactly one event.
func (s *Store) TotalLogMessages() int64 {
	return s.version.Version() + 1
}

func (s *Store) Subscriptions() []eh.SubscriberID {
	defer lockAndUnlock(&s.mu)()

	return s.state.Subscriptions
}

func (s *Store) Subscribed(id eh.SubscriberID) bool {
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

	return eh.NewSnapshot(s.version, data, s.Perspective()), nil
}

func (s *Store) Perspective() eh.SnapshotPerspective {
	return eh.NewV1Perspective("eh.streammeta") // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	metaDeserializer := ehclient.MetaDeserializer()[0]

	// somewhat hackish setup to:
	// 1) only actually parse LogDataKindMeta events
	// 2) but count all log messages (even encrypted ones) and synthesize statistics log event for them
	//    so we can count bytes consumed by the stream
	return injectSyntheticStatisticsDeserializer(metaDeserializer)
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
		s.state.Created = e.Meta().Time()
		s.state.DEK = &e.DEK
		s.state.KeyGroupId = &e.KeyGroupId
	case *eh.SubscriptionSubscribed:
		s.state.Subscriptions = append(s.state.Subscriptions, e.ID)
	case *eh.SubscriptionUnsubscribed:
		s.state.Subscriptions = remove(s.state.Subscriptions, e.ID)
	case *eh.StreamChildStreamCreated:
		s.state.ChildStreams = append(s.state.ChildStreams, e.Stream.Base())

		if len(s.state.ChildStreams) > maxKeepTrackOfChildren {
			s.state.ChildStreams = s.state.ChildStreams[1 : 1+maxKeepTrackOfChildren]
		}
	case *syntheticStatisticsEvent: // not actually present in the stream
		s.state.TotalBytes += int64(e.NumBytes)
	}

	return nil
}

type App struct {
	State  *Store
	Reader *ehclient.Reader
	Writer eh.Writer
}

func LoadUntilRealtime(
	ctx context.Context,
	stream eh.StreamName,
	client *ehclient.SystemClient,
	cache *Cache,
) (*App, error) {
	app := cache.Get(stream.String(), func() *App {
		store := New(stream)

		reader := ehclient.NewReader(store, client)
		// to disambiguate as there are probably multiple readers for this same stream to prevent this:
		//   Reader[/$/settings] [DEBUG] reached realtime: /$/settings@1
		//   Reader[/$/settings] [DEBUG] reached realtime: /$/settings@1
		reader.AddLogPrefix("(streammeta)")

		return &App{
			store,
			reader,
			client.EventLog}
	})

	return app, app.Reader.LoadUntilRealtimeIfStale(ctx, 5*time.Second)
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)

func remove(input []eh.SubscriberID, remove eh.SubscriberID) []eh.SubscriberID {
	removed := []eh.SubscriberID{}
	for _, item := range input {
		if !item.Equal(remove) {
			removed = append(removed, item)
		}
	}
	return removed
}
