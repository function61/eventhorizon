// Subscription activity (for deduplication)
package ehsubscription

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/system/ehsubscriptiondomain"
	"github.com/function61/gokit/sync/syncutil"
)

//go:generate genny -in=../../cachegen/cache.go -out=cache.gen.go -pkg=ehsubscription gen CacheItemType=*App

const (
	LogPrefix = "ehsubscription"
)

var (
	GlobalCache = NewCache()
)

type stateFormat struct {
	// TODO: investigate if this would benefit from bloom filter
	Recent []eh.CursorCompact // for deduplication. new changes at end of list
}

func newStateFormat() stateFormat {
	return stateFormat{[]eh.CursorCompact{}}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New(subscription eh.SubscriberID) *Store {
	return &Store{
		version: subscription.BackingStream().Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) RecentlySeen(cursor eh.Cursor) bool {
	defer lockAndUnlock(&s.mu)()

	for _, recent := range s.state.Recent {
		sameOrNewer := recent.Stream().Equal(cursor.Stream()) && !recent.Before(cursor)

		if sameOrNewer {
			return true
		}
	}

	return false
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
	return eh.NewV1Perspective("eh.sub") // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	return ehclient.EncryptedDataDeserializer(ehsubscriptiondomain.Types)
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
	case *ehsubscriptiondomain.SubscriptionActivity:
		s.state.Recent = insertIntoRecentList(s.state.Recent, e.Heads, 250)
	default:
		return ehclient.UnsupportedEventTypeErr(e)
	}

	return nil
}

func insertIntoRecentList(
	list []eh.CursorCompact,
	insert []eh.CursorCompact,
	max int,
) []eh.CursorCompact {
	// len(insert) as possible optimization, if the append of "insert" into "nextExisting" can
	// happen in-place
	nextExisting := make([]eh.CursorCompact, len(list)+len(insert))

	// for each cursor we're about to insert, if history had cursors for the same stream,
	// we'll drop them from the reconstructed slice (since we're inserting the new one anyway)
	nextExistingItems := 0

	// for quick lookup
	streamsToInsert := func() map[string]bool {
		sti := make(map[string]bool, len(insert))
		for _, cur := range insert {
			sti[cur.Stream().String()] = true
		}
		return sti
	}()

	// start with what we had in old cursors ..
	for _, cur := range list {
		// .. but only for streams we're not inserting
		if _, exists := streamsToInsert[cur.Stream().String()]; exists {
			continue
		}

		nextExisting[nextExistingItems] = cur
		nextExistingItems++
	}

	nextExisting = append(nextExisting[0:nextExistingItems], insert...)
	nextExistingItems += len(insert)

	if nextExistingItems > max {
		nextExisting = nextExisting[nextExistingItems-max:]
	}

	return nextExisting
}

type App struct {
	State  *Store
	Reader *ehclient.Reader
	Writer eh.Writer
}

func LoadUntilRealtime(
	ctx context.Context,
	subscription eh.SubscriberID,
	client *ehclient.SystemClient,
	cache *Cache,
) (*App, error) {
	app := cache.Get(subscription.String(), func() *App {
		store := New(subscription)

		return &App{
			store,
			ehclient.NewReader(
				store,
				client),
			client.EventLog}
	})

	return app, app.Reader.LoadUntilRealtimeIfStale(ctx, 5*time.Second)
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
