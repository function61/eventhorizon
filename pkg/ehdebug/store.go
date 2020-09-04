package ehdebug

// A "faux" store that'll capture the raw events for debug displaying. Previously we went
// lower level and just used eh.ReadResult directly, but that doesn't take into account
// encryption etc. By using a "dummy" store we can benefit from the high-level machinery.

import (
	"context"
	"log"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/eheventencryption"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/gokit/logex"
)

type entry struct {
	cursor eh.Cursor
	lines  []string
}

type store struct {
	version          eh.Cursor
	mu               sync.Mutex
	entries          []entry
	uncommittedLines []string
}

func (s *store) Entries() []entry {
	return s.entries
}

func (s *store) GetEventTypes() []ehreader.LogDataKindDeserializer {
	return mapEventsToRawEvent
}

func (s *store) ProcessEvents(_ context.Context, processAndCommit ehreader.EventProcessorHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return processAndCommit(
		s.version,
		func(ev ehevent.Event) error { return s.processEvent(ev) },
		func(version eh.Cursor) error {
			s.version = version
			s.entries = append(s.entries, entry{
				cursor: version,
				lines:  s.uncommittedLines,
			})
			s.uncommittedLines = []string{}
			return nil
		})
}

func (s *store) processEvent(ev ehevent.Event) error {
	switch e := ev.(type) {
	case *rawEvent:
		s.uncommittedLines = append(s.uncommittedLines, e.raw)
	default:
		return ehreader.UnsupportedEventTypeErr(ev)
	}

	return nil
}

func loadUntilRealtime(
	ctx context.Context,
	cursor eh.Cursor,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (*store, error) {
	store := &store{
		version:          cursor,
		uncommittedLines: []string{},
	}

	if err := ehreader.New(
		store,
		client,
		logex.Prefix("Reader", logger),
	).LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// encapsulates raw serialized payload by faking deserialization as succeeded, but all
// events map to this raw event whose raw content we can now access
func newRawEvent(raw string) ehevent.Event {
	return &rawEvent{raw}
}

type rawEvent struct {
	raw string
}

func (e *rawEvent) MetaType() string {
	return "rawEvent"
}

func (e *rawEvent) Meta() *ehevent.EventMeta {
	return &ehevent.EventMeta{}
}

var mapEventsToRawEvent = []ehreader.LogDataKindDeserializer{
	{
		Kind: eh.LogDataKindMeta,
		Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehreader.SystemClient) ([]ehevent.Event, error) {
			return []ehevent.Event{newRawEvent(string(entry.Data.Raw))}, nil
		},
	},
	{
		Kind: eh.LogDataKindEncryptedData,
		Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehreader.SystemClient) ([]ehevent.Event, error) {
			events := []ehevent.Event{}

			dek, err := client.LoadDek(ctx, entry.Cursor.Stream())
			if err != nil {
				return nil, err
			}

			eventsSerialized, err := eheventencryption.Decrypt(entry.Data, dek)
			if err != nil {
				return nil, err
			}

			for _, eventSerialized := range eventsSerialized {
				events = append(events, newRawEvent(eventSerialized))
			}

			return events, nil
		},
	},
}
