package ehdebug

// A "faux" store that'll capture the raw events for debug displaying. Previously we went
// lower level and just used eh.ReadResult directly, but that doesn't take into account
// encryption etc. By using a "dummy" store we can benefit from the high-level machinery.

import (
	"context"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/eheventencryption"
)

type entry struct {
	cursor eh.Cursor
	lines  []string
}

type store struct {
	version eh.Cursor
	entries []entry

	ehclient.NoSnapshots
}

func (s *store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	return mapEventsToRawEvent
}

func (s *store) ProcessEvents(_ context.Context, processAndCommit ehclient.EventProcessorHandler) error {
	uncommittedLines := []string{}

	return processAndCommit(
		s.version,
		func(ev ehevent.Event) error {
			uncommittedLines = append(uncommittedLines, ev.(*rawEvent).raw)
			return nil
		},
		func(version eh.Cursor) error {
			s.version = version
			s.entries = append(s.entries, entry{
				cursor: version,
				lines:  uncommittedLines,
			})
			return nil
		})
}

func loadUntilRealtime(
	ctx context.Context,
	cursor eh.Cursor,
	client *ehclient.SystemClient,
) (*store, error) {
	store := &store{
		version: cursor,
	}

	if err := ehclient.NewReader(
		store,
		client,
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

var mapEventsToRawEvent = []ehclient.LogDataKindDeserializer{
	{
		Kind: eh.LogDataKindMeta,
		Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehclient.SystemClient) ([]ehevent.Event, error) {
			return []ehevent.Event{newRawEvent(string(entry.Data.Raw))}, nil
		},
	},
	{
		Kind:       eh.LogDataKindEncryptedData,
		Encryption: true,
		Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehclient.SystemClient) ([]ehevent.Event, error) {
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
