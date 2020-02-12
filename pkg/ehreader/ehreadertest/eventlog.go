package ehreadertest

import (
	"context"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

// Dummy in-memory based event log for testing
type EventLog struct {
	memoryStore map[string]*[]ehclient.LogEntry
}

func NewEventLog() *EventLog {
	return &EventLog{
		memoryStore: map[string]*[]ehclient.LogEntry{},
	}
}

func (e *EventLog) Append(ctx context.Context, stream string, events []string) error {
	entries, found := e.memoryStore[stream]
	if !found {
		entries = &[]ehclient.LogEntry{}

		e.memoryStore[stream] = entries
	}

	*entries = append(*entries, ehclient.LogEntry{
		Stream:  stream,
		Version: int64(len(*entries)),
		Events:  events,
	})

	return nil
}

// testing helper
func (e *EventLog) AppendE(stream string, events ...ehevent.Event) {
	eventsSerialized := []string{}

	for _, event := range events {
		eventsSerialized = append(eventsSerialized, ehevent.Serialize(event))
	}

	if err := e.Append(context.TODO(), stream, eventsSerialized); err != nil {
		panic(err)
	}
}

func (e *EventLog) Read(_ context.Context, lastKnown ehclient.Cursor) (*ehclient.ReadResult, error) {
	streamAllEntries := e.memoryStore[lastKnown.Stream()]

	if streamAllEntries == nil {
		return nil, fmt.Errorf("stream not created: %s", lastKnown.Stream())
	}

	lastEntryCur := lastKnown

	nextCur := lastKnown.Next()

	entries := (*streamAllEntries)[nextCur.Version():]

	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]

		lastEntryCur = ehclient.At(lastEntry.Stream, lastEntry.Version)
	}

	return &ehclient.ReadResult{
		Entries:   entries,
		LastEntry: lastEntryCur,
		More:      false,
	}, nil
}
