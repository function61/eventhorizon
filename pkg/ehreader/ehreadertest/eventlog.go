// Test helpers for testing EventHorizon consumers. Provides dummy in-memory event log.
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

// interface assertion
var _ ehclient.ReaderWriter = (*EventLog)(nil)

func NewEventLog() *EventLog {
	return &EventLog{
		memoryStore: map[string]*[]ehclient.LogEntry{},
	}
}

func (e *EventLog) Append(ctx context.Context, stream string, events []string) (*ehclient.AppendResult, error) {
	entries := e.memoryStore[stream]

	if entries == nil {
		return e.AppendAfter(ctx, ehclient.Beginning(stream), events)
	} else {
		return e.AppendAfter(
			ctx,
			ehclient.At(stream, int64(len(*entries)-1)),
			events)
	}
}

func (e *EventLog) AppendAfter(ctx context.Context, after ehclient.Cursor, events []string) (*ehclient.AppendResult, error) {
	stream := after.Stream()

	entries, found := e.memoryStore[stream]
	if !found {
		entries = &[]ehclient.LogEntry{}

		e.memoryStore[stream] = entries
	}

	afterRequested := after.Next()
	afterActual := ehclient.At(after.Stream(), int64(len(*entries)))

	if !afterRequested.Equal(afterActual) {
		return nil, ehclient.NewErrOptimisticLockingFailed(fmt.Errorf(
			"conflict: afterRequested=%s afterActual=%s",
			afterRequested.Serialize(),
			afterActual.Serialize()))
	}

	*entries = append(*entries, ehclient.LogEntry{
		Stream:  stream,
		Version: afterActual.Version(),
		Events:  events,
	})

	return &ehclient.AppendResult{
		Cursor: afterActual,
	}, nil
}

// testing helper
func (e *EventLog) AppendE(stream string, events ...ehevent.Event) {
	eventsSerialized := []string{}

	for _, event := range events {
		eventsSerialized = append(eventsSerialized, ehevent.Serialize(event))
	}

	if _, err := e.Append(context.TODO(), stream, eventsSerialized); err != nil {
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
