// Test helpers for testing EventHorizon consumers. Provides dummy in-memory event log.
package ehreadertest

import (
	"context"
	"fmt"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/envelopeenc"
)

// Dummy in-memory based event log for testing
type EventLog struct {
	memoryStore  map[string]*[]eh.LogEntry
	dekEnvelopes map[string]*envelopeenc.Envelope
}

// interface assertion
var _ eh.ReaderWriter = (*EventLog)(nil)

func NewEventLog() *EventLog {
	return &EventLog{
		memoryStore:  map[string]*[]eh.LogEntry{},
		dekEnvelopes: map[string]*envelopeenc.Envelope{},
	}
}

func (e *EventLog) Append(ctx context.Context, stream eh.StreamName, data eh.LogData) (*eh.AppendResult, error) {
	entries := e.memoryStore[stream.String()]

	if entries == nil {
		return e.AppendAfter(ctx, stream.Beginning(), data)
	} else {
		return e.AppendAfter(
			ctx,
			stream.At(int64(len(*entries)-1)),
			data)
	}
}

func (e *EventLog) AppendAfter(ctx context.Context, after eh.Cursor, data eh.LogData) (*eh.AppendResult, error) {
	stream := after.Stream()

	entries, found := e.memoryStore[stream.String()]
	if !found {
		entries = &[]eh.LogEntry{}

		e.memoryStore[stream.String()] = entries
	}

	afterRequested := after.Next()
	afterActual := after.Stream().At(int64(len(*entries)))

	if !afterRequested.Equal(afterActual) {
		return nil, eh.NewErrOptimisticLockingFailed(fmt.Errorf(
			"conflict: %s afterRequested=%d afterActual=%d",
			stream.String(),
			afterRequested.Version(),
			afterActual.Version()))
	}

	*entries = append(*entries, eh.LogEntry{
		Cursor: afterActual,
		Data:   data,
	})

	return &eh.AppendResult{
		Cursor: afterActual,
	}, nil
}

func (e *EventLog) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	dekEnvelope envelopeenc.Envelope,
	data *eh.LogData,
) (*eh.AppendResult, error) {
	e.dekEnvelopes[stream.String()] = &dekEnvelope

	return nil, nil
}

func (e *EventLog) Read(_ context.Context, lastKnown eh.Cursor) (*eh.ReadResult, error) {
	streamAllEntries := e.memoryStore[lastKnown.Stream().String()]

	if streamAllEntries == nil {
		return nil, fmt.Errorf("stream not created: %s", lastKnown.Stream())
	}

	nextCur := lastKnown.Next()

	entries := (*streamAllEntries)[nextCur.Version():]

	lastEntryCur := func() eh.Cursor {
		if len(entries) > 0 {
			return entries[len(entries)-1].Cursor
		} else {
			return lastKnown
		}
	}()

	return &eh.ReadResult{
		Entries:   entries,
		LastEntry: lastEntryCur,
		More:      false,
	}, nil
}

// for testing
func (e *EventLog) ResolveDekEnvelope(stream eh.StreamName) *envelopeenc.Envelope {
	return e.dekEnvelopes[stream.String()]
}
