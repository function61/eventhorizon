package ehreader

import (
	"context"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

/* encapsulates:

1) validate that current version is what we think it is
2) process events (via callback)
3) commit (via callback), while updating version
*/
type EventProcessorHandler func(
	cur ehclient.Cursor,
	handleEvent func(ehevent.Event) error,
	commit func(ehclient.Cursor) error,
) error

type EventsProcessor interface {
	/*	returns error if:

		- failed to start a tx (user code)
		- failed to resolve the version (user code)
		- handle (infra code) failed which itself contains:
		  * error for handleEvent()
		  * commit()
		  * etc.
	*/
	ProcessEvents(ctx context.Context, handle EventProcessorHandler) error
}

type Reader struct {
	eventTypes ehevent.Allocators
	client     ehclient.Reader
}

func New(
	client ehclient.Reader,
	eventTypes ehevent.Allocators,
) *Reader {
	return &Reader{
		eventTypes,
		client,
	}
}

func (s *Reader) LoadUntilRealtime(
	ctx context.Context,
	processor EventsProcessor,
) error {
	var nextRead ehclient.Cursor

	// start with creating a dummy commit without handling any events, so we can only
	// query the initial version of the aggregate
	if err := processor.ProcessEvents(ctx, func(
		versionInDb ehclient.Cursor,
		handleEvent func(ehevent.Event) error,
		commit func(ehclient.Cursor) error,
	) error {
		nextRead = versionInDb

		// purposefully missing here: handleEvent(); commit()

		return nil
	}); err != nil {
		return fmt.Errorf("LoadUntilRealtime: load cursor: %v", err)
	}

	for {
		resp, err := s.client.Read(ctx, nextRead)
		if err != nil {
			return err
		}

		// each record is an event batch (could be transaction)
		// TODO: make it opt-in to increase transaction batch size to all resp.Entries
		for _, record := range resp.Entries {
			events := []ehevent.Event{}

			for _, eventSerialized := range record.Events {
				event, err := ehevent.Deserialize(eventSerialized, s.eventTypes)
				if err != nil {
					return err
				}

				events = append(events, event)
			}

			// NOTE: we cannot skip len(events)==0 because meta events still increment version
			// and thos version increments need to be committed to database even if we have no other writes

			versionAfter := ehclient.At(nextRead.Stream(), record.Version)

			if err := processor.ProcessEvents(ctx, func(
				versionInDb ehclient.Cursor,
				handleEvent func(ehevent.Event) error,
				commit func(ehclient.Cursor) error,
			) error {
				if !nextRead.Equal(versionInDb) {
					return fmt.Errorf(
						"LoadUntilRealtime: in-DB version (%s) out-of-sync with nextRead (%s)",
						versionInDb.Serialize(),
						nextRead.Serialize())
				}

				for _, event := range events {
					if errHandle := handleEvent(event); errHandle != nil {
						return fmt.Errorf("LoadUntilRealtime: handleEvent: %v", errHandle)
					}
				}

				return commit(versionAfter)
			}); err != nil {
				return err
			}

			nextRead = versionAfter // only needed for out-of-sync check
		}

		if !resp.More {
			return nil
		}

		nextRead = resp.LastEntry
	}
}

// helper for your code to generate an error
func UnsupportedEventTypeErr(e ehevent.Event) error {
	return fmt.Errorf("unsupported event type: %s", e.MetaType())
}
