// Easy-to-use consumer API on top of EventHorizon client (which is lower-level)
package ehreader

import (
	"context"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/logex"
	"log"
	"time"
)

var (
	SuggestedPollingInterval = 10 * time.Second
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
	GetEventTypes() ehevent.Allocators
}

// Serves reads for one processor
type Reader struct {
	client     ehclient.Reader
	eventTypes ehevent.Allocators
	processor  EventsProcessor
}

// "keep processor happy by feeding it from client"
func New(processor EventsProcessor, client ehclient.Reader) *Reader {
	return &Reader{
		client,
		processor.GetEventTypes(),
		processor,
	}
}

// starts "realtime" sync. until we get pub/sub, we're stuck with polling. but this is the
// API that will hide better realtime implementation once EventHorizon matures
func (s *Reader) Synchronizer(
	ctx context.Context,
	pollInterval time.Duration,
	logger *log.Logger,
) error {
	logl := logex.Levels(logger)

	pollIntervalTicker := time.NewTicker(pollInterval)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollIntervalTicker.C:
			// eventually we'll migrate to realtime notifications from eventhorizon,
			// but until then polling will do

			if err := s.LoadUntilRealtime(ctx); err != nil {
				logl.Error.Printf("LoadUntilRealtime: %v", err)
			}
		}
	}
}

func (s *Reader) LoadUntilRealtime(ctx context.Context) error {
	var nextRead ehclient.Cursor

	// start with creating a dummy commit without handling any events, so we can only
	// query the initial version of the aggregate
	if err := s.processor.ProcessEvents(ctx, func(
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

			if err := s.processor.ProcessEvents(ctx, func(
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
