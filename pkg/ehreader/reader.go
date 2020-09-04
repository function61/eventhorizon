// Easy-to-use consumer API on top of EventHorizon client (which is lower-level)
package ehreader

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/logex"
	"github.com/function61/gokit/syncutil"
)

/* encapsulates:

1) validate that current version is what we think it is
2) process events (via callback)
3) commit (via callback), while updating version
*/
type EventProcessorHandler func(
	cur eh.Cursor,
	handleEvent func(ehevent.Event) error,
	commit func(eh.Cursor) error,
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
	// 1st: types for app-specific events, 2nd: EventHorizon meta events (most times nil !)
	GetEventTypes() []LogDataKindDeserializer
}

type EventsProcessorSnapshotCapability interface {
	InstallSnapshot(*eh.Snapshot) error
	Snapshot() (*eh.Snapshot, error)
	SnapshotContextAndVersion() string
}

type EventsProcessorWithSnapshots interface {
	EventsProcessor
	EventsProcessorSnapshotCapability
}

// Serves reads for one processor. not safe for concurrent use
type Reader struct {
	client          *SystemClient
	deserializers   map[eh.LogDataKind]LogDataDeserializerFn
	processor       EventsProcessor
	snapCap         EventsProcessorSnapshotCapability
	snapStore       eh.SnapshotStore
	snapshotVersion *eh.Cursor
	logl            *logex.Leveled
	lastLoad        time.Time
	lastLoadMu      sync.Mutex
}

// "keep processor happy by feeding it from client"
func New(processor EventsProcessor, client *SystemClient, logger *log.Logger) *Reader {
	deserializers := map[eh.LogDataKind]LogDataDeserializerFn{}
	for _, item := range processor.GetEventTypes() {
		deserializers[item.Kind] = item.Deserializer
	}

	return &Reader{
		client:          client,
		deserializers:   deserializers,
		processor:       processor,
		snapCap:         nil,
		snapStore:       nil,
		snapshotVersion: nil,
		logl:            logex.Levels(logger),
	}
}

func NewWithSnapshots(
	processor EventsProcessorWithSnapshots,
	client *SystemClient,
	logger *log.Logger,
) *Reader {
	deserializers := map[eh.LogDataKind]LogDataDeserializerFn{}
	for _, item := range processor.GetEventTypes() {
		deserializers[item.Kind] = item.Deserializer
	}

	return &Reader{
		client:          client,
		deserializers:   deserializers,
		processor:       processor,
		snapCap:         processor,
		snapStore:       client.SnapshotStore,
		snapshotVersion: nil,
		logl:            logex.Levels(logger),
	}
}

// TODO: error-wrapping
func (r *Reader) LoadUntilRealtime(ctx context.Context) error {
	var nextRead eh.Cursor

	// start with creating a dummy commit without handling any events, so we can only
	// query the initial version of the aggregate
	if err := r.processor.ProcessEvents(ctx, func(
		versionInDb eh.Cursor,
		handleEvent func(ehevent.Event) error,
		commit func(eh.Cursor) error,
	) error {
		nextRead = versionInDb

		// purposefully missing here: handleEvent(); commit()

		return nil
	}); err != nil {
		return fmt.Errorf("LoadUntilRealtime: load cursor: %v", err)
	}

	// if we started from beginning, try to load a snapshot
	if nextRead.AtBeginning() && r.snapCap != nil {
		snap, err := r.snapStore.ReadSnapshot(
			ctx,
			nextRead.Stream(),
			r.snapCap.SnapshotContextAndVersion())
		if err != nil {
			if err == os.ErrNotExist { // the snapshot just does not exist
				r.logl.Info.Printf("ReadSnapshot: no initial snapshot for %s", nextRead.Stream())

				// using this to signal the logic at the end to take and store a new snapshot
				snapshotVersion := nextRead.Stream().Beginning()
				r.snapshotVersion = &snapshotVersion
			} else { // some other error (these are not fatal though)
				r.logl.Error.Printf("ReadSnapshot: %v", err)

				// this causes us to to not try saving snapshots
				// TODO: is this a good idea?
			}
		} else {
			r.snapshotVersion = &snap.Cursor

			if err := r.snapCap.InstallSnapshot(snap); err != nil {
				return fmt.Errorf("LoadUntilRealtime: InstallSnapshot: %w", err)
			}

			// easiest path is to start all over. yes, this recurses one level, but it
			// shouldn't recurse infinitely
			return r.LoadUntilRealtime(ctx)
		}
	}

	for {
		resp, err := r.client.EventLog.Read(ctx, nextRead)
		if err != nil {
			return err
		}

		// each record is an event batch (could be transaction)
		// TODO: make it opt-in to increase transaction batch size to all resp.Entries
		for _, record := range resp.Entries {
			events, err := func() ([]ehevent.Event, error) {
				deserializer, found := r.deserializers[record.Data.Kind]
				if !found {
					// not an error, but we've to proceed so we can commit going past this ignored entry
					return []ehevent.Event{}, nil
				}

				return deserializer(ctx, &record, r.client)
			}()
			if err != nil {
				return err
			}

			versionAfter := record.Cursor

			if err := r.processor.ProcessEvents(ctx, func(
				versionInDb eh.Cursor,
				handleEvent func(ehevent.Event) error,
				commit func(eh.Cursor) error,
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

		nextRead = resp.LastEntry

		if !resp.More {
			// EventsProcessor reached realtime. store newer snapshot if we:
			// - have a snapshot capability
			// - we know the latest snapshot is older than what EventsProcessor now knows
			if r.snapCap != nil && r.snapshotVersion != nil && r.snapshotVersion.Before(nextRead) {
				snap, err := r.snapCap.Snapshot()
				if err != nil {
					r.logl.Error.Printf("EventsProcessor.Snapshot: %v", err)
				} else {
					r.snapshotVersion = &snap.Cursor

					// TODO: store this async?
					if err := r.snapStore.WriteSnapshot(ctx, *snap); err != nil {
						r.logl.Error.Printf("WriteSnapshot: %v", err)
					}
				}
			}
			return nil
		}
	}
}

// same as LoadUntilRealtime(), but only loads if not done so recently
func (r *Reader) LoadUntilRealtimeIfStale(
	ctx context.Context,
	staleDuration time.Duration,
) error {
	// don't start concurrent ops
	defer syncutil.LockAndUnlock(&r.lastLoadMu)()

	if time.Since(r.lastLoad) > staleDuration {
		if err := r.LoadUntilRealtime(ctx); err != nil {
			return fmt.Errorf("LoadUntilRealtimeIfStale: %w", err)
		}

		r.lastLoad = time.Now()
	}

	return nil
}
