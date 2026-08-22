// Easy-to-use consumer API on top of EventHorizon client (which is lower-level)
package ehreader

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

var (
	SuggestedPollingInterval = 10 * time.Second
)

/*
	encapsulates:

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

type EventsProcessorWithSnapshots interface {
	EventsProcessor
	EventsProcessorSnapshotCapability
}

// Serves reads for one processor. not safe for concurrent use
type Reader struct {
	client          ehclient.Reader
	eventTypes      ehevent.Allocators
	processor       EventsProcessor
	snapCap         EventsProcessorSnapshotCapability
	snapStore       SnapshotStore
	snapshotVersion *ehclient.Cursor
	logger          *slog.Logger
}

// "keep processor happy by feeding it from client"
func New(processor EventsProcessor, client ehclient.Reader, logger *slog.Logger) *Reader {
	return &Reader{
		client:          client,
		eventTypes:      processor.GetEventTypes(),
		processor:       processor,
		snapCap:         nil,
		snapStore:       nil,
		snapshotVersion: nil,
		logger:          logger,
	}
}

func NewWithSnapshots(
	processor EventsProcessorWithSnapshots,
	client ehclient.Reader,
	snapStore SnapshotStore,
	logger *slog.Logger,
) *Reader {
	return &Reader{
		client:          client,
		eventTypes:      processor.GetEventTypes(),
		processor:       processor,
		snapCap:         processor,
		snapStore:       snapStore,
		snapshotVersion: nil,
		logger:          logger,
	}
}

type loggerWriter struct {
	logger *log.Logger
}

func (w loggerWriter) Write(message []byte) (int, error) {
	err := w.logger.Output(2, strings.TrimSuffix(string(message), "\n"))
	return len(message), err
}

// starts "realtime" sync. until we get pub/sub, we're stuck with polling. but this is the
// API that will hide better realtime implementation once EventHorizon matures.
// runs forever (or until ctx is cancelled).
func (r *Reader) Synchronizer(
	ctx context.Context,
	pollInterval time.Duration,
	logger *slog.Logger,
) error {
	pollIntervalTicker := time.NewTicker(pollInterval)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollIntervalTicker.C:
			// eventually we'll migrate to realtime notifications from eventhorizon,
			// but until then polling will do

			if err := r.LoadUntilRealtime(ctx); err != nil {
				logger.ErrorContext(ctx, "LoadUntilRealtime", "err", err)
			}
		}
	}
}

func (r *Reader) LoadUntilRealtime(ctx context.Context) error {
	var nextRead ehclient.Cursor

	// start with creating a dummy commit without handling any events, so we can only
	// query the initial version of the aggregate
	if err := r.processor.ProcessEvents(ctx, func(
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

	// if we started from beginning, try to load a snapshot
	if nextRead.AtBeginning() && r.snapCap != nil {
		snap, err := r.snapStore.LoadSnapshot(ctx, nextRead)
		if err != nil {
			if err == os.ErrNotExist { // the snapshot just does not exist
				r.logger.InfoContext(ctx, "LoadSnapshot: initial snapshot not found", "stream", nextRead.Stream())

				// using this to signal the logic at the end to take and store a new snapshot
				snapshotVersion := ehclient.Beginning(nextRead.Stream())
				r.snapshotVersion = &snapshotVersion
			} else { // some other error (these are not fatal though)
				r.logger.ErrorContext(ctx, "LoadSnapshot", "err", err)

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
		resp, err := r.client.Read(ctx, nextRead)
		if err != nil {
			return err
		}

		// each record is an event batch (could be transaction)
		// TODO: make it opt-in to increase transaction batch size to all resp.Entries
		for _, record := range resp.Entries {
			events := []ehevent.Event{}

			for _, eventSerialized := range record.Events {
				event, err := ehevent.Deserialize(eventSerialized, r.eventTypes)
				if err != nil {
					return err
				}

				events = append(events, event)
			}

			// NOTE: we cannot skip len(events)==0 because meta events still increment version
			// and thos version increments need to be committed to database even if we have no other writes

			versionAfter := ehclient.At(nextRead.Stream(), record.Version)

			if err := r.processor.ProcessEvents(ctx, func(
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

		nextRead = resp.LastEntry

		if !resp.More {
			// EventsProcessor reached realtime. store newer snapshot if we:
			// - have a snapshot capability
			// - we know the latest snapshot is older than what EventsProcessor now knows
			if r.snapCap != nil && r.snapshotVersion != nil && r.snapshotVersion.Less(nextRead) {
				snap, err := r.snapCap.Snapshot()
				if err != nil {
					r.logger.ErrorContext(ctx, "EventsProcessor.Snapshot", "err", err)
				} else {
					r.snapshotVersion = &snap.Cursor

					// TODO: store this async?
					if err := r.snapStore.StoreSnapshot(ctx, *snap); err != nil {
						r.logger.ErrorContext(ctx, "StoreSnapshot", "err", err)
					}
				}
			}
			return nil
		}
	}
}

// wraps your AppendAfter() result with state-refreshed retries for ErrOptimisticLockingFailed
// FIXME: currently this cannot be used along with Synchronizer(), because the Reader
//
//	is not safe for concurrent use
func (r *Reader) TransactWrite(ctx context.Context, fn func() error) error {
	maxTries := 4
	var err error

	for i := 0; i < maxTries; i++ {
		err = fn()
		if err == nil {
			return nil // success
		}

		if _, wasAboutLocking := err.(*ehclient.ErrOptimisticLockingFailed); !wasAboutLocking {
			return err // some other error
		}

		r.logger.DebugContext(ctx, "ErrOptimisticLockingFailed", "try", i+1, "err", err)

		// reach realtime again, so we can try again
		if err := r.LoadUntilRealtime(ctx); err != nil {
			return err
		}
	}

	return fmt.Errorf("maxTries failed (%d): %v", maxTries, err)
}

// helper for your code to generate an error
func UnsupportedEventTypeErr(e ehevent.Event) error {
	return fmt.Errorf("unsupported event type: %s", e.MetaType())
}
