// Easy-to-use consumer API on top of EventHorizon client (which is lower-level)
package ehclient

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/sync/syncutil"
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

	// Snapshot related APIs

	InstallSnapshot(*eh.Snapshot) error
	Snapshot() (*eh.Snapshot, error)
	// return "" if you don't implement snapshots
	SnapshotContextAndVersion() string
}

// Serves reads for one processor. not safe for concurrent use
type Reader struct {
	client           *SystemClient
	deserializers    map[eh.LogDataKind]LogDataDeserializerFn
	processor        EventsProcessor
	processorVersion *eh.Cursor // last known used as optimization, if gets out-of-sync it will be detected
	snapshotCapable  bool
	snapshotEncrypt  bool // true if processor handles any LogDataKind that are encrypted
	snapshotVersion  *eh.Cursor
	logl             *logex.Leveled
	lastLoad         time.Time
	lastLoadMu       sync.Mutex
}

// "keep processor happy by feeding it from client"
func NewReader(processor EventsProcessor, client *SystemClient) *Reader {
	snapshotEncrypt := false

	deserializers := map[eh.LogDataKind]LogDataDeserializerFn{}
	for _, deserializer := range processor.GetEventTypes() {
		deserializers[deserializer.Kind] = deserializer.Deserializer

		if deserializer.Encryption {
			snapshotEncrypt = true
		}
	}

	snapshotCapable := processor.SnapshotContextAndVersion() != ""

	return &Reader{
		client:           client,
		deserializers:    deserializers,
		processor:        processor,
		processorVersion: nil, // unknown at start
		snapshotCapable:  snapshotCapable,
		snapshotEncrypt:  snapshotEncrypt,
		snapshotVersion:  nil,                                                          // unknown at start
		logl:             logex.Levels(logex.Prefix("Reader[unknown]", client.logger)), // unknown stream name at start. will be augmented by discoverProcessorVersion()
	}
}

// feeds events to processor from the beginning of stream's event log (or as optimization
// starts from snapshot if there is one) and reads until we have reached realtime
// (= no more newer events) state
func (r *Reader) LoadUntilRealtime(ctx context.Context) error {
	if err := r.loadUntilRealtime(ctx); err != nil {
		return fmt.Errorf("LoadUntilRealtime: %w", err)
	}

	return nil
}

func (r *Reader) loadUntilRealtime(ctx context.Context) error {
	// after this r.processorVersion will be non-nil. this will be cached and will only
	// be done once.
	if err := r.discoverProcessorVersion(ctx); err != nil {
		return err
	}

	// if we started from beginning, try to load a snapshot
	if r.snapshotCapable && r.processorVersion.AtBeginning() {
		if err := r.fetchAndInstallSnapshot(ctx); err != nil {
			r.logl.Error.Printf("fetchAndInstallSnapshot: %v", err)

			// TODO: disable trying to write snapshots?
			// r.snapshotCapable = false
		}
	}

	for {
		resp, err := r.client.EventLog.Read(ctx, *r.processorVersion)
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

			versionToCommit := record.Cursor

			if err := r.processor.ProcessEvents(ctx, func(
				versionInDb eh.Cursor,
				handleEvent func(ehevent.Event) error,
				commit func(eh.Cursor) error,
			) error {
				// sanity check that DB is at version we need it to be at
				if !r.processorVersion.Equal(versionInDb) {
					return fmt.Errorf(
						"in-DB version (%s) out-of-sync with our perspective processorVersion (%s)",
						versionInDb.Serialize(),
						r.processorVersion.Serialize())
				}

				for _, event := range events {
					if errHandle := handleEvent(event); errHandle != nil {
						return fmt.Errorf("handleEvent: %v", errHandle)
					}
				}

				return commit(versionToCommit)
			}); err != nil {
				return err
			}

			// commit succeeded -> we know processor is at this version
			r.processorVersion = &versionToCommit
		}

		if !resp.More {
			r.logl.Debug.Printf("reached realtime: %s", r.processorVersion.Serialize())

			// store newer snapshot if we:
			// - didn't have a stored snapshot
			// - know the latest snapshot is older than what EventsProcessor now knows
			if r.snapshotCapable && (r.snapshotVersion == nil || r.snapshotVersion.Before(*r.processorVersion)) {
				snapshotVersion, err := r.uploadNewerSnapshot(ctx)
				if err != nil {
					r.logl.Error.Printf("uploadNewerSnapshot: %v", err)
				} else {
					r.snapshotVersion = snapshotVersion
				}
			}

			return nil
		}
	}
}

func (r *Reader) discoverProcessorVersion(ctx context.Context) error {
	// only discover it once
	if r.processorVersion != nil {
		return nil
	}

	var processorVersion eh.Cursor

	// start an empty transaction without committing any events, so we can
	// query the initial version of the processor
	if err := r.processor.ProcessEvents(ctx, func(
		versionInDb eh.Cursor,
		handleEvent func(ehevent.Event) error,
		commit func(eh.Cursor) error,
	) error {
		processorVersion = versionInDb

		// purposefully missing here: handleEvent(); commit()

		return nil
	}); err != nil {
		return fmt.Errorf("load cursor: %v", err)
	}

	r.processorVersion = &processorVersion

	// re-bind log prefix to not be unknown stream name
	newPrefix := fmt.Sprintf("Reader[%s]", processorVersion.Stream().String())
	r.logl = logex.Levels(logex.Prefix(newPrefix, r.client.logger))

	return nil
}

// caller determined that processor needs to start from beginning. try to see if we have
// a snapshot we could jumpstart from. (if not, just start from beginning.)
func (r *Reader) fetchAndInstallSnapshot(ctx context.Context) error {
	stream := r.processorVersion.Stream()

	snapPersisted, err := r.client.SnapshotStore.ReadSnapshot(
		ctx,
		stream,
		r.processor.SnapshotContextAndVersion())
	if err != nil {
		if err == os.ErrNotExist { // the snapshot just does not exist
			r.logl.Info.Printf("no initial snapshot for %s", stream)

			// not an error - we just need to continue fetching data from beginning
			return nil
		} else { // some other error (these are not fatal though)
			return err
		}
	}

	// convert to app-level snapshot by decrypting
	snap, err := snapPersisted.DecryptIfRequired(func() ([]byte, error) {
		return r.client.LoadDek(ctx, stream)
	})
	if err != nil {
		return err
	}

	if err := r.processor.InstallSnapshot(snap); err != nil {
		return fmt.Errorf("InstallSnapshot: %w", err)
	}

	r.processorVersion = &snap.Cursor
	r.snapshotVersion = &snap.Cursor

	return nil
}

func (r *Reader) uploadNewerSnapshot(ctx context.Context) (*eh.Cursor, error) {
	snap, err := r.processor.Snapshot()
	if err != nil {
		return nil, err
	}

	persisted, err := func() (*eh.PersistedSnapshot, error) {
		if r.snapshotEncrypt {
			dek, err := r.client.LoadDek(ctx, snap.Cursor.Stream())
			if err != nil {
				return nil, err
			}

			return snap.Encrypted(dek)
		} else {
			return snap.Unencrypted(), nil
		}
	}()
	if err != nil {
		return nil, err
	}

	// TODO: store this async?
	if err := r.client.SnapshotStore.WriteSnapshot(ctx, *persisted); err != nil {
		return nil, err
	}

	return &snap.Cursor, nil
}

// same as LoadUntilRealtime(), but only loads if not done so recently
func (r *Reader) LoadUntilRealtimeIfStale(
	ctx context.Context,
	staleDuration time.Duration,
) error {
	// don't start concurrent ops
	defer syncutil.LockAndUnlock(&r.lastLoadMu)()

	if time.Since(r.lastLoad) > staleDuration {
		if err := r.loadUntilRealtime(ctx); err != nil {
			return fmt.Errorf("LoadUntilRealtimeIfStale: %w", err)
		}

		r.lastLoad = time.Now()
	}

	return nil
}
