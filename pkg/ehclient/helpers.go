package ehclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/log/logex"
)

// embed this struct in your implementation if you want to opt-out of snapshotting
type NoSnapshots struct{}

func (n *NoSnapshots) InstallSnapshot(_ *eh.Snapshot) error {
	return errors.New("Snapshotting not implemented")
}

func (n *NoSnapshots) Snapshot() (*eh.Snapshot, error) {
	return nil, errors.New("Snapshotting not implemented")
}

func (n *NoSnapshots) SnapshotContextAndVersion() string {
	return "" // signals to Reader that we opt out of snapshotting
}

// wraps your AppendAfter() result with state-refreshed retries for ErrOptimisticLockingFailed
// FIXME: currently this cannot be used along with Synchronizer(), because the Reader
//        is not safe for concurrent use
func (r *Reader) TransactWrite(ctx context.Context, fn func() error) error {
	maxTries := 4
	var err error

	for i := 0; i < maxTries; i++ {
		err = fn()
		if err == nil {
			return nil // success
		}

		if _, wasAboutLocking := err.(*eh.ErrOptimisticLockingFailed); !wasAboutLocking {
			return err // some other error
		}

		r.logl.Info.Printf("ErrOptimisticLockingFailed, try %d: %v", i+1, err)

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

// another helper
func LogIgnoredUnrecognizedEventType(e ehevent.Event, logl *logex.Leveled) {
	logl.Debug.Printf("ignoring unrecognized event type: %s", e.MetaType())
}
