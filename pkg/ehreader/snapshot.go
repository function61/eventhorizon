package ehreader

import (
	"context"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"os"
)

type Snapshot struct {
	Cursor ehclient.Cursor
	Data   []byte
}

func NewSnapshot(cursor ehclient.Cursor, data []byte) *Snapshot {
	return &Snapshot{cursor, data}
}

type SnapshotStore interface {
	// NOTE: returns os.ErrNotExist if snapshot is not found (which MUST not be
	//       considered an actual error)
	LoadSnapshot(ctx context.Context, cursor ehclient.Cursor) (*Snapshot, error)
	StoreSnapshot(ctx context.Context, snapshot Snapshot) error
}

type EventsProcessorSnapshotCapability interface {
	InstallSnapshot(*Snapshot) error
	Snapshot() (*Snapshot, error)
}

type inMemSnapshotStore struct {
	snapshots map[string]*Snapshot
}

// interface assertion
var _ = (SnapshotStore)(&inMemSnapshotStore{})

// do not use in anything else than testing
func NewInMemSnapshotStore() *inMemSnapshotStore {
	return &inMemSnapshotStore{map[string]*Snapshot{}}
}

func (i *inMemSnapshotStore) LoadSnapshot(_ context.Context, cur ehclient.Cursor) (*Snapshot, error) {
	if snap, found := i.snapshots[cur.Stream()]; found {
		return snap, nil
	} else {
		return nil, os.ErrNotExist
	}
}

func (i *inMemSnapshotStore) StoreSnapshot(_ context.Context, snap Snapshot) error {
	i.snapshots[snap.Cursor.Stream()] = &snap

	return nil
}
