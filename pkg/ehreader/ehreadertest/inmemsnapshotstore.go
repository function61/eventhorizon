package ehreadertest

import (
	"context"
	"os"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehreader"
)

type inMemSnapshotStore struct {
	snapshots map[string]*ehreader.Snapshot
}

// interface assertion
var _ = (ehreader.SnapshotStore)(&inMemSnapshotStore{})

// do not use in anything else than testing
func NewInMemSnapshotStore() *inMemSnapshotStore {
	return &inMemSnapshotStore{map[string]*ehreader.Snapshot{}}
}

func (i *inMemSnapshotStore) LoadSnapshot(
	_ context.Context,
	cur ehclient.Cursor,
	snapshotContext string,
) (*ehreader.Snapshot, error) {
	if snap, found := i.snapshots[cur.Stream()]; found {
		return snap, nil
	} else {
		return nil, os.ErrNotExist
	}
}

func (i *inMemSnapshotStore) StoreSnapshot(_ context.Context, snap ehreader.Snapshot) error {
	i.snapshots[snap.Cursor.Stream()] = &snap

	return nil
}

func (i *inMemSnapshotStore) DeleteSnapshot(
	ctx context.Context,
	streamName string,
	snapshotContext string,
) error {
	delete(i.snapshots, streamName)

	return nil
}
