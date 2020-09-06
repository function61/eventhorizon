package ehreadertest

import (
	"context"
	"os"

	"github.com/function61/eventhorizon/pkg/eh"
)

type SnapshotStore struct {
	snapshots map[string]*eh.PersistedSnapshot
	stats     SnapshotStoreStats
}

// interface assertion
var _ = (eh.SnapshotStore)(&SnapshotStore{})

// do not use in anything else than testing
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{
		snapshots: map[string]*eh.PersistedSnapshot{},
	}
}

func (i *SnapshotStore) ReadSnapshot(
	_ context.Context,
	stream eh.StreamName,
	snapshotContext string,
) (*eh.PersistedSnapshot, error) {
	i.stats.ReadOps++

	if snap, found := i.snapshots[stream.String()]; found {
		return snap, nil
	} else {
		return nil, os.ErrNotExist
	}
}

func (i *SnapshotStore) WriteSnapshot(_ context.Context, snap eh.PersistedSnapshot) error {
	i.stats.WriteOps++

	i.snapshots[snap.Cursor.Stream().String()] = &snap

	return nil
}

func (i *SnapshotStore) DeleteSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) error {
	i.stats.DeleteOps++

	delete(i.snapshots, stream.String())

	return nil
}

func (i *SnapshotStore) Stats() SnapshotStoreStats {
	return i.stats
}

type SnapshotStoreStats struct {
	WriteOps  int
	ReadOps   int
	DeleteOps int
}

func (s SnapshotStoreStats) Diff(to SnapshotStoreStats) SnapshotStoreStats {
	return SnapshotStoreStats{
		WriteOps:  to.WriteOps - s.WriteOps,
		ReadOps:   to.ReadOps - s.ReadOps,
		DeleteOps: to.DeleteOps - s.DeleteOps,
	}
}
