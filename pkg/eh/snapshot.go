package eh

import (
	"context"

	"github.com/function61/eventhorizon/pkg/policy"
)

type Snapshot struct {
	Cursor  Cursor `json:"Cursor"`
	Data    []byte `json:"Data"` // opaque byte blob, usually but not necessarily JSON
	Context string `json:"Context"`
}

func NewSnapshot(cursor Cursor, data []byte, context string) *Snapshot {
	return &Snapshot{cursor, data, context}
}

type SnapshotStore interface {
	// NOTE: returns os.ErrNotExist if snapshot is not found (which MUST not be
	//       considered an actual error)
	ReadSnapshot(ctx context.Context, stream StreamName, snapshotContext string) (*Snapshot, error)
	WriteSnapshot(ctx context.Context, snapshot Snapshot) error
	// returns os.ErrNotExist if snapshot-to-delete not found
	DeleteSnapshot(ctx context.Context, stream StreamName, snapshotContext string) error
}

func WrapSnapshotStoreWithAuthorizer(
	inner SnapshotStore,
	policy policy.Policy,
) SnapshotStore {
	return &authorizedSnapshotStore{
		inner:  inner,
		policy: policy,
	}
}

type authorizedSnapshotStore struct {
	inner  SnapshotStore
	policy policy.Policy
}

func (a *authorizedSnapshotStore) ReadSnapshot(ctx context.Context, stream StreamName, snapshotContext string) (*Snapshot, error) {
	if err := a.policy.Authorize(ActionSnapshotRead, stream.ResourceName()); err != nil {
		return nil, err
	}

	if err := a.policy.Authorize(ActionSnapshotRead, ctxToResourceName(snapshotContext)); err != nil {
		return nil, err
	}

	return a.inner.ReadSnapshot(ctx, stream, snapshotContext)
}

func (a *authorizedSnapshotStore) WriteSnapshot(ctx context.Context, snapshot Snapshot) error {
	if err := a.policy.Authorize(ActionSnapshotWrite, snapshot.Cursor.Stream().ResourceName()); err != nil {
		return err
	}

	if err := a.policy.Authorize(ActionSnapshotWrite, ctxToResourceName(snapshot.Context)); err != nil {
		return err
	}

	return a.inner.WriteSnapshot(ctx, snapshot)
}

func (a *authorizedSnapshotStore) DeleteSnapshot(
	ctx context.Context,
	stream StreamName,
	snapshotContext string,
) error {
	if err := a.policy.Authorize(ActionSnapshotDelete, stream.ResourceName()); err != nil {
		return err
	}

	if err := a.policy.Authorize(ActionSnapshotDelete, ctxToResourceName(snapshotContext)); err != nil {
		return err
	}

	return a.inner.DeleteSnapshot(ctx, stream, snapshotContext)
}

func ctxToResourceName(snapshotContext string) policy.ResourceName {
	return ResourceNameSnapshot.Child(snapshotContext)
}
