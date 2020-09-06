package ehserver

// Wraps eh.Writer, eh.Reader and eh.SnapshotStore with access control checks

import (
	"context"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/eventhorizon/pkg/policy"
)

type authorizedWriter struct {
	inner  eh.Writer
	policy policy.Policy
}

// wraps a Writer so that write ops are only called if the client is allowed to do so
func wrapWriterWithAuthorizer(
	inner eh.Writer,
	policy policy.Policy,
) eh.Writer {
	return &authorizedWriter{
		inner:  inner,
		policy: policy,
	}
}

func (a *authorizedWriter) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	dekEnvelope envelopeenc.Envelope,
	data *eh.LogData,
) (*eh.AppendResult, error) {
	if err := a.policy.Authorize(eh.ActionStreamCreate, stream.ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.CreateStream(ctx, stream, dekEnvelope, data)
}

func (a *authorizedWriter) Append(
	ctx context.Context,
	stream eh.StreamName,
	data eh.LogData,
) (*eh.AppendResult, error) {
	if err := a.policy.Authorize(eh.ActionStreamAppend, stream.ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.Append(ctx, stream, data)
}

func (a *authorizedWriter) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	data eh.LogData,
) (*eh.AppendResult, error) {
	if err := a.policy.Authorize(eh.ActionStreamAppend, after.Stream().ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.AppendAfter(ctx, after, data)
}

// wraps a Reader so that read ops are only called if the client is allowed to do so
func wrapReaderWithAuthorizer(
	inner eh.Reader,
	policy policy.Policy,
) eh.Reader {
	return &authorizedReader{
		inner:  inner,
		policy: policy,
	}
}

type authorizedReader struct {
	inner  eh.Reader
	policy policy.Policy
}

func (a *authorizedReader) Read(
	ctx context.Context,
	lastKnown eh.Cursor,
) (*eh.ReadResult, error) {
	if err := a.policy.Authorize(eh.ActionStreamRead, lastKnown.Stream().ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.Read(ctx, lastKnown)
}

// wraps a SnapshotStore so that store is only accessed if the client is allowed to do so
func wrapSnapshotStoreWithAuthorizer(
	inner eh.SnapshotStore,
	policy policy.Policy,
) eh.SnapshotStore {
	return &authorizedSnapshotStore{
		inner:  inner,
		policy: policy,
	}
}

type authorizedSnapshotStore struct {
	inner  eh.SnapshotStore
	policy policy.Policy
}

func (a *authorizedSnapshotStore) ReadSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) (*eh.PersistedSnapshot, error) {
	if err := a.policy.Authorize(eh.ActionSnapshotRead, stream.ResourceName()); err != nil {
		return nil, err
	}

	if err := a.policy.Authorize(eh.ActionSnapshotRead, ctxToResourceName(snapshotContext)); err != nil {
		return nil, err
	}

	return a.inner.ReadSnapshot(ctx, stream, snapshotContext)
}

func (a *authorizedSnapshotStore) WriteSnapshot(
	ctx context.Context,
	snapshot eh.PersistedSnapshot,
) error {
	if err := a.policy.Authorize(eh.ActionSnapshotWrite, snapshot.Cursor.Stream().ResourceName()); err != nil {
		return err
	}

	if err := a.policy.Authorize(eh.ActionSnapshotWrite, ctxToResourceName(snapshot.Context)); err != nil {
		return err
	}

	return a.inner.WriteSnapshot(ctx, snapshot)
}

func (a *authorizedSnapshotStore) DeleteSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) error {
	if err := a.policy.Authorize(eh.ActionSnapshotDelete, stream.ResourceName()); err != nil {
		return err
	}

	if err := a.policy.Authorize(eh.ActionSnapshotDelete, ctxToResourceName(snapshotContext)); err != nil {
		return err
	}

	return a.inner.DeleteSnapshot(ctx, stream, snapshotContext)
}

func ctxToResourceName(snapshotContext string) policy.ResourceName {
	return eh.ResourceNameSnapshot.Child(snapshotContext)
}
