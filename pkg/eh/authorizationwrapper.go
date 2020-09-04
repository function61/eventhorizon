package eh

import (
	"context"

	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/eventhorizon/pkg/policy"
)

var (
	ActionStreamCreate   = policy.NewAction("eventhorizon:stream:Create")
	ActionStreamRead     = policy.NewAction("eventhorizon:stream:Read")
	ActionStreamAppend   = policy.NewAction("eventhorizon:stream:Append")
	ActionSnapshotRead   = policy.NewAction("eventhorizon:snapshot:Read")
	ActionSnapshotWrite  = policy.NewAction("eventhorizon:snapshot:Write")
	ActionSnapshotDelete = policy.NewAction("eventhorizon:snapshot:Delete")
)

type authorizedWriter struct {
	inner  Writer
	policy policy.Policy
}

// wraps a Writer so that write ops are only called if the client is allowed to do so
func WrapWriterWithAuthorizer(
	inner Writer,
	policy policy.Policy,
) Writer {
	return &authorizedWriter{
		inner:  inner,
		policy: policy,
	}
}

func (a *authorizedWriter) CreateStream(
	ctx context.Context,
	stream StreamName,
	dekEnvelope envelopeenc.Envelope,
	data *LogData,
) (*AppendResult, error) {
	if err := a.policy.Authorize(ActionStreamCreate, stream.ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.CreateStream(ctx, stream, dekEnvelope, data)
}

func (a *authorizedWriter) Append(
	ctx context.Context,
	stream StreamName,
	data LogData,
) (*AppendResult, error) {
	if err := a.policy.Authorize(ActionStreamAppend, stream.ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.Append(ctx, stream, data)
}

func (a *authorizedWriter) AppendAfter(
	ctx context.Context,
	after Cursor,
	data LogData,
) (*AppendResult, error) {
	if err := a.policy.Authorize(ActionStreamAppend, after.Stream().ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.AppendAfter(ctx, after, data)
}

// wraps a Reader so that read ops are only called if the client is allowed to do so
func WrapReaderWithAuthorizer(
	inner Reader,
	policy policy.Policy,
) Reader {
	return &authorizedReader{
		inner:  inner,
		policy: policy,
	}
}

type authorizedReader struct {
	inner  Reader
	policy policy.Policy
}

func (a *authorizedReader) Read(
	ctx context.Context,
	lastKnown Cursor,
) (*ReadResult, error) {
	if err := a.policy.Authorize(ActionStreamRead, lastKnown.Stream().ResourceName()); err != nil {
		return nil, err
	}

	return a.inner.Read(ctx, lastKnown)
}
