package ehreaderfactory

import (
	"context"
	"errors"
	"fmt"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
)

// TODO: implement ClientFrom

func SystemClientFrom(getter ehreader.ConfigStringGetter) (*ehreader.SystemClient, error) {
	// derive bootstrap systemClient (one that cannot resolve DEK envelopes)
	bootstrapClient, err := ehreader.SystemClientFrom(getter, nullResolver)
	if err != nil {
		return nil, err
	}

	return ehreader.SystemClientFrom(getter, func(ctx context.Context, stream eh.StreamName) (*envelopeenc.Envelope, error) {
		streamMeta, err := ehstreammeta.LoadUntilRealtime(
			ctx,
			stream,
			bootstrapClient,
			ehstreammeta.GlobalCache,
			nil)
		if err != nil {
			return nil, err
		}

		dekEnvelope := streamMeta.State.DekEnvelope()
		if dekEnvelope == nil {
			return nil, fmt.Errorf("no DEK envelope for %s", stream.String())
		}

		return dekEnvelope, nil
	})
}

func nullResolver(_ context.Context, _ eh.StreamName) (*envelopeenc.Envelope, error) {
	return nil, errors.New("nullResolver called")
}