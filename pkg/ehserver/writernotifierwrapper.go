package ehserver

import (
	"context"
	"log"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
)

// notifies subscriber (subscriptionId) that one of hers subscription streams (stream cursor) has new data
type SubscriptionNotifier interface {
	// nil return does not indicate success, but only that the handling did not immediately fail
	NotifySubscriberOfActivity(
		ctx context.Context,
		subscription eh.SubscriptionId,
		appendResult eh.AppendResult,
	) error
	// waits for in-flight notifications to be sent. this is so that before reporting a request
	// in Lambda as succeeded, our process won't get paused before async notifications are sent.
	WaitInFlight()
}

type writerNotifierWrapper struct {
	innerWriter  eh.Writer
	notifier     SubscriptionNotifier
	systemClient *ehclient.SystemClient
	logl         *logex.Leveled
}

// wraps a Writer so that successfull writes:
// - resolve which subscribers are subscribed to the stream that was written into
// - invoker noficiation for each subscriber
func wrapWriterWithNotifier(
	innerWriter eh.Writer,
	notifier SubscriptionNotifier,
	systemClient *ehclient.SystemClient,
	logger *log.Logger,
) eh.Writer {
	return &writerNotifierWrapper{
		innerWriter:  innerWriter,
		notifier:     notifier,
		systemClient: systemClient,
		logl:         logex.Levels(logger),
	}
}

func (w *writerNotifierWrapper) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	dekEnvelope envelopeenc.Envelope,
	initialData *eh.LogData,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.CreateStream(ctx, stream, dekEnvelope, initialData)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) Append(
	ctx context.Context,
	stream eh.StreamName,
	data eh.LogData,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.Append(ctx, stream, data)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	data eh.LogData,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.AppendAfter(ctx, after, data)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) notifySubscribers(ctx context.Context, result *eh.AppendResult) error {
	streamMeta, err := ehstreammeta.LoadUntilRealtime(
		ctx,
		result.Cursor.Stream(),
		w.systemClient,
		ehstreammeta.GlobalCache)
	if err != nil {
		return err
	}

	for _, subscriptionId := range streamMeta.State.Subscriptions() {
		if err := w.notifier.NotifySubscriberOfActivity(ctx, subscriptionId, *result); err != nil {
			w.logl.Error.Printf("NotifySubscriberOfActivity: %v", err)
		}
	}

	return nil
}

func (w *writerNotifierWrapper) logIfNotifyError(err error) {
	if err != nil {
		w.logl.Error.Printf("notify: %v", err)
	}
}
