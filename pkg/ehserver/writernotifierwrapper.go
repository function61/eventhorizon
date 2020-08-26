package ehserver

import (
	"context"
	"log"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/system/ehstreamsubscribers"
	"github.com/function61/gokit/logex"
)

// notifies subscriber (subscriptionId) that one of hers subscription streams (stream cursor) has new data
type SubscriptionNotifier interface {
	// nil return does not indicate success, but only that the handling did not immediately fail
	NotifySubscriberOfActivity(
		ctx context.Context,
		subscription eh.SubscriptionId,
		appendResult eh.AppendResult,
	) error
}

type writerNotifierWrapper struct {
	innerWriter  eh.Writer
	notifier     SubscriptionNotifier
	systemClient *ehreader.SystemClient
	logl         *logex.Leveled
}

// wraps a Writer so that successfull writes:
// - resolve which subscribers are subscribed to the stream that was written into
// - invoker noficiation for each subscriber
func wrapWriterWithNotifier(
	innerWriter eh.Writer,
	notifier SubscriptionNotifier,
	systemClient *ehreader.SystemClient,
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
	initialEvents []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.CreateStream(ctx, stream, initialEvents)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) Append(
	ctx context.Context,
	stream eh.StreamName,
	events []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.Append(ctx, stream, events)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	events []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.AppendAfter(ctx, after, events)

	if err == nil {
		w.logIfNotifyError(w.notifySubscribers(ctx, result))
	}

	return result, err
}

func (w *writerNotifierWrapper) notifySubscribers(ctx context.Context, result *eh.AppendResult) error {
	subscriptionsState, err := ehstreamsubscribers.LoadUntilRealtime(
		ctx,
		result.Cursor.Stream(),
		w.systemClient,
		ehstreamsubscribers.GlobalCache,
		logex.Prefix(ehstreamsubscribers.LogPrefix, w.logl.Original))
	if err != nil {
		return err
	}

	for _, subscriptionId := range subscriptionsState.State.Subscriptions() {
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
