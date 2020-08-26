package ehserver

import (
	"context"
	"log"
	"sync"

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
	// meat & bones of the implementation

	innerWriter eh.Writer
	notifier    SubscriptionNotifier

	// these are used to resolve subscribers for a stream

	streamSubscribers   map[string]*ehstreamsubscribers.App
	streamSubscribersMu sync.Mutex
	systemClient        *ehreader.SystemClient

	logger *log.Logger
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
		innerWriter: innerWriter,
		notifier:    notifier,

		streamSubscribers: map[string]*ehstreamsubscribers.App{},
		systemClient:      systemClient,

		logger: logger,
	}
}

func (w *writerNotifierWrapper) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	initialEvents []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.CreateStream(ctx, stream, initialEvents)

	if err == nil { // notify only on successes
		w.notify(ctx, result)
	}

	return result, err
}

func (w *writerNotifierWrapper) Append(
	ctx context.Context,
	stream eh.StreamName,
	events []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.Append(ctx, stream, events)

	if err == nil { // notify only on successes
		w.notify(ctx, result)
	}

	return result, err
}

func (w *writerNotifierWrapper) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	events []string,
) (*eh.AppendResult, error) {
	result, err := w.innerWriter.AppendAfter(ctx, after, events)

	if err == nil { // notify only on successes
		w.notify(ctx, result)
	}

	return result, err
}

func (w *writerNotifierWrapper) notify(ctx context.Context, result *eh.AppendResult) {
	if err := w.notify2(ctx, result); err != nil {
		logex.Levels(w.logger).Error.Printf("notify: %v", err)
	}
}

func (w *writerNotifierWrapper) notify2(ctx context.Context, result *eh.AppendResult) error {
	defer lockAndUnlock(&w.streamSubscribersMu)()

	subscriptionsState, found := w.streamSubscribers[result.Cursor.Stream().String()]
	if !found {
		// TODO: this blocks for a long while
		// TODO: logger
		var err error
		subscriptionsState, err = ehstreamsubscribers.LoadUntilRealtime(
			ctx,
			result.Cursor.Stream(),
			w.systemClient,
			logex.Prefix(ehstreamsubscribers.LogPrefix, w.logger))
		if err != nil {
			return err
		}

		w.streamSubscribers[result.Cursor.Stream().String()] = subscriptionsState
	}

	for _, subscriptionId := range subscriptionsState.State.Subscriptions() {
		// TODO: don't stop notifying on queue error
		if err := w.notifier.NotifySubscriberOfActivity(ctx, subscriptionId, *result); err != nil {
			return err
		}
	}

	return nil
}
