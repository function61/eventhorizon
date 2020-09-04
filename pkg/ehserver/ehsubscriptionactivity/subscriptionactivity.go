// Task for publishing stream changes to their subscribers as activity events
package ehsubscriptionactivity

import (
	"context"
	"log"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/system/ehstreammeta"
	"github.com/function61/eventhorizon/pkg/system/ehsubscription"
	"github.com/function61/eventhorizon/pkg/system/ehsubscriptiondomain"
	"github.com/function61/gokit/logex"
)

//go:generate genny -in ../../syncutilgen/concurrently.go -out concurrently.gen.go -pkg ehsubscriptionactivity gen ItemType=eh.Cursor,*subscriptionActivity

func PublishStreamChangesToSubscribers(
	ctx context.Context,
	discovered *DiscoveredMaxCursors,
	client *ehreader.SystemClient,
	logger *log.Logger,
) error {
	logl := logex.Levels(logger)

	subscriptionsActivity, err := groupStreamsChangesBySubscription(ctx, discovered, client, logl)
	if err != nil {
		return err
	}

	return publishActivityToSubscribers(ctx, subscriptionsActivity, client, logl)
}

// ["/@123", "/t-1/users/u-42@8"]
// => subscription "all" activity=["/@123", "/t-1/users/u-42@8"]
// => subscription "demo" activity=["/t-1/users/u-42@8"]
func groupStreamsChangesBySubscription(
	ctx context.Context,
	discovered *DiscoveredMaxCursors,
	client *ehreader.SystemClient,
	logl *logex.Leveled,
) (*allSubscriptionsActivities, error) {
	logl.Debug.Printf("got %d changed stream(s)", len(discovered.streams))

	allSubscriptionsActivity := newAllSubscriptionsActivities()

	// process each (non-subscription) cursor concurrently
	cursors := discovered.StreamsWithoutSubscriptionStreams()

	return allSubscriptionsActivity, concurrentlyEhCursorSlice(ctx, 3, cursors, func(ctx context.Context, cursor eh.Cursor) error {
		logl.Debug.Printf("resolving subscribers for %s", cursor.Serialize())

		streamMeta, err := ehstreammeta.LoadUntilRealtime(
			ctx,
			cursor.Stream(),
			client,
			ehstreammeta.GlobalCache,
			logl.Original)
		if err != nil {
			return err
		}

		for _, subscription := range streamMeta.State.Subscriptions() {
			allSubscriptionsActivity.UpsertSubscription(subscription).Append(cursor)
		}

		return nil
	})
}

func publishActivityToSubscribers(
	ctx context.Context,
	subscriptionsActivity *allSubscriptionsActivities,
	client *ehreader.SystemClient,
	logl *logex.Leveled,
) error {
	now := time.Now()

	return concurrentlySubscriptionActivitySlice(ctx, 3, subscriptionsActivity.Subscriptions(), func(ctx context.Context, subAct *subscriptionActivity) error {
		// for deduplication. since we're processing a large batch, errors mid-batch could yield
		// duplicate activity events upon re-processing
		subRecent, err := ehsubscription.LoadUntilRealtime(
			ctx,
			subAct.subscription,
			client,
			ehsubscription.GlobalCache,
			logl.Original)
		if err != nil {
			return err
		}

		return subRecent.Reader.TransactWrite(ctx, func() error {
			dedupedCursors := []eh.Cursor{}

			for _, cur := range subAct.cursors {
				if !subRecent.State.RecentlySeen(cur) {
					dedupedCursors = append(dedupedCursors, cur)
				}
			}

			logl.Info.Printf(
				"sub %s new cursors: %d",
				subAct.subscription.String(),
				len(dedupedCursors))

			if len(dedupedCursors) == 0 {
				return nil
			}

			return client.AppendAfter(
				ctx,
				subRecent.State.Version(),
				ehsubscriptiondomain.NewSubscriptionActivity(dedupedCursors, ehevent.MetaSystemUser(now)))
		})
	})
}
