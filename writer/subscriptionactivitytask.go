package writer

import (
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/writer/transaction"
	"log"
	"time"
)

// - Every 5 seconds
// - For each stream that have new events
// - See which subscribers have subscribed to changes for those streams
// - Deliver notification as a SubscriptionActivity meta event to the respective
//   subscribers streams (implemented as regular streams).
// - All activity for the tracked streams are aggregated in 5 second intervals and all it does
//   is contain the latest offset, so no perf difference in 1 or millions of events/sec.
//
// Therefore, a subscriber can just listen to its own stream to get aggregate
// notifications for all the streams it its following, whether it's 1 or millions of streams.
//
// This design does not impose 5 second delay on events because the pub/sub subsystem delivers
// the change notifications in realtime, so in practice all subscribed events are delivered instantly.
//
// But the pub/sub system does not guarantee delivery (connection problem or at-times-offline subscribers),
// so we need this mechanism to guarantee that all events will be delivered when subscriber comes back online.

type SubscriptionActivityTask struct {
	writer                   *EventstoreWriter
	subscriptionActivityStop chan bool
	subscriptionActivityDone chan bool
}

func NewSubscriptionActivityTask(writer *EventstoreWriter) *SubscriptionActivityTask {
	t := &SubscriptionActivityTask{
		writer: writer,
		subscriptionActivityStop: make(chan bool),
		subscriptionActivityDone: make(chan bool),
	}

	go t.loopUntilStopped()

	return t
}

func (t *SubscriptionActivityTask) MarkOneDirty(cursorAfter *cursor.Cursor, tx *transaction.EventstoreTransaction) error {
	dirtyStreamsBucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte("_dirtystreams"))
	if err != nil {
		return err
	}

	if err := dirtyStreamsBucket.Put([]byte(cursorAfter.Stream), []byte(cursorAfter.Serialize())); err != nil {
		return err
	}

	return nil
}

func (t *SubscriptionActivityTask) loopUntilStopped() {
	for {
		select {
		case <-t.subscriptionActivityStop:
			t.subscriptionActivityDone <- true
			return
		case <-time.After(5 * time.Second):
			break
		}

		t.writer.mu.Lock()

		tx := transaction.NewEventstoreTransaction(t.writer.database)

		err := t.writer.database.Update(func(boltTx *bolt.Tx) error {
			tx.BoltTx = boltTx

			return t.broadcastSubscriptionActivities(tx)
		})
		if err != nil {
			panic(err)
		}

		if err := t.writer.applySideEffects(tx); err != nil {
			panic(err)
		}

		t.writer.mu.Unlock()
	}
}

func (t *SubscriptionActivityTask) broadcastSubscriptionActivities(tx *transaction.EventstoreTransaction) error {
	dirtyStreamsBucket, err := tx.BoltTx.CreateBucketIfNotExists([]byte("_dirtystreams"))
	if err != nil {
		return err
	}

	activityBySubscription := make(map[string]*metaevents.SubscriptionActivity)

	dirtyStreamsBucket.ForEach(func(dirtyStream []byte, latestCursorSerialized []byte) error {
		subscriptions := getSubscriptionsForStream(string(dirtyStream), tx.BoltTx)

		for _, subscription := range subscriptions {
			if _, exists := activityBySubscription[subscription]; !exists {
				activityBySubscription[subscription] = metaevents.NewSubscriptionActivity()
			}

			// append() by itself does not guarantee that each stream is mentioned
			// only once (a must for SubscriptionActivity event), but the above
			// ForEach() iterates over unique streams, so we're good
			activityBySubscription[subscription].Activity = append(
				activityBySubscription[subscription].Activity,
				string(latestCursorSerialized))
		}

		if err := dirtyStreamsBucket.Delete(dirtyStream); err != nil {
			panic(err) // cannot delete a key we just found?
		}

		return nil
	})

	for subscription, subscriptionActivityEvent := range activityBySubscription {
		log.Printf("SubscriptionActivityTask: %s: %v", subscription, subscriptionActivityEvent.Activity)

		t.writer.metrics.SubscriptionActivityEventsRaised.Inc()

		// FIXME: this will fail all subscriptions if even one subscription stream is deleted later.
		//        automatically unsubscribe if subscription stream does not exist?

		if err := t.writer.appendToStreamInternal(subscription, nil, subscriptionActivityEvent.Serialize(), tx); err != nil {
			return err
		}
	}

	return nil
}

func (t *SubscriptionActivityTask) Close() {
	log.Printf("SubscriptionActivityTask: stopping")

	t.subscriptionActivityStop <- true

	<-t.subscriptionActivityDone

	log.Printf("SubscriptionActivityTask: stopped")
}
