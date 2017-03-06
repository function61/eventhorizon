package writer

import (
	"github.com/boltdb/bolt"
	"github.com/function61/eventhorizon/metaevents"
	"github.com/function61/eventhorizon/writer/transaction"
	"log"
	"time"
)

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

func (t *SubscriptionActivityTask) loopUntilStopped() {
	for {
		select {
		case <-t.subscriptionActivityStop:
			t.subscriptionActivityDone <- true
			return
			break
		case <-time.After(5 * time.Second):
			break
		}

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

			activityBySubscription[subscription].Activity[string(dirtyStream)] = string(latestCursorSerialized)
		}

		if err := dirtyStreamsBucket.Delete(dirtyStream); err != nil {
			panic(err) // cannot delete a key we just found?
		}

		return nil
	})

	for subscription, subscriptionActivityEvent := range activityBySubscription {
		log.Printf("SubscriptionActivityTask: %s: %v", subscription, subscriptionActivityEvent.Activity)

		// FIXME: this will fail all subscriptions if even one subscription stream is deleted later.
		//        automatically unsubscribe if subscription stream does not exist?

		if err := t.writer.appendToStreamInternal(subscriptionStreamPath(subscription), nil, subscriptionActivityEvent.Serialize(), tx); err != nil {
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
