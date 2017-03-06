package writer

import (
	"github.com/boltdb/bolt"
	"strings"
)

func getSubscriptionsForStream(streamName string, tx *bolt.Tx) []string {
	streamSubscriptionsBucket, err := tx.CreateBucketIfNotExists([]byte("_streamsubscriptions"))
	if err != nil {
		panic(err)
	}

	subscriptionsSerialized := streamSubscriptionsBucket.Get([]byte(streamName))

	if subscriptionsSerialized == nil {
		return []string{}
	}

	return strings.Split(string(subscriptionsSerialized), ",")
}

func saveSubscriptionsForStream(streamName string, subscriptions []string, tx *bolt.Tx) error {
	streamSubscriptionsBucket, err := tx.CreateBucketIfNotExists([]byte("_streamsubscriptions"))
	if err != nil {
		return err
	}

	// when empty, don't store empty string because that would:
	// 1) consume unnecessary space 2) yield [""] (len=1) when deserializing
	if len(subscriptions) == 0 {
		return streamSubscriptionsBucket.Delete([]byte(streamName))
	}

	subscriptionsSerialized := strings.Join(subscriptions, ",")

	return streamSubscriptionsBucket.Put([]byte(streamName), []byte(subscriptionsSerialized))
}
