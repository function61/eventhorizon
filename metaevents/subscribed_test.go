package metaevents

import (
	"testing"
)

func TestSubscribed(t *testing.T) {
	is, _, event := Parse(".Subscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !is {
		t.Fatalf("Expecting is meta event")
	}

	subscribed := event.(Subscribed)

	EqualString(t, subscribed.SubscriptionId, "6894605c-2a8e")
}
