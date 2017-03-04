package metaevents

import (
	"testing"
)

func TestUnsubscribed(t *testing.T) {
	isMeta, _, event := Parse(".Unsubscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	unsubscribed := event.(Unsubscribed)

	EqualString(t, unsubscribed.SubscriptionId, "6894605c-2a8e")
}
