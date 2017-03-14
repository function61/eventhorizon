package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestSubscribed(t *testing.T) {
	isMeta, _, event := Parse(".Subscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, isMeta)

	subscribed := event.(Subscribed)

	ass.EqualString(t, subscribed.SubscriptionId, "6894605c-2a8e")
}
