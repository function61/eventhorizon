package metaevents

import (
	"github.com/function61/eventhorizon/pkg/legacy/util/ass"
	"testing"
)

func TestSubscribed(t *testing.T) {
	metaType, _, event := Parse("/Subscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, metaType == SubscribedId)

	subscribed := event.(Subscribed)

	ass.EqualString(t, subscribed.SubscriptionId, "6894605c-2a8e")
}
