package metaevents

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestSubscribed(t *testing.T) {
	metaType, _, event := Parse("/Subscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	assert.Assert(t, metaType == SubscribedId)

	subscribed := event.(Subscribed)

	assert.EqualString(t, subscribed.SubscriptionId, "6894605c-2a8e")
}
