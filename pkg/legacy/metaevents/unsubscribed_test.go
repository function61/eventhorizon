package metaevents

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestUnsubscribed(t *testing.T) {
	metaType, _, event := Parse("/Unsubscribed {\"subscription_id\":\"6894605c-2a8e\",\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	assert.Assert(t, metaType == UnsubscribedId)

	unsubscribed := event.(Unsubscribed)

	assert.EqualString(t, unsubscribed.SubscriptionId, "6894605c-2a8e")
}
