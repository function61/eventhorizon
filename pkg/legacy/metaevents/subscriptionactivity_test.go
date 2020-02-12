package metaevents

import (
	"github.com/function61/gokit/assert"
	"strings"
	"testing"
)

func TestSubscriptionActivity(t *testing.T) {
	metaType, _, event := Parse("/SubscriptionActivity {\"activity\": [\"/tenants/foo:0:700\", \"/tenants/bar:245:14\"], \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	assert.Assert(t, metaType == SubscriptionActivityId)

	subscriptionActivity := event.(SubscriptionActivity)

	assert.Assert(t, len(subscriptionActivity.Activity) == 2)
	assert.EqualString(t, strings.Join(subscriptionActivity.Activity, " | "), "/tenants/foo:0:700 | /tenants/bar:245:14")
	assert.EqualString(t, subscriptionActivity.Timestamp, "2017-02-27T17:12:31.446Z")
}
