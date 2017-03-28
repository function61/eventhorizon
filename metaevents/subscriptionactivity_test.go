package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"strings"
	"testing"
)

func TestSubscriptionActivity(t *testing.T) {
	metaType, _, event := Parse("/SubscriptionActivity {\"activity\": [\"/tenants/foo:0:700\", \"/tenants/bar:245:14\"], \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, metaType == SubscriptionActivityId)

	subscriptionActivity := event.(SubscriptionActivity)

	ass.EqualInt(t, len(subscriptionActivity.Activity), 2)
	ass.EqualString(t, strings.Join(subscriptionActivity.Activity, " | "), "/tenants/foo:0:700 | /tenants/bar:245:14")
	ass.EqualString(t, subscriptionActivity.Timestamp, "2017-02-27T17:12:31.446Z")
}
