package metaevents

import (
	"strings"
	"testing"
)

func TestSubscriptionActivity(t *testing.T) {
	isMeta, _, event := Parse(".SubscriptionActivity {\"activity\": [\"/tenants/foo:0:700\", \"/tenants/bar:245:14\"], \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	subscriptionActivity := event.(SubscriptionActivity)

	EqualInt(t, len(subscriptionActivity.Activity), 2)
	EqualString(t, strings.Join(subscriptionActivity.Activity, " | "), "/tenants/foo:0:700 | /tenants/bar:245:14")
	EqualString(t, subscriptionActivity.Timestamp, "2017-02-27T17:12:31.446Z")
}
