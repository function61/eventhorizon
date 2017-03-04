package metaevents

import (
	"testing"
)

func TestRotated(t *testing.T) {
	isMeta, line, event := Parse(".Rotated {\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	rotated := event.(Rotated)

	EqualString(t, rotated.Next, "/tenants/foo:1:0:127.0.0.1")
	EqualString(t, rotated.Timestamp, "2017-03-03T19:33:49.709Z")

	EqualString(t, line, ".Rotated {\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")
}
