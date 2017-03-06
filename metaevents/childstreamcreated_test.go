package metaevents

import (
	"testing"
)

func TestChildStreamCreated(t *testing.T) {
	isMeta, _, event := Parse(".ChildStreamCreated {\"child_stream\": \"/tenants/foo:0:0\", \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	childStreamCreated := event.(ChildStreamCreated)

	EqualString(t, childStreamCreated.ChildStream, "/tenants/foo:0:0")
	EqualString(t, childStreamCreated.Timestamp, "2017-02-27T17:12:31.446Z")
}
