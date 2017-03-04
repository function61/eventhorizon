package metaevents

import (
	"testing"
)

func TestCreated(t *testing.T) {
	isMeta, _, event := Parse(".Created {\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	created := event.(Created)

	EqualString(t, created.Timestamp, "2017-02-27T17:12:31.446Z")
}
