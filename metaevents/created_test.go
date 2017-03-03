package metaevents

import (
	"testing"
)

func TestCreated(t *testing.T) {
	is, _, event := Parse(".Created {\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !is {
		t.Fatalf("Expecting is meta event")
	}

	created := event.(Created)

	EqualString(t, created.Timestamp, "2017-02-27T17:12:31.446Z")
}
