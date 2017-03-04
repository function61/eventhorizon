package metaevents

import (
	"testing"
)

func TestAuthorityChanged(t *testing.T) {
	isMeta, _, event := Parse(".AuthorityChanged {\"peers\":[\"127.0.0.1\"],\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	if !isMeta {
		t.Fatalf("Expecting is meta event")
	}

	authorityChanged := event.(AuthorityChanged)

	EqualString(t, authorityChanged.Timestamp, "2017-02-27T17:12:31.446Z")
}
