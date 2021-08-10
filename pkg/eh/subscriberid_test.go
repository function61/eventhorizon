package eh

import (
	"testing"

	"github.com/function61/gokit/testing/assert"
)

func TestSubscriberIDToStream(t *testing.T) {
	assert.EqualString(t, NewSubscriberID("foo").StreamName().String(), "/_/sub/foo")
}

func TestSubscriberIDContainsSlash(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(string), `SubscriberID cannot contain '/'`)
	}()

	NewSubscriberID("foo/bar")
}

func TestSubscriberIDEmpty(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(string), "SubscriberID cannot be empty")
	}()

	NewSubscriberID("")
}
