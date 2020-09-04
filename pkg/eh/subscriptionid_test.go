package eh

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestSubscriptionIdToStream(t *testing.T) {
	assert.EqualString(t, NewSubscriptionId("foo").StreamName().String(), "/_/sub/foo")
}

func TestSubscriptionIdContainsSlash(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(string), `SubscriptionId cannot contain '/'`)
	}()

	NewSubscriptionId("foo/bar")
}

func TestSubscriptionIdEmpty(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(string), "SubscriptionId cannot be empty")
	}()

	NewSubscriptionId("")
}
