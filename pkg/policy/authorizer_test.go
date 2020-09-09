package policy

import (
	"strings"
	"testing"

	"github.com/function61/gokit/testing/assert"
)

var (
	eventHorizonRn = F61rn.Child("eventhorizon") // to prevent dependency
)

func TestAuthorize(t *testing.T) {
	policy := testingPolicy()

	act := func(code string) Action {
		return NewAction(code)
	}

	disallowed := func(err error) {
		t.Helper()

		assert.Assert(t, err != nil)
		assert.Assert(t, strings.Contains(err.Error(), " implicitly denied to "))
	}

	assert.Ok(t, policy.Authorize(act("eventhorizon:Read"), eventHorizonRn.Child("/_system")))

	// resource was allowed, but different action must not
	disallowed(policy.Authorize(act("eventhorizon:Write"), eventHorizonRn.Child("/_system")))

	// wildcard should match this ..
	assert.Ok(t, policy.Authorize(act("eventhorizon:Read"), eventHorizonRn.Child("/_system/foo")))

	// .. but not this
	disallowed(policy.Authorize(act("eventhorizon:Read"), eventHorizonRn.Child("/_sys")))
}
