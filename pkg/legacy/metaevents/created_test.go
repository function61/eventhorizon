package metaevents

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestCreated(t *testing.T) {
	metaType, _, event := Parse("/Created {\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	assert.Assert(t, metaType == CreatedId)

	created := event.(Created)

	assert.EqualString(t, created.Timestamp, "2017-02-27T17:12:31.446Z")
}
