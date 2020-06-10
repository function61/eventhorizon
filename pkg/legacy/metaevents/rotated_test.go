package metaevents

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestRotated(t *testing.T) {
	metaType, line, event := Parse("/Rotated {\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")

	assert.Assert(t, metaType == RotatedId)

	rotated := event.(Rotated)

	assert.EqualString(t, rotated.Next, "/tenants/foo:1:0:127.0.0.1")
	assert.EqualString(t, rotated.Timestamp, "2017-03-03T19:33:49.709Z")

	assert.EqualString(t, line, "{\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")
}
