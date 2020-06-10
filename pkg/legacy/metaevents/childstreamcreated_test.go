package metaevents

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestChildStreamCreated(t *testing.T) {
	metaType, _, event := Parse("/ChildStreamCreated {\"name\": \"/tenants/foo\", \"cursor\": \"/tenants/foo:0:0\", \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	assert.Assert(t, metaType == ChildStreamCreatedId)

	childStreamCreated := event.(ChildStreamCreated)

	assert.EqualString(t, childStreamCreated.Name, "/tenants/foo")
	assert.EqualString(t, childStreamCreated.Cursor, "/tenants/foo:0:0")
	assert.EqualString(t, childStreamCreated.Timestamp, "2017-02-27T17:12:31.446Z")
}
