package metaevents

import (
	"github.com/function61/eventhorizon/util/ass"
	"testing"
)

func TestChildStreamCreated(t *testing.T) {
	metaType, _, event := Parse("/ChildStreamCreated {\"name\": \"/tenants/foo\", \"cursor\": \"/tenants/foo:0:0\", \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, metaType == ChildStreamCreatedId)

	childStreamCreated := event.(ChildStreamCreated)

	ass.EqualString(t, childStreamCreated.Name, "/tenants/foo")
	ass.EqualString(t, childStreamCreated.Cursor, "/tenants/foo:0:0")
	ass.EqualString(t, childStreamCreated.Timestamp, "2017-02-27T17:12:31.446Z")
}
