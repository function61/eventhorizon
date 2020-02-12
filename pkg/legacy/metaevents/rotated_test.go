package metaevents

import (
	"github.com/function61/eventhorizon/pkg/legacy/util/ass"
	"testing"
)

func TestRotated(t *testing.T) {
	metaType, line, event := Parse("/Rotated {\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")

	ass.True(t, metaType == RotatedId)

	rotated := event.(Rotated)

	ass.EqualString(t, rotated.Next, "/tenants/foo:1:0:127.0.0.1")
	ass.EqualString(t, rotated.Timestamp, "2017-03-03T19:33:49.709Z")

	ass.EqualString(t, line, "{\"next\":\"/tenants/foo:1:0:127.0.0.1\",\"ts\":\"2017-03-03T19:33:49.709Z\"}")
}
