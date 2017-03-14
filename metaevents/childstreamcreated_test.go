package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestChildStreamCreated(t *testing.T) {
	isMeta, _, event := Parse(".ChildStreamCreated {\"child_stream\": \"/tenants/foo:0:0\", \"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, isMeta)

	childStreamCreated := event.(ChildStreamCreated)

	ass.EqualString(t, childStreamCreated.ChildStream, "/tenants/foo:0:0")
	ass.EqualString(t, childStreamCreated.Timestamp, "2017-02-27T17:12:31.446Z")
}
