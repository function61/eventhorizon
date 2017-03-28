package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestCreated(t *testing.T) {
	metaType, _, event := Parse("/Created {\"ts\":\"2017-02-27T17:12:31.446Z\"}")

	ass.True(t, metaType == CreatedId)

	created := event.(Created)

	ass.EqualString(t, created.Timestamp, "2017-02-27T17:12:31.446Z")
}
