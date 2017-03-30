package lineformatsimple

import (
	"github.com/function61/eventhorizon/util/ass"
	"testing"
)

func TestParse(t *testing.T) {
	eventType, payload, err := Parse("FooEvent {\"pi\": 3.14}")

	ass.True(t, err == nil)
	ass.EqualString(t, eventType, "FooEvent")
	ass.EqualString(t, payload, "{\"pi\": 3.14}")
}

func TestParseFails(t *testing.T) {
	_, _, err := Parse("FooEvent invalid {\"pi\": 3.14}")

	ass.EqualString(t, err.Error(), "Unable to parse line: FooEvent invalid {\"pi\": 3.14}")
}
