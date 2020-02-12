package lineformatsimple

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestParse(t *testing.T) {
	eventType, payload, err := Parse("FooEvent {\"pi\": 3.14}")

	assert.Assert(t, err == nil)
	assert.EqualString(t, eventType, "FooEvent")
	assert.EqualString(t, payload, "{\"pi\": 3.14}")
}

func TestParseFails(t *testing.T) {
	_, _, err := Parse("FooEvent invalid {\"pi\": 3.14}")

	assert.EqualString(t, err.Error(), "Unable to parse line: FooEvent invalid {\"pi\": 3.14}")
}
