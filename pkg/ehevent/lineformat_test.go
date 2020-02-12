package ehevent

import (
	"github.com/function61/gokit/assert"
	"testing"
	"time"
)

func TestSerializeAndDeserialize(t *testing.T) {
	serialized := Serialize(NewTestEvent(
		"/_sub",
		MetaSystemUser(time.Date(2020, 1, 30, 12, 2, 0, 0, time.UTC))))

	assert.EqualString(t, serialized, `2020-01-30T12:02:00.000Z TestEvent    {"Name":"/_sub"}`)

	e, err := Deserialize(serialized, testEventAllocators)
	assert.Ok(t, err)

	eCreated := e.(*TestEvent)
	meta := e.Meta()

	assert.EqualString(t, eCreated.Name, "/_sub")
	assert.EqualString(t, meta.UserId, "")
	assert.EqualString(t, meta.ImpersonatingUserId, "")
	assert.EqualString(t, meta.Timestamp.Format(rfc3339Milli), "2020-01-30T12:02:00.000Z")
	assert.Assert(t, meta.TimestampOfRecording.IsZero())
}

var testEventAllocators = Allocators{
	"TestEvent": func() Event { return &TestEvent{} },
}

type TestEvent struct {
	meta EventMeta
	Name string
}

func (e *TestEvent) MetaType() string { return "TestEvent" }
func (e *TestEvent) Meta() *EventMeta { return &e.meta }

func NewTestEvent(name string, meta EventMeta) *TestEvent {
	return &TestEvent{meta, name}
}
