package ehevent

import (
	"testing"
	"time"

	"github.com/function61/gokit/assert"
)

var (
	testEvent = NewCredentialCreated("123", Meta(time.Date(2020, 8, 20, 8, 55, 0, 123456789, time.UTC), "u987"))
)

func TestSerializationFormat(t *testing.T) {
	assert.EqualString(t, SerializeOne(testEvent), `{"_":"credential.Created","t":"2020-08-20T08:55:00.123Z","u":"u987"} {"Id":"123"}`)
}

func TestDeserialize(t *testing.T) {
	o, err := Deserialize(SerializeOne(testEvent), testEventTypes)
	assert.Ok(t, err)

	assert.EqualString(t, o.(*CredentialCreated).Id, "123")
	assert.EqualString(t, o.MetaType(), "credential.Created")
	assert.EqualString(t, o.Meta().UserIdOrEmptyIfSystem(), "u987")
}

// structure for test event

var testEventTypes = Types{
	"credential.Created": func() Event { return &CredentialCreated{} },
}

// ------

type CredentialCreated struct {
	meta EventMeta
	Id   string
}

func (e *CredentialCreated) MetaType() string { return "credential.Created" }
func (e *CredentialCreated) Meta() *EventMeta { return &e.meta }

func NewCredentialCreated(
	id string,
	meta EventMeta,
) *CredentialCreated {
	return &CredentialCreated{
		meta: meta,
		Id:   id,
	}
}
