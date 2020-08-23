package ehevent

import (
	"testing"
	"time"

	"github.com/function61/gokit/assert"
)

var (
	tenOClock    = time.Date(2020, 10, 0, 0, 0, 0, 0, time.UTC)
	elevenOClock = time.Date(2020, 11, 0, 0, 0, 0, 0, time.UTC)
)

func TestBackdate(t *testing.T) {
	normal := Meta(elevenOClock, "u987")
	withBackdate := MetaBackdate(tenOClock, elevenOClock, "u987")

	assert.Assert(t, normal.Time().Equal(elevenOClock))
	assert.Assert(t, normal.TimeOfRecording().Equal(elevenOClock))

	assert.Assert(t, withBackdate.Time().Equal(tenOClock))
	assert.Assert(t, withBackdate.TimeOfRecording().Equal(elevenOClock))
}

func TestBackdateSystemUser(t *testing.T) {
	normal := MetaSystemUser(elevenOClock)
	withBackdate := MetaSystemUserBackdate(tenOClock, elevenOClock)

	assert.Assert(t, normal.Time().Equal(elevenOClock))
	assert.Assert(t, normal.TimeOfRecording().Equal(elevenOClock))

	assert.Assert(t, withBackdate.Time().Equal(tenOClock))
	assert.Assert(t, withBackdate.TimeOfRecording().Equal(elevenOClock))
}

func TestTimestampRounding(t *testing.T) {
	mk := func(ns int) string {
		meta := MetaSystemUser(time.Date(2020, 10, 0, 0, 0, 0, ns, time.UTC))
		return meta.Time().Format(".999999999")
	}

	assert.EqualString(t, mk(123456789), ".123")
	assert.EqualString(t, mk(123500000), ".124")
	assert.EqualString(t, mk(12350000), ".012")
	assert.EqualString(t, mk(1235000), ".001")
	assert.EqualString(t, mk(123500), "")
	assert.EqualString(t, mk(499000), "")
	assert.EqualString(t, mk(500000), ".001")
	assert.EqualString(t, mk(0), "")
}
