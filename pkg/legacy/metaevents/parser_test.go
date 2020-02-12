package metaevents

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestEncodeRegularLine(t *testing.T) {
	// empty line
	assert.EqualString(t, EncodeRegularLine(""), " ")

	// regular line with normal chars
	assert.EqualString(t, EncodeRegularLine("a"), " a")
	assert.EqualString(t, EncodeRegularLine("foobar"), " foobar")
}

func TestUnknownMeta(t *testing.T) {
	metaType, line, event := Parse("/poop {\"foo\": \"bar\"}")

	assert.Assert(t, metaType == "poop")

	_, castSucceeded := event.(Rotated)

	assert.Assert(t, !castSucceeded)
	assert.EqualString(t, line, "{\"foo\": \"bar\"}")
}

func TestRegularText(t *testing.T) {
	metaType, line, _ := Parse(" foobar")

	assert.Assert(t, metaType == "")
	assert.EqualString(t, line, "foobar")
}

func TestEmptyLine(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(error).Error(), errorEmptyLine.Error())
	}()

	Parse("")
}

func TestInvalidMetaLine(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(error).Error(), "Unable to parse meta line: /fooMissingPayload")
	}()

	Parse("/fooMissingPayload")
}

func TestUnknownType(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(error).Error(), errorUnknownType.Error())
	}()

	Parse("yes oh hai")
}
