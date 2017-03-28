package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestEncodeRegularLine(t *testing.T) {
	// empty line
	ass.EqualString(t, EncodeRegularLine(""), " ")

	// regular line with normal chars
	ass.EqualString(t, EncodeRegularLine("a"), " a")
	ass.EqualString(t, EncodeRegularLine("foobar"), " foobar")
}

func TestUnknownMeta(t *testing.T) {
	metaType, line, event := Parse("/poop {\"foo\": \"bar\"}")

	ass.True(t, metaType == "poop")

	_, castSucceeded := event.(Rotated)

	ass.False(t, castSucceeded)
	ass.EqualString(t, line, "{\"foo\": \"bar\"}")
}

func TestRegularText(t *testing.T) {
	metaType, line, _ := Parse(" foobar")

	ass.True(t, metaType == "")
	ass.EqualString(t, line, "foobar")
}

func TestEmptyLine(t *testing.T) {
	defer func() {
		ass.EqualString(t, recover().(error).Error(), errorEmptyLine.Error())
	}()

	Parse("")
}

func TestInvalidMetaLine(t *testing.T) {
	defer func() {
		ass.EqualString(t, recover().(error).Error(), "Unable to parse meta line: /fooMissingPayload")
	}()

	Parse("/fooMissingPayload")
}

func TestUnknownType(t *testing.T) {
	defer func() {
		ass.EqualString(t, recover().(error).Error(), errorUnknownType.Error())
	}()

	Parse("yes oh hai")
}
