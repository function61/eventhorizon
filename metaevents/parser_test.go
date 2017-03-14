package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestUnknownMeta(t *testing.T) {
	isMeta, line, event := Parse(".poop {\"foo\": \"bar\"}")

	ass.True(t, isMeta)

	_, castSucceeded := event.(Rotated)

	ass.False(t, castSucceeded)
	ass.EqualString(t, line, ".poop {\"foo\": \"bar\"}")
}

func TestRegularText(t *testing.T) {
	isMeta, line, _ := Parse("foobar")

	ass.False(t, isMeta)
	ass.EqualString(t, line, "foobar")
}

func TestEmptyLine(t *testing.T) {
	isMeta, line, _ := Parse("")

	ass.False(t, isMeta)
	ass.EqualString(t, line, "")
}

func TestDotEscapedRegularLine(t *testing.T) {
	isMeta, line, _ := Parse("\\.Rotated")

	ass.False(t, isMeta)
	ass.EqualString(t, line, ".Rotated")
}

func TestBackslashEscapedRegularLine(t *testing.T) {
	isMeta, line, _ := Parse("\\\\foo")

	ass.False(t, isMeta)
	ass.EqualString(t, line, "\\foo")
}

func TestNotMetaEvent(t *testing.T) {
	isMeta, line, _ := Parse("yes oh hai")

	ass.False(t, isMeta)
	ass.EqualString(t, line, "yes oh hai")
}
