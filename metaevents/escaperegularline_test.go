package metaevents

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestEscapeRegularLine(t *testing.T) {
	// empty line
	ass.EqualString(t, EscapeRegularLine(""), "")

	// regular line with normal chars
	ass.EqualString(t, EscapeRegularLine("a"), "a")
	ass.EqualString(t, EscapeRegularLine("foobar"), "foobar")

	// leading . has to be escaped
	ass.EqualString(t, EscapeRegularLine(".foo."), "\\.foo.")

	// so does \
	ass.EqualString(t, EscapeRegularLine("\\foo\\"), "\\\\foo\\")
}
