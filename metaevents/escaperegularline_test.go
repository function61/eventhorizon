package metaevents

import (
	"testing"
)

func TestEscapeRegularLine(t *testing.T) {
	// empty line
	EqualString(t, EscapeRegularLine(""), "")

	// regular line with normal chars
	EqualString(t, EscapeRegularLine("a"), "a")
	EqualString(t, EscapeRegularLine("foobar"), "foobar")

	// leading . has to be escaped
	EqualString(t, EscapeRegularLine(".foo."), "\\.foo.")

	// so does \
	EqualString(t, EscapeRegularLine("\\foo\\"), "\\\\foo\\")
}
