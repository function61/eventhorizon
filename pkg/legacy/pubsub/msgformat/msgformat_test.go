package msgformat

import (
	"fmt"
	"github.com/function61/gokit/assert"
	"testing"
)

func TestSerialize(t *testing.T) {
	assert.EqualString(t, Serialize(nil), "\n")
	assert.EqualString(t, Serialize([]string{}), "\n")
	assert.EqualString(t, Serialize([]string{"BYE"}), "BYE\n")
	assert.EqualString(t, Serialize([]string{"SUB", "foo"}), "SUB foo\n")
}

func TestDeserialize(t *testing.T) {
	assert.Assert(t, len(Deserialize("")) == 1)
	assert.EqualString(t, fmt.Sprintf("%v", Deserialize("\n")), "[]")

	assert.EqualString(t, fmt.Sprintf("%v", Deserialize("FOO\n")), "[FOO]")
	assert.Assert(t, len(Deserialize("SUB BAR\n")) == 2)
	assert.EqualString(t, fmt.Sprintf("%v", Deserialize("SUB BAR\n")), "[SUB BAR]")
}
