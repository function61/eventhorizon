package msgformat

import (
	"fmt"
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestSerialize(t *testing.T) {
	ass.EqualString(t, Serialize(nil), "\n")
	ass.EqualString(t, Serialize([]string{}), "\n")
	ass.EqualString(t, Serialize([]string{"BYE"}), "BYE\n")
	ass.EqualString(t, Serialize([]string{"SUB", "foo"}), "SUB foo\n")
}

func TestDeserialize(t *testing.T) {
	ass.EqualInt(t, len(Deserialize("")), 1)
	ass.EqualString(t, fmt.Sprintf("%v", Deserialize("\n")), "[]")

	ass.EqualString(t, fmt.Sprintf("%v", Deserialize("FOO\n")), "[FOO]")
	ass.EqualInt(t, len(Deserialize("SUB BAR\n")), 2)
	ass.EqualString(t, fmt.Sprintf("%v", Deserialize("SUB BAR\n")), "[SUB BAR]")
}
