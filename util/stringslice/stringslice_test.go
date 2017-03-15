package stringslice

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestItemIndex(t *testing.T) {
	collection := []string{"foo", "bar", "baz"}

	ass.EqualInt(t, ItemIndex("foo", collection), 0)
	ass.EqualInt(t, ItemIndex("baz", collection), 2)
	ass.EqualInt(t, ItemIndex("bar", collection), 1)

	ass.EqualInt(t, ItemIndex("", collection), -1)
	ass.EqualInt(t, ItemIndex("fasdf", collection), -1)
}
