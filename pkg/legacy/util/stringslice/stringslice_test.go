package stringslice

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestItemIndex(t *testing.T) {
	collection := []string{"foo", "bar", "baz"}

	assert.Assert(t, ItemIndex("foo", collection) == 0)
	assert.Assert(t, ItemIndex("baz", collection) == 2)
	assert.Assert(t, ItemIndex("bar", collection) == 1)

	assert.Assert(t, ItemIndex("", collection) == -1)
	assert.Assert(t, ItemIndex("fasdf", collection) == -1)
}
