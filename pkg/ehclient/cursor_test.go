package ehclient

import (
	"testing"

	"github.com/function61/gokit/assert"
)

var (
	example = At("/foo", 314)
)

func TestStream(t *testing.T) {
	assert.EqualString(t, example.Stream(), "/foo")
}

func TestVersion(t *testing.T) {
	assert.Assert(t, example.Version() == 314)
}

func TestAtBeginning(t *testing.T) {
	is := Beginning("/foo")

	assert.Assert(t, is.AtBeginning())
	assert.Assert(t, !example.AtBeginning())
}

func TestLess(t *testing.T) {
	v1 := At("/foo", 1)
	v2 := At("/foo", 2)
	v2Other := At("/bar", 2)

	assert.Assert(t, v1.Less(v2))
	assert.Assert(t, !v2.Less(v1))
	assert.Assert(t, !v2.Less(v2))

	defer func() {
		assert.EqualString(t, recover().(string), "cannot compare unrelated streams")
	}()

	v2.Less(v2Other) // panics
}

func TestEqual(t *testing.T) {
	v1 := At("/foo", 1)
	v2 := At("/foo", 2)
	v2Other := At("/bar", 2)

	assert.Assert(t, v1.Equal(v1))
	assert.Assert(t, !v1.Equal(v2))
	assert.Assert(t, !v2.Equal(v2Other))
}

func TestNext(t *testing.T) {
	next := example.Next()

	assert.Assert(t, next.Version() == 315)
}

func TestSerialize(t *testing.T) {
	// also tests At() indirectly
	assert.EqualString(t, example.Serialize(), "/foo@314")
}

func TestBeginning(t *testing.T) {
	beg := Beginning("/foo")
	assert.EqualString(t, beg.Serialize(), "/foo@-1")
	assert.Assert(t, beg.Equal(At("/foo", -1)))
}
