package eh

import (
	"testing"

	"github.com/function61/gokit/assert"
)

var (
	foo   = RootName.Child("foo")
	fooAt = foo.At(314)
)

func TestStream(t *testing.T) {
	assert.EqualString(t, fooAt.Stream().String(), "/foo")
}

func TestVersion(t *testing.T) {
	assert.Assert(t, fooAt.Version() == 314)
}

func TestAtBeginning(t *testing.T) {
	is := foo.Beginning()

	assert.Assert(t, is.AtBeginning())
	assert.Assert(t, !fooAt.AtBeginning())
}

func TestLess(t *testing.T) {
	v1 := foo.At(1)
	v2 := foo.At(2)
	v2Other := RootName.Child("bar").At(2)

	assert.Assert(t, v1.Less(v2))
	assert.Assert(t, !v2.Less(v1))
	assert.Assert(t, !v2.Less(v2))

	defer func() {
		assert.EqualString(t, recover().(string), "cannot compare unrelated streams")
	}()

	v2.Less(v2Other) // panics
}

func TestEqual(t *testing.T) {
	v1 := foo.At(1)
	v2 := foo.At(2)
	v2Other := RootName.Child("bar").At(2)

	assert.Assert(t, v1.Equal(v1))
	assert.Assert(t, !v1.Equal(v2))
	assert.Assert(t, !v2.Equal(v2Other))
}

func TestNext(t *testing.T) {
	next := fooAt.Next()

	assert.Assert(t, next.Version() == 315)
}

func TestSerialize(t *testing.T) {
	// also tests At() indirectly
	assert.EqualString(t, fooAt.Serialize(), "/foo@314")
}

func TestBeginning(t *testing.T) {
	beg := foo.Beginning()
	assert.EqualString(t, beg.Serialize(), "/foo@-1")
	assert.Assert(t, beg.Equal(foo.At(-1)))
}

func TestStreamName(t *testing.T) {
	assert.Assert(t, RootName.Parent() == nil)

	assert.EqualString(t, RootName.ResourceName().String(), "f61rn:eventhorizon:stream:/")

	uid3 := RootName.Child("t-3").Child("users").Child("uid3")

	assert.EqualString(t, uid3.String(), "/t-3/users/uid3")
	assert.EqualString(t, uid3.Parent().String(), "/t-3/users")
	assert.EqualString(t, uid3.Parent().Parent().String(), "/t-3")
	assert.EqualString(t, uid3.Parent().Parent().Parent().String(), "/")
	assert.Assert(t, uid3.Parent().Parent().Parent().Parent() == nil)
}

func TestStreamNameIsUnder(t *testing.T) {
	uid3 := RootName.Child("t-3").Child("users").Child("uid3")

	// everything's under root name
	assert.Assert(t, uid3.IsUnder(RootName))
	assert.Assert(t, uid3.IsUnder(RootName.Child("t-3")))
	assert.Assert(t, uid3.IsUnder(RootName.Child("t-3").Child("users")))
	assert.Assert(t, !uid3.IsUnder(RootName.Child("t-3").Child("usErs")))
	assert.Assert(t, !uid3.IsUnder(RootName.Child("t-4")))
	assert.Assert(t, !uid3.IsUnder(RootName.Child("t-4").Child("users")))
}
