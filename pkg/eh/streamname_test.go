package eh

import (
	"testing"

	"github.com/function61/gokit/assert"
)

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
