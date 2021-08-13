package policy

import (
	"testing"

	"github.com/function61/gokit/testing/assert"
)

func TestResourceName(t *testing.T) {
	iam := F61.Child("iam")
	iamUser := iam.Child("user3")

	assert.EqualString(t, F61.String(), "f61")
	assert.EqualString(t, iam.String(), "f61:iam")
	assert.EqualString(t, iamUser.String(), "f61:iam:user3")
}
