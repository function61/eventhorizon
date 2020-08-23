package policy

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestResourceName(t *testing.T) {
	iam := F61rn.Child("iam")
	iamUser := iam.Child("user3")

	assert.EqualString(t, F61rn.String(), "f61rn")
	assert.EqualString(t, iam.String(), "f61rn:iam")
	assert.EqualString(t, iamUser.String(), "f61rn:iam:user3")
}
