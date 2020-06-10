package writer

import (
	"testing"

	"github.com/function61/gokit/assert"
)

func TestParentStreamName(t *testing.T) {
	assert.EqualString(t, parentStreamName("/tenants/foo"), "/tenants")
	assert.EqualString(t, parentStreamName("/tenants"), "/")
	assert.EqualString(t, parentStreamName("/"), "/")
}

func TestStringArrayToRawLines(t *testing.T) {
	satrl := func(arr []string) string {
		ret, err := stringArrayToRawLines(arr)
		if err != nil {
			panic(err)
		}

		return ret
	}

	assert.EqualString(t, satrl([]string{"foo"}), " foo\n")
	assert.EqualString(t, satrl([]string{"foo", "bar"}), " foo\n bar\n")
}

func TestStringArrayToRawLinesFails(t *testing.T) {
	_, err := stringArrayToRawLines([]string{"foo\nbar"})

	assert.EqualString(t, err.Error(), "content cannot contain \\n")
}
