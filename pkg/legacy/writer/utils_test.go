package writer

import (
	"github.com/function61/eventhorizon/pkg/legacy/util/ass"
	"testing"
)

func TestParentStreamName(t *testing.T) {
	ass.EqualString(t, parentStreamName("/tenants/foo"), "/tenants")
	ass.EqualString(t, parentStreamName("/tenants"), "/")
	ass.EqualString(t, parentStreamName("/"), "/")
}

func TestStringArrayToRawLines(t *testing.T) {
	satrl := func(arr []string) string {
		ret, err := stringArrayToRawLines(arr)
		if err != nil {
			panic(err)
		}

		return ret
	}

	ass.EqualString(t, satrl([]string{"foo"}), " foo\n")
	ass.EqualString(t, satrl([]string{"foo", "bar"}), " foo\n bar\n")
}

func TestStringArrayToRawLinesFails(t *testing.T) {
	_, err := stringArrayToRawLines([]string{"foo\nbar"})

	ass.EqualString(t, err.Error(), "content cannot contain \\n")
}
