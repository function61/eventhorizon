package writer

import (
	"github.com/function61/pyramid/util/ass"
	"testing"
)

func TestParentStreamName(t *testing.T) {
	ass.EqualString(t, parentStreamName("/tenants/foo"), "/tenants")
	ass.EqualString(t, parentStreamName("/tenants"), "/")
	ass.EqualString(t, parentStreamName("/"), "/")
}

func TestStringSliceItemIndex(t *testing.T) {
	collection := []string{"foo", "bar", "baz"}

	ass.EqualInt(t, stringSliceItemIndex("foo", collection), 0)
	ass.EqualInt(t, stringSliceItemIndex("baz", collection), 2)
	ass.EqualInt(t, stringSliceItemIndex("bar", collection), 1)

	ass.EqualInt(t, stringSliceItemIndex("", collection), -1)
	ass.EqualInt(t, stringSliceItemIndex("fasdf", collection), -1)
}

func TestStringArrayToRawLines(t *testing.T) {
	satrl := func(arr []string) string {
		ret, err := stringArrayToRawLines(arr)
		if err != nil {
			panic(err)
		}

		return ret
	}

	ass.EqualString(t, satrl([]string{"foo"}), "foo\n")
	ass.EqualString(t, satrl([]string{"foo", "bar"}), "foo\nbar\n")
	ass.EqualString(t, satrl([]string{".foo", "\\bar", "baz"}), "\\.foo\n\\\\bar\nbaz\n")
}
