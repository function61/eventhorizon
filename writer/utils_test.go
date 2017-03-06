package writer

import (
	"testing"
)

func EqualString(t *testing.T, actual string, expected string) {
	if actual != expected {
		t.Fatalf("exp=%v; got=%v", expected, actual)
	}
}

func EqualInt(t *testing.T, actual int, expected int) {
	if actual != expected {
		t.Fatalf("exp=%v; got=%v", expected, actual)
	}
}

func TestParentStreamName(t *testing.T) {
	EqualString(t, parentStreamName("/tenants/foo"), "/tenants")
	EqualString(t, parentStreamName("/tenants"), "/")
	EqualString(t, parentStreamName("/"), "/")
}

func TestStringSliceItemIndex(t *testing.T) {
	collection := []string{"foo", "bar", "baz"}

	EqualInt(t, stringSliceItemIndex("foo", collection), 0)
	EqualInt(t, stringSliceItemIndex("baz", collection), 2)
	EqualInt(t, stringSliceItemIndex("bar", collection), 1)

	EqualInt(t, stringSliceItemIndex("", collection), -1)
	EqualInt(t, stringSliceItemIndex("fasdf", collection), -1)
}

func TestStringArrayToRawLines(t *testing.T) {
	satrl := func(arr []string) string {
		ret, err := stringArrayToRawLines(arr)
		if err != nil {
			panic(err)
		}

		return ret
	}

	EqualString(t, satrl([]string{"foo"}), "foo\n")
	EqualString(t, satrl([]string{"foo", "bar"}), "foo\nbar\n")
	EqualString(t, satrl([]string{".foo", "\\bar", "baz"}), "\\.foo\n\\\\bar\nbaz\n")
}
