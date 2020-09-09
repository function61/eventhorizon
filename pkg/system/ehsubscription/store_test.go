package ehsubscription

import (
	"fmt"
	"strings"
	"testing"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/gokit/testing/assert"
)

func TestInsertIntoRecentList(t *testing.T) {
	recentList := []eh.CursorCompact{}

	makeCursor := func(streamNumber int, version int64) eh.CursorCompact {
		return eh.NewCursorCompact(eh.RootName.Child(fmt.Sprintf("s-%d", streamNumber)).At(version))
	}

	makeCursors := func(amount int, streamNumberBase int, version int64) []eh.CursorCompact {
		cursors := []eh.CursorCompact{}
		for i := 0; i < amount; i++ {
			cursors = append(cursors, makeCursor(streamNumberBase+i, version))
		}
		return cursors
	}

	dump := func() string {
		out := []string{}
		for _, cur := range recentList {
			out = append(out, cur.Serialize())
		}
		return "\n" + strings.Join(out, "\n")
	}

	recentList = insertIntoRecentList(recentList, makeCursors(15, 0, 0), 10)

	assert.EqualString(t, dump(), `
/s-5@0
/s-6@0
/s-7@0
/s-8@0
/s-9@0
/s-10@0
/s-11@0
/s-12@0
/s-13@0
/s-14@0`)

	recentList = insertIntoRecentList(recentList, []eh.CursorCompact{makeCursor(9, 1)}, 10)

	assert.EqualString(t, dump(), `
/s-5@0
/s-6@0
/s-7@0
/s-8@0
/s-10@0
/s-11@0
/s-12@0
/s-13@0
/s-14@0
/s-9@1`)

	recentList = insertIntoRecentList(recentList, []eh.CursorCompact{makeCursor(5, 1)}, 10)

	assert.EqualString(t, dump(), `
/s-6@0
/s-7@0
/s-8@0
/s-10@0
/s-11@0
/s-12@0
/s-13@0
/s-14@0
/s-9@1
/s-5@1`)

	recentList = insertIntoRecentList(recentList, []eh.CursorCompact{makeCursor(666, 123)}, 10)

	assert.EqualString(t, dump(), `
/s-7@0
/s-8@0
/s-10@0
/s-11@0
/s-12@0
/s-13@0
/s-14@0
/s-9@1
/s-5@1
/s-666@123`)
}
