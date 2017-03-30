package cursor

import (
	"github.com/function61/eventhorizon/util/ass"
	"testing"
)

func TestNew(t *testing.T) {
	cursor := New("/tenants/foo", 3, 42, "poop")

	ass.EqualString(t, cursor.Serialize(), "/tenants/foo:3:42:poop")
}

func TestForOffsetQuery(t *testing.T) {
	ass.EqualString(t, ForOffsetQuery("/foo").Serialize(), "/foo:-1:-1")
}

func TestBeginningOfStream(t *testing.T) {
	ass.EqualString(t, BeginningOfStream("/foo", NoServer).Serialize(), "/foo:0:0")
}

func TestCursorWithoutServer(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	ass.EqualString(t, cursor.Serialize(), "/tenants/foo:3:42")

	ass.EqualString(t, cursor.Stream, "/tenants/foo")
	ass.EqualInt(t, cursor.Chunk, 3)
	ass.EqualInt(t, cursor.Offset, 42)
	ass.EqualString(t, cursor.Server, "")
}

func TestIsAheadComparedTo(t *testing.T) {
	// ------- intra-chunk tests

	ass.True(t, CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
	ass.False(t, CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))

	// equal => false
	ass.False(t, CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))

	// ------- cross-chunk tests

	ass.True(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))
	ass.False(t, CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:1:42")))

	// same offset but bigger chunk
	ass.True(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
}

func TestIsAheadComparedToPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		ass.EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.IsAheadComparedTo(c2)
}

func TestPositionEquals(t *testing.T) {
	ass.True(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:45:server")))
	ass.False(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:46")))
	ass.False(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:10:45")))
}

func TestPositionEqualsPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		ass.EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.PositionEquals(c2)
}

func TestToChunkPath(t *testing.T) {
	ass.EqualString(
		t,
		CursorFromserializedMust("/tenants/foo:3:42").ToChunkPath(),
		"/tenants/foo/_/3.log")

	// root
	ass.EqualString(
		t,
		CursorFromserializedMust("/:3:42").ToChunkPath(),
		"/_/3.log")
}

func TestToChunkSafePath(t *testing.T) {
	ass.EqualString(
		t,
		CursorFromserializedMust("/tenants/foo:3:42").ToChunkSafePath(),
		"_tenants_foo___3.log")

	// root
	ass.EqualString(
		t,
		CursorFromserializedMust("/:3:42").ToChunkSafePath(),
		"___3.log")
}

func TestCursorTooFewComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3")
	// ass.EqualString(t, tooFewComponents, nil)
	ass.EqualString(t, err.Error(), "Cursor: too few components: /tenants/foo:3")
}

func TestCursorTooManyComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:42:server:6")
	// ass.EqualString(t, tooFewComponents, nil)
	ass.EqualString(t, err.Error(), "Cursor: too many components: /tenants/foo:3:42:server:6")
}

func TestCursorInvalidChunk(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:a:42")
	// ass.EqualString(t, tooFewComponents, nil)
	ass.EqualString(t, err.Error(), "Cursor: invalid chunk idx")
}

func TestCursorInvalidOffset(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:b")
	// ass.EqualString(t, tooFewComponents, nil)
	ass.EqualString(t, err.Error(), "Cursor: invalid chunk offset")
}

func TestCursorWithServer(t *testing.T) {
	withServer := CursorFromserializedMust("/tenants/foo:3:42:poop")

	ass.EqualString(t, withServer.Serialize(), "/tenants/foo:3:42:poop")

	ass.EqualString(t, withServer.Stream, "/tenants/foo")
	ass.EqualInt(t, withServer.Chunk, 3)
	ass.EqualInt(t, withServer.Offset, 42)
	ass.EqualString(t, withServer.Server, "poop")
}

func TestOffsetString(t *testing.T) {
	withServer := CursorFromserializedMust("/tenants/foo:3:42:poop")

	ass.EqualString(t, withServer.OffsetString(), "3:42")
}
