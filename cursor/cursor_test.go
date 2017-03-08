package cursor

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

func True(t *testing.T, actual bool) {
	if !actual {
		t.Fatalf("Expecting true; got false")
	}
}

func False(t *testing.T, actual bool) {
	if actual {
		t.Fatalf("Expecting true; got false")
	}
}

func TestNew(t *testing.T) {
	cursor := New("/tenants/foo", 3, 42, "poop")

	EqualString(t, cursor.Serialize(), "/tenants/foo:3:42:poop")
}

func TestBeginningOfStream(t *testing.T) {
	EqualString(t, BeginningOfStream("/foo", NoServer).Serialize(), "/foo:0:0")
}

func TestCursorWithoutServer(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	EqualString(t, cursor.Serialize(), "/tenants/foo:3:42")

	EqualString(t, cursor.Stream, "/tenants/foo")
	EqualInt(t, cursor.Chunk, 3)
	EqualInt(t, cursor.Offset, 42)
	EqualString(t, cursor.Server, "")
}

func TestIsAheadComparedTo(t *testing.T) {
	// ------- intra-chunk tests

	True(t, CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
	False(t, CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))

	// equal => false
	False(t, CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))

	// ------- cross-chunk tests

	True(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))
	False(t, CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:1:42")))

	// same offset but bigger chunk
	True(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
}

func TestIsAheadComparedToPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.IsAheadComparedTo(c2)
}

func TestPositionEquals(t *testing.T) {
	True(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:45:server")))
	False(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:46")))
	False(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:10:45")))
}

func TestPositionEqualsPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.PositionEquals(c2)
}

func TestToChunkPath(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	EqualString(t, cursor.ToChunkPath(), "/tenants/foo/_/3.log")
}

func TestToChunkSafePath(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	EqualString(t, cursor.ToChunkSafePath(), "_tenants_foo___3.log")
}

func TestCursorTooFewComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3")
	// EqualString(t, tooFewComponents, nil)
	EqualString(t, err.Error(), "Cursor: too few components: /tenants/foo:3")
}

func TestCursorTooManyComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:42:server:6")
	// EqualString(t, tooFewComponents, nil)
	EqualString(t, err.Error(), "Cursor: too many components: /tenants/foo:3:42:server:6")
}

func TestCursorInvalidChunk(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:a:42")
	// EqualString(t, tooFewComponents, nil)
	EqualString(t, err.Error(), "Cursor: invalid chunk idx")
}

func TestCursorInvalidOffset(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:b")
	// EqualString(t, tooFewComponents, nil)
	EqualString(t, err.Error(), "Cursor: invalid chunk offset")
}

func TestCursorWithServer(t *testing.T) {
	withServer := CursorFromserializedMust("/tenants/foo:3:42:poop")

	EqualString(t, withServer.Serialize(), "/tenants/foo:3:42:poop")

	EqualString(t, withServer.Stream, "/tenants/foo")
	EqualInt(t, withServer.Chunk, 3)
	EqualInt(t, withServer.Offset, 42)
	EqualString(t, withServer.Server, "poop")
}
