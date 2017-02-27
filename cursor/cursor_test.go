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

func TestCursor(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	EqualString(t, cursor.Serialize(), "/tenants/foo:3:42")

	EqualString(t, cursor.Stream, "/tenants/foo")
	EqualInt(t, cursor.Chunk, 3)
	EqualInt(t, cursor.Offset, 42)
	EqualString(t, cursor.Server, "")
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
