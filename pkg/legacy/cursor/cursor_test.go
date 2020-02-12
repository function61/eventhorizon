package cursor

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestNew(t *testing.T) {
	cursor := New("/tenants/foo", 3, 42, "poop")

	assert.EqualString(t, cursor.Serialize(), "/tenants/foo:3:42:poop")
}

func TestForOffsetQuery(t *testing.T) {
	assert.EqualString(t, ForOffsetQuery("/foo").Serialize(), "/foo:-1:-1")
}

func TestBeginningOfStream(t *testing.T) {
	assert.EqualString(t, BeginningOfStream("/foo", NoServer).Serialize(), "/foo:0:0")
}

func TestCursorWithoutServer(t *testing.T) {
	cursor := CursorFromserializedMust("/tenants/foo:3:42")

	assert.EqualString(t, cursor.Serialize(), "/tenants/foo:3:42")

	assert.EqualString(t, cursor.Stream, "/tenants/foo")
	assert.Assert(t, cursor.Chunk == 3)
	assert.Assert(t, cursor.Offset == 42)
	assert.EqualString(t, cursor.Server, "")
}

func TestIsAheadComparedTo(t *testing.T) {
	// ------- intra-chunk tests

	assert.Assert(t, CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
	assert.Assert(t, !CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))

	// equal => false
	assert.Assert(t, !CursorFromserializedMust("/:0:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))

	// ------- cross-chunk tests

	assert.Assert(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:123")))
	assert.Assert(t, !CursorFromserializedMust("/:0:123").IsAheadComparedTo(CursorFromserializedMust("/:1:42")))

	// same offset but bigger chunk
	assert.Assert(t, CursorFromserializedMust("/:1:42").IsAheadComparedTo(CursorFromserializedMust("/:0:42")))
}

func TestIsAheadComparedToPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.IsAheadComparedTo(c2)
}

func TestPositionEquals(t *testing.T) {
	assert.Assert(t, CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:45:server")))
	assert.Assert(t, !CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:13:46")))
	assert.Assert(t, !CursorFromserializedMust("/foo:13:45").PositionEquals(CursorFromserializedMust("/foo:10:45")))
}

func TestPositionEqualsPanicsWithDifferingStreams(t *testing.T) {
	defer func() {
		assert.EqualString(t, recover().(error).Error(), "Cursor: cannot compare cursors from different streams")
	}()

	c1 := CursorFromserializedMust("/foo:0:0")
	c2 := CursorFromserializedMust("/bar:0:0")

	c1.PositionEquals(c2)
}

func TestToChunkPath(t *testing.T) {
	assert.EqualString(
		t,
		CursorFromserializedMust("/tenants/foo:3:42").ToChunkPath(),
		"/tenants/foo/_/3.log")

	// root
	assert.EqualString(
		t,
		CursorFromserializedMust("/:3:42").ToChunkPath(),
		"/_/3.log")
}

func TestToChunkSafePath(t *testing.T) {
	assert.EqualString(
		t,
		CursorFromserializedMust("/tenants/foo:3:42").ToChunkSafePath(),
		"_tenants_foo___3.log")

	// root
	assert.EqualString(
		t,
		CursorFromserializedMust("/:3:42").ToChunkSafePath(),
		"___3.log")
}

func TestCursorTooFewComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3")
	// assert.EqualString(t, tooFewComponents, nil)
	assert.EqualString(t, err.Error(), "Cursor: too few components: /tenants/foo:3")
}

func TestCursorTooManyComponents(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:42:server:6")
	// assert.EqualString(t, tooFewComponents, nil)
	assert.EqualString(t, err.Error(), "Cursor: too many components: /tenants/foo:3:42:server:6")
}

func TestCursorInvalidChunk(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:a:42")
	// assert.EqualString(t, tooFewComponents, nil)
	assert.EqualString(t, err.Error(), "Cursor: invalid chunk idx")
}

func TestCursorInvalidOffset(t *testing.T) {
	_, err := CursorFromserialized("/tenants/foo:3:b")
	// assert.EqualString(t, tooFewComponents, nil)
	assert.EqualString(t, err.Error(), "Cursor: invalid chunk offset")
}

func TestCursorWithServer(t *testing.T) {
	withServer := CursorFromserializedMust("/tenants/foo:3:42:poop")

	assert.EqualString(t, withServer.Serialize(), "/tenants/foo:3:42:poop")

	assert.EqualString(t, withServer.Stream, "/tenants/foo")
	assert.Assert(t, withServer.Chunk == 3)
	assert.Assert(t, withServer.Offset == 42)
	assert.EqualString(t, withServer.Server, "poop")
}

func TestOffsetString(t *testing.T) {
	withServer := CursorFromserializedMust("/tenants/foo:3:42:poop")

	assert.EqualString(t, withServer.OffsetString(), "3:42")
}
