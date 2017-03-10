package cursor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Cursor is an exact byte position into a block inside a stream. You can (de)serialize
// and compare cursors (is cursor A behind cursor B?). Same cursor can look different
// because some cursors can have server hints and some don't. They still represent
// the same position, that is to say equality is tested only for (stream, block, offset).
//
// The good thing about this unambiquous cursor is that under the hood reading
// data from a cursor is ultimately just an fseek into a single file. Which is fast.

type Cursor struct {
	Stream string
	Chunk  int
	Offset int
	Server string
}

const (
	NoServer = ""
)

func New(stream string, chunk int, offset int, server string) *Cursor {
	return &Cursor{
		Stream: stream,
		Chunk:  chunk,
		Offset: offset,
		Server: server,
	}
}

func ForOffsetQuery(stream string) *Cursor {
	// negative values are syntactically valid but not logically.
	// we use this mainly to provide valid looking offset to push operation
	// for which we will get incorrect offset response with correct offset back,
	// so this is essentially to implement offset queries
	return New(stream, -1, -1, NoServer)
}

func BeginningOfStream(stream string, server string) *Cursor {
	return New(stream, 0, 0, server)
}

func (c *Cursor) Serialize() string {
	if c.Server == "" {
		return c.Stream + ":" + strconv.Itoa(c.Chunk) + ":" + strconv.Itoa(c.Offset)
	}

	return c.Stream + ":" + strconv.Itoa(c.Chunk) + ":" + strconv.Itoa(c.Offset) + ":" + c.Server
}

func (c *Cursor) PositionEquals(other *Cursor) bool {
	if c.Stream != other.Stream {
		panic(errors.New("Cursor: cannot compare cursors from different streams"))
	}

	return c.Chunk == other.Chunk && c.Offset == other.Offset
}

func (c *Cursor) IsAheadComparedTo(other *Cursor) bool {
	if c.Stream != other.Stream {
		panic(errors.New("Cursor: cannot compare cursors from different streams"))
	}

	// block index equal => forward if intra-chunk offset greater
	if c.Chunk == other.Chunk {
		return c.Offset > other.Offset
	} else {
		// block index not equal => definitely EITHER forward OR backward
		return c.Chunk > other.Chunk
	}
}

// "/tenants/root/_/0.log"
func (c *Cursor) ToChunkPath() string {
	return fmt.Sprintf("%s/_/%d.log", c.Stream, c.Chunk)
}

// "_tenants_root___0.log"
func (c *Cursor) ToChunkSafePath() string {
	return strings.Replace(c.ToChunkPath(), "/", "_", -1)
}

// stream:chunk:pos
//   /tenants/1:13:42
// stream:chunk:pos:server
func CursorFromserialized(serialized string) (*Cursor, error) {
	arr := strings.Split(serialized, ":")
	arrLen := len(arr)

	if arrLen < 3 {
		return nil, errors.New("Cursor: too few components: " + serialized)
	}

	if arrLen > 4 {
		return nil, errors.New("Cursor: too many components: " + serialized)
	}

	chunkIdx, err := strconv.Atoi(arr[1])
	if err != nil {
		return nil, errors.New("Cursor: invalid chunk idx")
	}

	offset, err := strconv.Atoi(arr[2])
	if err != nil {
		return nil, errors.New("Cursor: invalid chunk offset")
	}

	c := &Cursor{
		Stream: arr[0],
		Chunk:  chunkIdx,
		Offset: offset,
	}

	if arrLen == 4 {
		c.Server = arr[3]
	}

	return c, nil
}

func CursorFromserializedMust(serialized string) *Cursor {
	c, err := CursorFromserialized(serialized)
	if err != nil {
		panic(err)
	}

	return c
}
