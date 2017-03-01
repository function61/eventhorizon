package cursor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Cursor struct {
	Stream string
	Chunk  int
	Offset int
	Server string
}

func NewWithoutServer(stream string, chunk int, offset int) *Cursor {
	return &Cursor{
		Stream: stream,
		Chunk:  chunk,
		Offset: offset,
	}
}

func (c *Cursor) Serialize() string {
	if c.Server == "" {
		return c.Stream + ":" + strconv.Itoa(c.Chunk) + ":" + strconv.Itoa(c.Offset)
	}

	return c.Stream + ":" + strconv.Itoa(c.Chunk) + ":" + strconv.Itoa(c.Offset) + ":" + c.Server
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
