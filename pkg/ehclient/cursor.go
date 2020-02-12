package ehclient

import (
	"fmt"
)

type Cursor struct {
	stream  string
	version int64
}

func (c *Cursor) Stream() string {
	return c.stream
}

func (c *Cursor) Version() int64 {
	return c.version
}

func (c *Cursor) Equal(other Cursor) bool {
	return c.stream == other.stream && c.version == other.version
}

func (c *Cursor) Next() Cursor {
	return Cursor{c.stream, c.version + 1}
}

func (c *Cursor) Serialize() string {
	return fmt.Sprintf("%s@%d", c.stream, c.version)
}

func At(stream string, version int64) Cursor {
	if stream == "" {
		panic("stream name cannot be empty")
	}

	return Cursor{stream, version}
}

func Beginning(stream string) Cursor {
	return At(stream, -1)
}
