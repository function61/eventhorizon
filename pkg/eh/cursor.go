package eh

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type Cursor struct {
	stream  string
	version int64
}

// separate struct because fields have to be exported
type cursorJson struct {
	Stream  *string // both are pointers so we can detect missing values
	Version *int64
}

func (c Cursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(cursorJson{
		Stream:  &c.stream,
		Version: &c.version,
	})
}

func (c *Cursor) UnmarshalJSON(data []byte) error {
	cj := cursorJson{}
	if err := json.Unmarshal(data, &cj); err != nil {
		return err
	}
	if cj.Stream == nil || cj.Version == nil {
		return fmt.Errorf("Cursor.UnmarshalJSON(): Stream or Version nil: %s", data)
	}
	c.stream = *cj.Stream
	c.version = *cj.Version
	return nil
}

func (c *Cursor) Stream() StreamName {
	return newStreamNameNoValidation(c.stream)
}

func (c *Cursor) Version() int64 {
	return c.version
}

func (c *Cursor) AtBeginning() bool {
	return c.version == -1
}

func (c *Cursor) Before(other Cursor) bool {
	if c.stream != other.stream {
		panic("cannot compare unrelated streams")
	}

	return c.version < other.version
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

// TODO: improve
func DeserializeCursor(serialized string) (Cursor, error) {
	comps := strings.Split(serialized, "@")
	if len(comps) != 2 {
		return Cursor{}, fmt.Errorf("DeserializeCursor: syntax error: %s", serialized)
	}

	version, err := strconv.ParseInt(comps[1], 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("DeserializeCursor: %w", err)
	}

	streamName, err := DeserializeStreamName(comps[0])
	if err != nil {
		return Cursor{}, fmt.Errorf("DeserializeCursor: %w", err)
	}

	return streamName.At(version), nil
}
