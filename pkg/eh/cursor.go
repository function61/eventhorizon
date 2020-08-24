package eh

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/function61/eventhorizon/pkg/policy"
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

func (c *Cursor) Less(other Cursor) bool {
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

type StreamName struct {
	name string
}

func newStreamNameNoValidation(name string) StreamName {
	return StreamName{name}
}

// TODO: add better validation
func DeserializeStreamName(name string) (StreamName, error) {
	if name == "" {
		return StreamName{}, errors.New("StreamName empty")
	}

	return newStreamNameNoValidation(name), nil
}

var RootName = newStreamNameNoValidation("/")

func (s StreamName) Child(name string) StreamName {
	if name == "" {
		panic(fmt.Errorf("tried to create empty StreamName child for '%s'", s.name))
	}

	if strings.Contains(name, "/") {
		panic("StreamName child cannot contain '/'")
	}

	return StreamName{path.Join(s.name, name)}
}

func (s StreamName) String() string {
	return s.name
}

func (s StreamName) IsUnder(other StreamName) bool {
	return strings.HasPrefix(s.name, other.name)
}

func (s StreamName) ResourceName() policy.ResourceName {
	return ResourceNameStream.Child(s.String())
}

// "/foo" => "/"
// "/foo/bar" => "/foo"
// "/" => nil
func (s StreamName) Parent() *StreamName {
	parent := path.Dir(s.name)
	if parent == s.name { // already at root? Dir("/") == "/"
		return nil
	}

	return &StreamName{parent}
}

func (s StreamName) At(version int64) Cursor {
	return Cursor{s.String(), version}
}

// in context of "give events > -1" (not >=). the actual beginning is 0
func (s StreamName) Beginning() Cursor {
	return s.At(-1)
}
