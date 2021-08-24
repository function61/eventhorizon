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

func (s StreamName) Equal(other StreamName) bool {
	return s.name == other.name
}

func (s StreamName) IsDescendantOf(other StreamName) bool {
	return strings.HasPrefix(s.name+"/", other.name)
}

func (s StreamName) ResourceName() policy.ResourceName {
	return ResourceNameStream.Child(s.String())
}

func (s StreamName) DEKResourceName(dekVersion int) policy.ResourceName {
	return resourceNameDEK.Child(s.String()).Child(strconv.Itoa(dekVersion))
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

// "/foo" => "foo"
// "/foo/bar" => "bar"
// "/foo/bar/baz" => "baz"
// "/" => "/"
func (s StreamName) Base() string {
	return path.Base(s.name)
}

func (s StreamName) At(version int64) Cursor {
	return Cursor{s.String(), version}
}

// in context of "give events > -1" (not >=). the actual beginning is 0
func (s StreamName) Beginning() Cursor {
	return s.At(-1)
}

func (s StreamName) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.name)
}

func (s *StreamName) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.name); err != nil {
		return fmt.Errorf("StreamName.UnmarshalJSON: %w", err)
	}

	return nil
}
