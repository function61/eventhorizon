package eh

import (
	"errors"
	"fmt"
	"path"
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
