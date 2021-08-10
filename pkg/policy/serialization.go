package policy

import (
	"bytes"

	"github.com/function61/gokit/encoding/jsonfile"
)

func Serialize(policy Policy) []byte {
	buf := &bytes.Buffer{}
	if err := jsonfile.Marshal(buf, policy); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func Deserialize(policySerialized []byte) (*Policy, error) {
	pol := &Policy{}
	return pol, jsonfile.UnmarshalDisallowUnknownFields(bytes.NewReader(policySerialized), pol)
}
