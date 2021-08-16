package ehevent

import (
	"encoding/json"
	"fmt"
	"strings"
)

func Serialize(events ...Event) []string {
	serialized := []string{}
	for _, event := range events {
		serialized = append(serialized, SerializeOne(event))
	}

	return serialized
}

func SerializeOne(e Event) string {
	return e.Meta().Serialize(e)
}

func (e *EventMeta) Serialize(payload Event) string {
	// before serializing, annotate EventMeta with event type information
	metaJson, err := json.Marshal(eventMetaWithEventType{payload.MetaType(), *e})
	if err != nil {
		panic(err) // JSON marshaling shouldn't ever fail
	}

	payloadJson, err := json.Marshal(payload)
	if err != nil {
		panic(err) // JSON marshaling shouldn't ever fail
	}

	return fmt.Sprintf("%s %s", metaJson, payloadJson)
}

// deserialized one event from the line format
func Deserialize(input string, allocators Types) (Event, error) {
	dec := json.NewDecoder(strings.NewReader(input))

	// we would like to DisallowUnknownFields() for metadata, but since our input stream
	// contains two different types and we can't reset "DisallowUnknownFields" without
	// making new decoder with io.MultiReader(io.Buffered(), input) hack.

	metaWithType := &eventMetaWithEventType{}
	if err := dec.Decode(metaWithType); err != nil {
		return nil, fmt.Errorf("deserialize: meta: %v", err)
	}

	// intentionally not setting DisallowUnknownFields() for payload to be forward-compatible

	eventAllocator, found := allocators[metaWithType.Type]
	if !found {
		return nil, fmt.Errorf("deserialize: unknown type: %s", metaWithType.Type)
	}

	// initialize zero-valued struct for this event type that we can unmarshal JSON into
	event := eventAllocator()

	if err := dec.Decode(event); err != nil {
		return nil, fmt.Errorf("deserialize: event: %v", err)
	}

	// assign metadata (mutating via reference is a bit of a hack..)
	*event.Meta() = metaWithType.EventMeta

	return event, nil
}

// type for JSON-serializing EventMeta with added type information
type eventMetaWithEventType struct {
	Type string `json:"_"`
	EventMeta
}

// helper. last line won't have \n after it.
// WARNING: you're responsible for making sure none of the lines have \n on it.
func SerializeLines(lines []string) []byte {
	return []byte(strings.Join(lines, "\n"))
}

func DeserializeLines(lines []byte) []string {
	return strings.Split(string(lines), "\n")
}

