package ehevent

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

func Serialize(e Event) string {
	return e.Meta().Serialize(e)
}

// <time> <event> <targetUser> [<actingUser>] [<backdate>] <payload>
//
// looks like:
// 2017-12-17T10:28:28.528Z account.Created 2   {"Id":"bab11eda","FolderId":"root","Title":"Test SSH key"}
func (e *EventMeta) Serialize(payload Event) string {
	backdated := ""
	if !e.TimestampOfRecording.IsZero() {
		backdated = e.TimestampOfRecording.UTC().Format(rfc3339Milli)
	}

	payloadJson, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return strings.Join([]string{
		e.Timestamp.UTC().Format(rfc3339Milli),
		payload.MetaType(),
		e.UserId,
		e.ImpersonatingUserId,
		backdated,
		string(payloadJson),
	}, " ")
}

var deserializeRe = regexp.MustCompile("^([^ ]+) ([^ ]+) ([^ ]*) ([^ ]*) ([^ ]*) (.+)$")

func Deserialize(input string, allocators Allocators) (Event, error) {
	components := deserializeRe.FindStringSubmatch(input)
	if components == nil {
		// NOTE: this might be unsafe to show to the user
		return nil, fmt.Errorf("deserialize: invalid format: %s", input)
	}

	eventType := components[2]

	allocator, allocatorFound := allocators[eventType]
	if !allocatorFound {
		return nil, fmt.Errorf("deserialize: unknown event: %s", eventType)
	}

	e := allocator()

	// intentionally not having DisallowUnknownFields for forward compatibility
	if err := json.Unmarshal([]byte(components[6]), e); err != nil {
		return nil, fmt.Errorf("deserialize: JSON unmarshal: %v", err)
	}

	// using reference for mutations (dirty)
	meta := e.Meta()

	var err error
	meta.Timestamp, err = time.Parse(rfc3339Milli, components[1])
	if err != nil {
		return nil, fmt.Errorf("deserialize: Timestamp: %v", err)
	}

	if components[5] != "" {
		meta.TimestampOfRecording, err = time.Parse(rfc3339Milli, components[5])
		if err != nil {
			return nil, fmt.Errorf("deserialize: TimestampOfRecording: %v", err)
		}
	}

	meta.UserId = components[3]
	meta.ImpersonatingUserId = components[4]

	return e, nil
}
