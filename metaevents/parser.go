package metaevents

// This package implements raw storage format. See docs for more details.

import (
	"encoding/json"
	"errors"
	"regexp"
)

var (
	errorEmptyLine   = errors.New("empty line encountered")
	errorUnknownType = errors.New("line wasn't neither of regular line or meta event line")

	metaEventParseRe = regexp.MustCompile(`^\/([a-zA-Z]+) (\{.+)$`)
)

// encodes a regular line for storing in Event Horizon. caller's responsibility
// is to check that input does not contain \n
func EncodeRegularLine(input string) string {
	return " " + input
}

// parses both regular and meta event lines
func Parse(line string) (metaType string, lineContent string, metaEvent interface{}) {
	// this shouldn't happen, but [0] would panic so
	if len(line) == 0 {
		panic(errorEmptyLine)
	}

	if line[0:1] == " " {
		return "", line[1:], nil
	}

	if line[0:1] != "/" {
		panic(errorUnknownType)
	}

	parsed := metaEventParseRe.FindStringSubmatch(line)

	if parsed == nil {
		panic(errors.New("Unable to parse meta line: " + line))
	}

	typ := parsed[1]
	payload := parsed[2]

	// yes, unfortunately we must repeat even the JSON unmarshaling to every case,
	// because if we'd declare "obj" *here* as interface{}, Unmarshal() would not
	// know the concrete type

	if typ == "Rotated" {
		obj := Rotated{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	} else if typ == "Created" {
		obj := Created{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	} else if typ == "Subscribed" {
		obj := Subscribed{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	} else if typ == "Unsubscribed" {
		obj := Unsubscribed{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	} else if typ == "ChildStreamCreated" {
		obj := ChildStreamCreated{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	} else if typ == "SubscriptionActivity" {
		obj := SubscriptionActivity{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return typ, payload, obj
	}

	// do not crash if we encounter unknown meta types
	return typ, payload, nil
}
