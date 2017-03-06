package metaevents

import (
	"encoding/json"
	"errors"
	"regexp"
)

func Parse(line string) (bool, string, interface{}) {
	// only meta lines start with ".". therefore this is either:
	// a) regular unescaped line (this is hottest path)
	// b) regular escaped line
	if len(line) != 0 && line[0:1] != "." {
		// a) return regular line as-is
		if line[0:1] != "\\" {
			return false, line, nil
		}

		// b) return escaped line with the leading "\" escape char trimmed
		// this way we can represent both "." and "\" chars in regular lines
		return false, line[1:], nil
	}

	// regular empty line is syntactically valid but
	// to append empty lines you'd be stupid
	if len(line) == 0 {
		return false, line, nil
	}

	parseRe := regexp.MustCompile("^\\.([a-zA-Z]+) (\\{.+)$")

	parsed := parseRe.FindStringSubmatch(line)

	if parsed == nil {
		panic(errors.New("Unable to parse meta line: " + line))
	}

	typ := parsed[1]
	payload := parsed[2]

	// yes, unfortunately we must repeat even the JSON unmarshaling to every case,
	// because if we'd declar "obj" *here* as interface{}, unmarshal would not
	// know the concrete type

	if typ == "Rotated" {
		obj := Rotated{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	} else if typ == "Created" {
		obj := Created{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	} else if typ == "AuthorityChanged" {
		obj := AuthorityChanged{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	} else if typ == "Subscribed" {
		obj := Subscribed{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	} else if typ == "Unsubscribed" {
		obj := Unsubscribed{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	} else if typ == "ChildStreamCreated" {
		obj := ChildStreamCreated{}
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			panic(errors.New("Unable to parse meta line: " + typ))
		}

		return true, line, obj
	}

	// do not crash if we encounter unknown meta types
	return true, line, nil
}
