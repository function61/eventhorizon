package lineformatsimple

// Event line format: simple.
// Format: "<event type> <payload in JSON>"

import (
	"fmt"
	"regexp"
)

var parseRe = regexp.MustCompile(`^([a-zA-Z]+) (\{.+)$`)

func Parse(line string) (eventType string, payload string, err error) {
	parsed := parseRe.FindStringSubmatch(line)

	if parsed == nil {
		return "", "", fmt.Errorf("Unable to parse line: %s", line)
	}

	return parsed[1], parsed[2], nil
}
