package target

import (
	"errors"
	"regexp"
)

var parseRe = regexp.MustCompile("^([a-zA-Z]+) (\\{.+)$")

func applySerializedEvent(line string, pa *Target) (bool, error) {
	parsed := parseRe.FindStringSubmatch(line)

	if parsed == nil {
		panic(errors.New("Unable to parse line: " + line))
	}

	typ := parsed[1]
	payload := parsed[2]

	if fn, ok := eventNameToApplyFn[typ]; ok {
		err := fn(pa, payload)
		return true, err
	}

	return false, nil
}
