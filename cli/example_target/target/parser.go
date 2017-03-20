package target

import (
	"errors"
	"regexp"
)

var parseRe = regexp.MustCompile("^([a-zA-Z]+) (\\{.+)$")

func parse(line string) interface{} {
	parsed := parseRe.FindStringSubmatch(line)

	if parsed == nil {
		panic(errors.New("Unable to parse line: " + line))
	}

	typ := parsed[1]
	payload := parsed[2]

	if fn, ok := eventNameToDecoderFn[typ]; ok {
		return fn(payload)
	} else {
		return nil
	}
}
