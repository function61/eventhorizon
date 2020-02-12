package main

import (
	"errors"
)

func stringKeyedMapToStringSlice(mymap map[string]func([]string) error) []string {
	keys := make([]string, len(mymap))

	i := 0
	for k := range mymap {
		keys[i] = k
		i++
	}

	return keys
}

func usage(help string) error {
	return errors.New("Usage: " + help)
}
