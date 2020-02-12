package stringslicediff

import (
	"github.com/function61/gokit/assert"
	"testing"
)

func TestDiff(t *testing.T) {
	runScenario(t,
		[]string{"foo"},
		[]string{},
		" -foo")

	runScenario(t,
		[]string{"foo"},
		[]string{"foo"},
		"")

	runScenario(t,
		[]string{"foo"},
		[]string{"foo", "bar"},
		" +bar")

	runScenario(t,
		[]string{"foo"},
		[]string{"foo", "bar", "baz"},
		" +bar +baz")

	runScenario(t,
		[]string{"foo", "bar", "baz"},
		[]string{"foo", "shizzle"},
		" +shizzle -bar -baz")
}

func runScenario(t *testing.T, base []string, other []string, expected string) {
	result := Diff(base, other)

	resultSerialized := ""

	for _, val := range result.Added {
		resultSerialized += " +" + val
	}

	for _, val := range result.Removed {
		resultSerialized += " -" + val
	}

	assert.EqualString(t, resultSerialized, expected)
}
