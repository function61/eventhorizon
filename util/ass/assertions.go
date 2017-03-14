package ass

import (
	"testing"
)

func EqualString(t *testing.T, actual string, expected string) {
	if actual != expected {
		t.Fatalf("exp=%v; got=%v", expected, actual)
	}
}

func EqualInt(t *testing.T, actual int, expected int) {
	if actual != expected {
		t.Fatalf("exp=%v; got=%v", expected, actual)
	}
}

func True(t *testing.T, actual bool) {
	if !actual {
		t.Fatalf("Expecting true; got false")
	}
}

func False(t *testing.T, actual bool) {
	if actual {
		t.Fatalf("Expecting true; got false")
	}
}
