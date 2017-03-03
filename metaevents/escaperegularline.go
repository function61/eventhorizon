package metaevents

// regular and meta lines are stored in our event store alongside each other.
// meta lines always start with a dot, but regular lines must be able to start with a dot as well.
func EscapeRegularLine(input string) string {
	// hot path: return as-is
	if len(input) != 0 && input[0:1] != "." && input[0:1] != "\\" {
		return input
	}

	if len(input) == 0 {
		return input
	}

	return "\\" + input
}
