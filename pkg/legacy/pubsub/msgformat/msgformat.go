package msgformat

import (
	"strings"
)

// 'SET key value\n' => [ 'SET', 'key', 'value' ]
func Deserialize(line string) []string {
	// below code would panic on line == "" without this check
	if len(line) < 1 {
		// guarantee that we always return len() >= 1
		return []string{""}
	}

	msgParts := strings.Split(line[0:len(line)-1], " ")

	return msgParts
}

func Serialize(message []string) string {
	return strings.Join(message, " ") + "\n"
}
