package pubsub

import (
	"strings"
)

// 'SET key value\n' => [ 'SET', 'key', 'value' ]
func MsgformatDecode(line string) []string {
	// no need to assert len(..) >= 1 because: split("", ",") => [""]
	msgParts := strings.Split(line[0:len(line)-1], " ")

	return msgParts
}

func MsgformatEncode(message []string) string {
	return strings.Join(message, " ") + "\n"
}
