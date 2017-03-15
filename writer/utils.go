package writer

import (
	"errors"
	"github.com/function61/pyramid/metaevents"
	"path"
	"strings"
)

func parentStreamName(streamName string) string {
	return path.Dir(streamName)
}

func stringArrayToRawLines(contentArr []string) (string, error) {
	buf := ""

	for _, line := range contentArr {
		if strings.Contains(line, "\n") {
			return "", errors.New("EventstoreWriter.AppendToStream: content cannot contain \n")
		}

		buf += metaevents.EscapeRegularLine(line) + "\n"
	}

	return buf, nil
}
