package metaevents

import (
	"encoding/json"
	"time"
)

// .Created {"ts":"2017-02-27T17:12:31.446Z"}
type Created struct {
	Timestamp string `json:"ts"`
}

func (c *Created) Serialize() string {
	asJson, _ := json.Marshal(c)

	return ".Created " + string(asJson) + "\n"
}

func NewCreated() *Created {
	return &Created{
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
