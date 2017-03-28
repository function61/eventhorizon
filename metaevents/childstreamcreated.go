package metaevents

import (
	"encoding/json"
	"time"
)

const ChildStreamCreatedId = "ChildStreamCreated"

// /ChildStreamCreated {"child_stream": "/tenants/foo:0:0", "ts":"2017-02-27T17:12:31.446Z"}
type ChildStreamCreated struct {
	ChildStream string `json:"child_stream"`
	Timestamp   string `json:"ts"`
}

func (c *ChildStreamCreated) Serialize() string {
	asJson, _ := json.Marshal(c)

	return "/ChildStreamCreated " + string(asJson) + "\n"
}

func NewChildStreamCreated(childStream string) *ChildStreamCreated {
	return &ChildStreamCreated{
		ChildStream: childStream,
		Timestamp:   time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
