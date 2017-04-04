package metaevents

import (
	"encoding/json"
	"time"
)

const ChildStreamCreatedId = "ChildStreamCreated"

// /ChildStreamCreated {"name": "/tenants/foo", "cursor": "/tenants/foo:0:0", "ts":"2017-02-27T17:12:31.446Z"}
type ChildStreamCreated struct {
	Name      string `json:"name"`
	Cursor    string `json:"cursor"`
	Timestamp string `json:"ts"`
}

func (c *ChildStreamCreated) Serialize() string {
	asJson, _ := json.Marshal(c)

	return "/ChildStreamCreated " + string(asJson) + "\n"
}

func NewChildStreamCreated(name string, cursor string) *ChildStreamCreated {
	return &ChildStreamCreated{
		Name:      name,
		Cursor:    cursor,
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
