package metaevents

import (
	"encoding/json"
	"time"
)

// /Rotated {"next":"/tenants/foo:1:0:127.0.0.1","ts":"2017-03-03T19:33:49.709Z"}
type Rotated struct {
	Next      string `json:"next"`
	Timestamp string `json:"ts"`
}

func (r *Rotated) Serialize() string {
	asJson, _ := json.Marshal(r)

	return "/Rotated " + string(asJson) + "\n"
}

func NewRotated(next string) *Rotated {
	return &Rotated{
		Next:      next,
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
