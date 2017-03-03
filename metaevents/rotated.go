package metaevents

import (
	"time"
)

type Rotated struct {
	Type      string `json:"_"`
	Timestamp string `json:"ts"`
}

func NewRotated() *Rotated {
	return &Rotated{
		Type:      "Rotated",
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
