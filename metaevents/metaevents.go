package metaevents

import (
	"time"
)

// {"_":"Created","ts":"2017-02-27T17:12:31.446Z"}
type Created struct {
	Type      string `json:"_"`
	Timestamp string `json:"ts"`
}

func NewCreated() *Created {
	return &Created{
		Type:      "Created",
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}

// .{"_":"AuthorityChange","peers":["127.0.0.1"]}
type AuthorityChange struct {
	Type  string   `json:"_"`
	Peers []string `json:"peers"`
}

func NewAuthorityChange(peers []string) *AuthorityChange {
	return &AuthorityChange{
		Type:  "AuthorityChange",
		Peers: peers,
	}
}
