package metaevents

import (
	"encoding/json"
	"time"
)

// .AuthorityChanged {"peers":["127.0.0.1"],"ts":"2017-02-27T17:12:31.446Z"}
type AuthorityChanged struct {
	Peers     []string `json:"peers"`
	Timestamp string   `json:"ts"`
}

func (a *AuthorityChanged) Serialize() string {
	asJson, _ := json.Marshal(a)

	return ".AuthorityChanged " + string(asJson) + "\n"
}

func NewAuthorityChanged(peers []string) *AuthorityChanged {
	return &AuthorityChanged{
		Peers:     peers,
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
