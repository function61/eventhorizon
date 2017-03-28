package metaevents

import (
	"encoding/json"
	"time"
)

const CreatedId = "Created"

// /Created {"subscription_ids": "89a3c083-6396", "ts":"2017-02-27T17:12:31.446Z"}
type Created struct {
	SubscriptionIds []string `json:"subscription_ids"`
	Timestamp       string   `json:"ts"`
}

func (c *Created) Serialize() string {
	asJson, _ := json.Marshal(c)

	return "/Created " + string(asJson) + "\n"
}

func NewCreated(subscriptionIds []string) *Created {
	return &Created{
		SubscriptionIds: subscriptionIds,
		Timestamp:       time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
