package metaevents

import (
	"encoding/json"
	"time"
)

const SubscribedId = "Subscribed"

// /Subscribed {"subscription_id":"6894605c-2a8e","ts":"2017-02-27T17:12:31.446Z"}
type Subscribed struct {
	SubscriptionId string `json:"subscription_id"`
	Timestamp      string `json:"ts"`
}

func (s *Subscribed) Serialize() string {
	asJson, _ := json.Marshal(s)

	return "/Subscribed " + string(asJson) + "\n"
}

func NewSubscribed(subscriptionId string) *Subscribed {
	return &Subscribed{
		SubscriptionId: subscriptionId,
		Timestamp:      time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
