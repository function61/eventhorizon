package metaevents

import (
	"encoding/json"
	"time"
)

const UnsubscribedId = "Unsubscribed"

// /Unsubscribed {"subscription_id":"6894605c-2a8e","ts":"2017-02-27T17:12:31.446Z"}
type Unsubscribed struct {
	SubscriptionId string `json:"subscription_id"`
	Timestamp      string `json:"ts"`
}

func (u *Unsubscribed) Serialize() string {
	asJson, _ := json.Marshal(u)

	return "/Unsubscribed " + string(asJson) + "\n"
}

func NewUnsubscribed(subscriptionId string) *Unsubscribed {
	return &Unsubscribed{
		SubscriptionId: subscriptionId,
		Timestamp:      time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
