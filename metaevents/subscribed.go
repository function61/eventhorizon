package metaevents

import (
	"time"
)

type Subscribed struct {
	Type           string `json:"_"`
	SubscriptionId string `json:"subscription_id"`
	Timestamp      string `json:"ts"`
}

func NewSubscribed(subscriptionId string) *Subscribed {
	return &Subscribed{
		Type:           "Subscribed",
		SubscriptionId: subscriptionId,
		Timestamp:      time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
