package metaevents

import (
	"time"
)

type Unsubscribed struct {
	Type           string `json:"_"`
	SubscriptionId string `json:"subscription_id"`
	Timestamp      string `json:"ts"`
}

func NewUnsubscribed(subscriptionId string) *Unsubscribed {
	return &Unsubscribed{
		Type:           "Unsubscribed",
		SubscriptionId: subscriptionId,
		Timestamp:      time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
