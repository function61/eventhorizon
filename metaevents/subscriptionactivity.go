package metaevents

import (
	"encoding/json"
	"time"
)

// .SubscriptionActivity {"activity": {"/tenants/foo": "/tenants/foo:0:700", "/tenants/bar": "/tenants/bar:245:14"}, "ts":"2017-02-27T17:12:31.446Z"}
type SubscriptionActivity struct {
	Activity  map[string]string `json:"activity"`
	Timestamp string            `json:"ts"`
}

func (c *SubscriptionActivity) Serialize() string {
	asJson, _ := json.Marshal(c)

	return ".SubscriptionActivity " + string(asJson) + "\n"
}

func NewSubscriptionActivity() *SubscriptionActivity {
	return &SubscriptionActivity{
		Activity:  make(map[string]string),
		Timestamp: time.Now().Format("2006-01-02T15:04:05.999Z"),
	}
}
