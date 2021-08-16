package eh

import (
	"encoding/json"
	"fmt"
	"strings"
)

// bare subscriber ID (without stream path)
type SubscriberID struct {
	id string
}

func NewSubscriberID(id string) SubscriberID {
	if id == "" {
		panic("SubscriberID cannot be empty")
	}

	if strings.Contains(id, "/") {
		panic("SubscriberID cannot contain '/'")
	}

	return SubscriberID{id}
}

func (s SubscriberID) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.id)
}

func (s *SubscriberID) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.id); err != nil {
		return fmt.Errorf("SubscriberID.UnmarshalJSON: %w", err)
	}

	return nil
}

func (s SubscriberID) Equal(other SubscriberID) bool {
	return s.id == other.id
}

func (s SubscriberID) String() string {
	return s.id
}

// returns the stream name that backs subscription notifications for this subscriber
func (s SubscriberID) BackingStream() StreamName {
	return SysSubscribers.Child(s.id)
}
