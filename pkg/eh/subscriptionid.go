package eh

import (
	"encoding/json"
	"fmt"
)

// bare subscription ID (without stream path)
type SubscriptionId struct {
	id string
}

func NewSubscriptionId(id string) SubscriptionId {
	if id == "" {
		panic("SubscriptionId cannot be empty")
	}

	// TODO: further validation

	return SubscriptionId{id}
}

func (s SubscriptionId) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.id)
}

func (s *SubscriptionId) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.id); err != nil {
		return fmt.Errorf("SubscriptionId.UnmarshalJSON: %w", err)
	}

	return nil
}

func (s SubscriptionId) Equal(other SubscriptionId) bool {
	return s.id == other.id
}

func (s SubscriptionId) String() string {
	return s.id
}

func (s SubscriptionId) StreamName() StreamName {
	return SysSubscriptions.Child(s.id)
}
