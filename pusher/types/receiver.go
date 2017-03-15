package types

import (
	rtypes "github.com/function61/pyramid/reader/types"
)

type PushInput struct {
	SubscriptionId string
	Read           *rtypes.ReadResult
}

func NewPushInput(subscriptionId string, readResult *rtypes.ReadResult) *PushInput {
	return &PushInput{
		SubscriptionId: subscriptionId,
		Read:           readResult,
	}
}

type Receiver interface {
	Push(*PushInput) (*PushOutput, error)
}
