package types

import (
	rtypes "github.com/function61/pyramid/reader/types"
)

type PushInput struct {
	Read *rtypes.ReadResult
}

func NewPushInput(readResult *rtypes.ReadResult) *PushInput {
	return &PushInput{
		Read: readResult,
	}
}

type Receiver interface {
	GetSubscriptionId() (string, error)
	Push(*PushInput) (*PushOutput, error)
}
